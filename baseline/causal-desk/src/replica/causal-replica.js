// 消费台副本：从主库增量同步；可注入"某分区滞后"和"同步超时"。
// 放行规则与主侧一致：只看前驱对本消费者是否可见，缺口前停下，绝不标已送达。
import { conflict } from '../shared/errors.js';

const TIMEOUT_MS = () => Number(process.env.REPLICA_SYNC_TIMEOUT_MS || 1500);
const FETCH_LIMIT = 500;

// state: { sync, partitions, events, queue, cursors, consumers, failures, lastSync }
export function applyEvents(draft, events) {
  const have = new Map(draft.events.map((e) => [e.seq, e]));
  let applied = 0;
  let skippedLagged = 0;
  const skipped = [];
  const lagParts = new Set(draft.sync.lagPartitions ?? []);

  // 严格连续应用：从主库按 seq 升序到达，遇到一个"被滞后分区"的事件就停在那，
  // 它后面的事件（哪怕分区已追上、哪怕属于别的分区）本副本一律先不收。
  // 这样副本视图永远是主库的一个 seq 前缀（叠加分区过滤），
  // 跨分区前驱在本副本看不见时，后继也一定还没进来 / 不可能被放行。
  let stuckAt = null;
  for (const ev of events) {
    if (have.has(ev.seq)) continue; // 幂等（修复窗口里的重复）
    if (draft.sync.mode === 'lag' && lagParts.has(ev.partition)) {
      stuckAt = { seq: ev.seq, id: ev.id, partition: ev.partition };
      break;
    }
    draft.events.push(ev);
    have.set(ev.seq, ev);
    applied++;
  }
  if (stuckAt) {
    skippedLagged = 1;
    skipped.push({ ...stuckAt, reason: 'PARTITION_LAG' });
  }

  draft.events.sort((a, b) => a.seq - b.seq);

  // 分区水位 = 本副本已应用到的该分区最大 seq
  for (const e of draft.events) {
    draft.partitions[e.partition] = Math.max(draft.partitions[e.partition] ?? 0, e.seq);
  }
  return { applied, skippedLagged, skipped };
}

// 一次同步。副本维护两个位点：
//   catchUpSeq —— 已连续应用到的 seq（下次从这里之后拉；滞后恢复从断点续上）
//   fetchHigh  —— 已从主库窗口看到的最大 seq（仅诊断用）
export async function syncOnce(draft, primaryUrl, internalToken) {
  if (draft.sync.paused) {
    return { status: 'paused', applied: 0, repaired: 0 };
  }
  const timeoutMode = draft.sync.mode === 'timeout';

  // ---- 同步超时（本次直接记失败，不应用任何事件） ----
  if (timeoutMode) {
    throw conflict(
      'REPLICA_SYNC_TIMEOUT',
      `副本同步超时（注入）：本次放行记失败，未把任何后继标成已送达`
    );
  }

  // ---- 从断点续拉（滞后恢复后天然把卡住的分区事件及其后继补齐） ----
  const url = `${primaryUrl}/api/internal/sync?after=${draft.sync.catchUpSeq ?? 0}&limit=${FETCH_LIMIT}`;
  const r = await fetchJson(url, internalToken, TIMEOUT_MS());
  const { applied, skippedLagged, skipped } = applyEvents(draft, r.events);

  const appliedMax = draft.events.length ? draft.events[draft.events.length - 1].seq : draft.sync.catchUpSeq ?? 0;
  draft.sync.catchUpSeq = appliedMax;
  const seenMax = r.events.length ? r.events[r.events.length - 1].seq : draft.sync.fetchHigh ?? 0;
  draft.sync.fetchHigh = Math.max(draft.sync.fetchHigh ?? 0, seenMax);
  draft.queue = r.queue; // 队列裁决状态随同步刷新
  draft.lastSync = {
    at: Date.now(),
    primaryLastSeq: r.lastSeq,
    applied,
    skippedLagged,
    stuckAt: skipped[0] ?? null,
    catchUpSeq: draft.sync.catchUpSeq
  };
  return {
    status: skippedLagged ? 'lagging' : 'ok',
    applied,
    stuckAt: skipped[0] ?? null,
    catchUpSeq: draft.sync.catchUpSeq,
    primaryLastSeq: r.lastSeq
  };
}

async function fetchJson(url, token, timeoutMs) {
  const ctrl = new AbortController();
  const timer = setTimeout(() => ctrl.abort(), timeoutMs);
  try {
    const res = await fetch(url, {
      headers: { 'x-internal-token': token },
      signal: ctrl.signal
    });
    if (!res.ok) {
      const body = await res.json().catch(() => ({}));
      throw new Error(`主库同步返回 ${res.status}: ${body?.error?.message || res.statusText}`);
    }
    return await res.json();
  } catch (e) {
    if (e.name === 'AbortError') {
      const err = new Error('SYNC_TIMEOUT');
      err.code = 'SYNC_TIMEOUT';
      throw err;
    }
    throw e;
  } finally {
    clearTimeout(timer);
  }
}

// 前驱所在分区：优先本副本已镜像事件；未镜像的缺口事件取同步卡点 stuckAt
// （applyEvents 在滞后分区第一条处停住，stuckAt 正是"哪一侧分区没追上"）。
function describeMissing(draft, predId) {
  const inPrimary = draft.events.find((e) => e.id === predId);
  if (inPrimary) {
    return { predecessor: predId, partition: inPrimary.partition, atSeq: inPrimary.seq };
  }
  const stuck = draft.lastSync?.stuckAt;
  if (stuck && stuck.id === predId) {
    return { predecessor: predId, partition: stuck.partition, atSeq: stuck.seq };
  }
  return { predecessor: predId, partition: 'unknown', atSeq: null };
}

// ---- 消费者拉动：只放行"全部前驱在本副本已对本消费者可见"的事件 ----
export function pullReplica(draft, consumer, limit = 50, now = Date.now()) {
  if (!draft.consumers[consumer]) draft.consumers[consumer] = { registeredAt: now };
  const cur = draft.cursors[consumer];
  const startSeq = cur?.lastSeq ?? 0;
  const bySeq = new Map(draft.events.map((e) => [e.seq, e]));
  const delivered = [];

  let blockingGap = null; // { seq, predecessor, partition, atSeq, replicaWatermark, kind }

  for (let n = startSeq + 1; delivered.length < limit; n++) {
    const ev = bySeq.get(n);
    const visibleThrough = startSeq + delivered.length;

    // 这个 seq 在本副本还没镜像到。
    // 如果同步正卡在某个滞后分区（stuckAt 就落在这个 seq），把缺口显式报出来：
    // 后继（含其之后的事件）对本副本消费者不可见，游标停在缺口前。
    if (!ev) {
      const stuck = draft.lastSync?.stuckAt;
      if (stuck && stuck.seq === n) {
        // 尝试从主库同步负载里推断被挡事件；拿不到就只报分区
        blockingGap = {
          atSeq: n,
          event: stuck.id,
          root: null,
          predecessor: stuck.id,
          laggingPartition: stuck.partition,
          requiredSeq: stuck.seq,
          replicaWatermark: draft.partitions[stuck.partition] ?? 0,
          kind: 'PARTITION_BEHIND_WAITING_REPLICATION'
        };
      }
      break; // 窗口外：正常等待；有卡点则上面已记录，绝不跳过
    }

    const missingPred = ev.preds.find((pid) => {
      const p = draft.events.find((x) => x.id === pid);
      return !p || p.seq > visibleThrough;
    });

    if (missingPred) {
      const info = describeMissing(draft, missingPred);
      blockingGap = {
        atSeq: n,
        event: ev.id,
        root: ev.root,
        predecessor: missingPred,
        laggingPartition: info.partition,
        requiredSeq: info.atSeq,
        replicaWatermark: info.partition === 'unknown' ? 0 : draft.partitions[info.partition] ?? 0,
        kind: info.atSeq === null ? 'EVENT_NOT_REPLICATED' : 'PARTITION_BEHIND'
      };
      break; // 游标停在缺口前，不跳过、不标已送达
    }
    delivered.push(ev);
  }

  const lastSeq = startSeq + delivered.length;
  draft.cursors[consumer] = {
    lastSeq,
    blockedAt: blockingGap?.atSeq ?? null,
    gap: blockingGap ?? cur?.gap ?? null,
    updatedAt: now
  };

  return {
    side: 'replica',
    consumer,
    delivered,
    cursor: lastSeq,
    blockedAt: blockingGap?.atSeq ?? null,
    blockedReason: blockingGap
      ? [
          `前驱未齐: ${blockingGap.predecessor}（分区 ${blockingGap.laggingPartition}）`,
          `该分区副本水位 ${blockingGap.replicaWatermark}，需要先追上 seq ${blockingGap.requiredSeq ?? '?'}`
        ]
      : null,
    gap: blockingGap,
    caughtUp: blockingGap === null
  };
}

export function recordFailure(draft, rec, now = Date.now()) {
  draft.failures.unshift({ id: 'f' + ++draft.sync.failureCounter, ts: now, ...rec });
  if (draft.failures.length > 100) draft.failures.length = 100;
}

export function exportView(draft) {
  const open = new Set();
  for (const q of draft.queue) if (!q.verdict) { open.add(q.a); open.add(q.b); }
  const events = draft.events.filter((e) => !open.has(e.id));
  return {
    side: 'replica',
    generatedAt: new Date().toISOString(),
    counts: { mirrored: draft.events.length, exportable: events.length, heldInConflict: draft.events.length - events.length },
    events,
    note: '仅含本副本已镜像、且并发已裁完的事件；跨分区缺口本身不会进入镜像'
  };
}
