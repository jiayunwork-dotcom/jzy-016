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
  draft.holds = r.holds ?? {}; // 按根暂扣状态随同步镜像（暂扣只挡放行，不挡镜像入库）
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

// ---- 消费者拉动：只放行"全部前驱在本副本已对本消费者可见、且所在根没被暂扣"的事件 ----
// 与主侧同口径：游标记"已放行集合"。被扣的根停在该根第一条未放出的事件前，不越过它
// 装后面的；别的根照常拉动。副本特有的缺口（分区滞后/没镜像到）仍然是整流停下不跳过。
export function pullReplica(draft, consumer, limit = 50, now = Date.now()) {
  if (!draft.consumers[consumer]) draft.consumers[consumer] = { registeredAt: now };
  const cur = draft.cursors[consumer] ?? { lastSeq: 0 };
  if (!Array.isArray(cur.deliveredSeqs)) {
    // 旧游标是连续前缀模型：1..lastSeq 都已放行
    cur.deliveredSeqs = Array.from({ length: cur.lastSeq ?? 0 }, (_, i) => i + 1);
  }
  const done = new Set(cur.deliveredSeqs);
  const holds = draft.holds ?? {};
  const bySeq = new Map(draft.events.map((e) => [e.seq, e]));
  const byId = new Map(draft.events.map((e) => [e.id, e]));
  const delivered = [];
  const heldBack = new Map(); // root -> 该根第一条被拦住的事件

  let blockingGap = null; // { seq, predecessor, partition, atSeq, replicaWatermark, kind }

  // 本副本已连续镜像到 catchUpSeq；扫描其中还没放行的（已放行集合不必是连续前缀）
  const high = draft.sync.catchUpSeq ?? 0;
  for (let n = 1; n <= high && delivered.length < limit; n++) {
    if (done.has(n)) continue; // 已放到的不再重复放
    const ev = bySeq.get(n);
    if (!ev) {
      // 镜像空洞（同步严格连续应用，正常不会发生；防御性停下，不跳过）
      blockingGap = {
        atSeq: n,
        event: null,
        root: null,
        predecessor: null,
        laggingPartition: 'unknown',
        requiredSeq: n,
        replicaWatermark: 0,
        kind: 'EVENT_NOT_REPLICATED'
      };
      break;
    }
    if (holds[ev.root]) {
      if (!heldBack.has(ev.root)) heldBack.set(ev.root, ev);
      continue; // 暂扣只挡这根的放行：不越过它装同根后面的；别的根照常
    }
    const missingPred = ev.preds.find((pid) => {
      const p = byId.get(pid);
      return !p || !done.has(p.seq);
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
    done.add(ev.seq);
  }

  // 窗口边缘：同步正卡在某个滞后分区时，把缺口显式报出来（没镜像到的 seq 不跳过）
  if (!blockingGap && delivered.length < limit) {
    const stuck = draft.lastSync?.stuckAt;
    if (stuck && stuck.seq === high + 1) {
      blockingGap = {
        atSeq: stuck.seq,
        event: stuck.id,
        root: null,
        predecessor: stuck.id,
        laggingPartition: stuck.partition,
        requiredSeq: stuck.seq,
        replicaWatermark: draft.partitions[stuck.partition] ?? 0,
        kind: 'PARTITION_BEHIND_WAITING_REPLICATION'
      };
    }
  }

  // 连续前缀水位（展示用）：被扣的根会让前缀停在它第一条未放出的事件前
  let prefix = 0;
  while (done.has(prefix + 1)) prefix++;

  cur.deliveredSeqs = [...done].sort((a, b) => a - b);
  cur.lastSeq = prefix;
  cur.heldBack = [...heldBack.entries()].map(([root, ev]) => ({
    root, atSeq: ev.seq, event: ev.id, holdId: holds[root]?.id ?? null
  }));
  cur.blockedAt = blockingGap?.atSeq ?? null;
  cur.gap = blockingGap ?? cur?.gap ?? null;
  cur.updatedAt = now;
  draft.cursors[consumer] = cur;

  return {
    side: 'replica',
    consumer,
    delivered,
    cursor: prefix,
    heldBack: cur.heldBack,
    blockedAt: cur.blockedAt,
    blockedReason: blockingGap
      ? [
          `前驱未齐: ${blockingGap.predecessor}（分区 ${blockingGap.laggingPartition}）`,
          `该分区副本水位 ${blockingGap.replicaWatermark}，需要先追上 seq ${blockingGap.requiredSeq ?? '?'}`
        ]
      : null,
    gap: blockingGap,
    caughtUp: blockingGap === null && heldBack.size === 0
  };
}

export function recordFailure(draft, rec, now = Date.now()) {
  draft.failures.unshift({ id: 'f' + ++draft.sync.failureCounter, ts: now, ...rec });
  if (draft.failures.length > 100) draft.failures.length = 100;
}

export function exportView(draft) {
  const open = new Set();
  for (const q of draft.queue) if (!q.verdict) { open.add(q.a); open.add(q.b); }
  // 按根暂扣同主侧口径：被扣的根只带本副本已经放到的
  const holds = draft.holds ?? {};
  const released = new Set();
  for (const c of Object.values(draft.cursors ?? {})) {
    for (const s of c.deliveredSeqs ?? []) released.add(s);
  }
  const events = draft.events.filter((e) =>
    !open.has(e.id) && !(holds[e.root] && !released.has(e.seq))
  );
  return {
    side: 'replica',
    generatedAt: new Date().toISOString(),
    counts: { mirrored: draft.events.length, exportable: events.length, heldInConflict: draft.events.length - events.length },
    events,
    note: '仅含本副本已镜像、并发已裁完、且（对被暂扣的根）已经放到的事件；跨分区缺口本身不会进入镜像'
  };
}
