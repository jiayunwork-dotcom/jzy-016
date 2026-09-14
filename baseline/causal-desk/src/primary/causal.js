// 落库台核心：整批因果追加 / 并发队列 / 裁决 / 导出 / 快照
import { mergeClocks, tickClock, compareClocks } from '../shared/clock.js';
import { badRequest, conflict, forbidden, notFound } from '../shared/errors.js';

const ID_RE = /^[A-Za-z0-9][A-Za-z0-9:_-]{0,63}$/;

function genId() {
  // 客户端不指定 id 时生成（批内引用仍可由前端预先生成）
  return 'e' + crypto.randomUUID().replace(/-/g, '').slice(0, 16);
}

function indexEvents(events) {
  const byId = new Map();
  for (const e of events) byId.set(e.id, e);
  return byId;
}

// ---- 整批追加：要么全进，要么整批退 ----
// draft: store 的深副本（抛错即丢弃，不会落盘）
export function appendBatch(draft, input, { now = Date.now() } = {}) {
  if (!draft.channelUp) {
    throw conflict('CHANNEL_DOWN', '落库通道已断开：不能新追加；在库事件仍按最后一次有效时钟守序');
  }
  const rawItems = Array.isArray(input?.items) ? input.items : null;
  if (!rawItems || rawItems.length === 0) {
    throw badRequest('EMPTY_BATCH', '批次不能为空');
  }

  // ---- 1) 逐条做静态校验，先不写任何东西 ----
  const items = rawItems.map((it, i) => {
    const id = (it.id ?? genId()).trim();
    const root = (it.root ?? '').trim();
    const partition = (it.partition ?? '').trim();
    const payload = it.payload ?? null;
    const preds = Array.isArray(it.preds) ? it.preds.map((p) => String(p).trim()).filter(Boolean) : [];
    if (!ID_RE.test(id)) throw badRequest('BAD_EVENT_ID', `第 ${i + 1} 条事件 id 非法：${id}`);
    if (!root) throw badRequest('NO_ROOT', `第 ${i + 1} 条事件缺少聚合根`, { index: i });
    if (!partition) throw badRequest('NO_PARTITION', `第 ${i + 1} 条事件缺少分区`, { index: i });
    if (payload === null || payload === undefined)
      throw badRequest('NO_PAYLOAD', `第 ${i + 1} 条事件缺少载荷`, { index: i });
    return { id, root, partition, payload, preds, index: i };
  });

  const committed = indexEvents(draft.events);
  const local = new Map(items.map((e) => [e.id, e]));

  const seen = new Set();
  for (const it of items) {
    if (seen.has(it.id)) throw badRequest('DUP_ID_IN_BATCH', `批内事件 id 重复：${it.id}`);
    if (committed.has(it.id)) throw badRequest('DUP_ID_IN_STORE', `事件 id 已在库：${it.id}`);
    seen.add(it.id);
  }

  // ---- 2) 停写封条：封的是根，不是通道。通道可以开着，被封的聚合根整批退回 ----
  // 与通道断开互不影响：通道恢复后封条继续拦；被封不影响其他根追加。
  const seals = draft.seals ?? {};
  const sealedRoots = [...new Set(items.map((it) => it.root).filter((r) => seals[r]))];
  if (sealedRoots.length) {
    throw conflict(
      'ROOT_SEALED',
      `聚合根 ${sealedRoots.join('、')} 已贴停写封条：整批退回，一条都不落库；揭掉封条后才能再交新事件（被拦的本批不会偷偷补进）`,
      { sealedRoots }
    );
  }

  // ---- 3) 依赖检查：声明的前驱每条都已在库，或在本批更早处理 ----
  for (const it of items) {
    for (const pid of it.preds) {
      const inStore = committed.get(pid);
      const inBatch = local.get(pid);
      if (!inStore && !inBatch) {
        throw badRequest(
          'PREDECESSOR_MISSING',
          `事件 ${it.id} 声明的前驱 ${pid} 不在库、也不在本批，本批对外依赖不齐，整批退回`,
          { event: it.id, missing: pid }
        );
      }
      const owner = inStore ?? inBatch;
      if (owner.root !== it.root) {
        throw badRequest(
          'PREDECESSOR_ROOT_MISMATCH',
          `事件 ${it.id} 与前驱 ${pid} 不属于同一聚合根（${it.root} vs ${owner.root}）`,
          { event: it.id, predecessor: pid }
        );
      }
    }
  }

  // ---- 4) 批内拓扑排序（批内依赖必须是 DAG，有环整批退）----
  const order = [];
  const state = new Map(items.map((e) => [e.id, 0]));
  const visit = (node, stack) => {
    const s = state.get(node.id);
    if (s === 1) throw badRequest('CYCLE_IN_BATCH', `批内前驱成环：${[...stack, node.id].join(' -> ')}`);
    if (s === 2) return;
    state.set(node.id, 1);
    for (const pid of node.preds) {
      const dep = local.get(pid);
      if (dep) visit(dep, [...stack, node.id]);
    }
    state.set(node.id, 2);
    order.push(node);
  };
  for (const it of items) visit(it, []);

  // ---- 5) 逐条计算向量时钟 ----
  // 规则：并上前驱各分量的最大值，本事件所在分区分量再加一。
  // 批内前驱用其"待提交时钟"，库内前驱用已提交时钟。
  const pendingClock = new Map();
  const materialize = new Map(); // id -> 完整事件（未提交）

  for (const it of order) {
    const predClocks = it.preds.map((pid) => {
      const pe = materialize.get(pid) ?? committed.get(pid);
      return pe.clock;
    });
    const clock = tickClock(mergeClocks(...predClocks), it.partition);
    pendingClock.set(it.id, clock);

    materialize.set(it.id, {
      id: it.id,
      root: it.root,
      partition: it.partition,
      payload: it.payload,
      preds: [...it.preds],
      clock,
      seq: null, // 提交时统一编号
      batchId: null,
      ts: now
    });
  }

  // ---- 6) 同根因果检查：两笔时钟互不可比 => 进并发队列，不得自动并成一条 ----
  // 不同事件时钟相等也视为互不可比（两个不同事件不能占据同一时间点）。
  const newQueuePairs = [];
  const pairKeys = new Set(draft.queue.map((q) => qKey(q.a, q.b)));
  const considerPair = (x, y) => {
    if (x.root !== y.root) return;
    if (x.id === y.id) return;
    // 若 y 已显式（传递）声明 x 为前驱，则有因果序，不算并发
    if (isAncestor(materialize, committed, y, x.id)) return;
    if (isAncestor(materialize, committed, x, y.id)) return;
    const cmp = compareClocks(x.clock, y.clock);
    if (cmp === null || cmp === 0) {
      const key = qKey(x.id, y.id);
      if (pairKeys.has(key)) return;
      pairKeys.add(key);
      newQueuePairs.push({
        id: 'q' + (draft.counters.queue++),
        a: x.id,
        b: y.id,
        root: x.root,
        reason: cmp === 0 ? 'CLOCK_EQUAL' : 'CLOCKS_INCOMPARABLE',
        clocks: { [x.id]: x.clock, [y.id]: y.clock },
        verdict: null,
        decidedBy: null,
        decidedAt: null,
        createdAt: now
      });
    }
  };

  // 新事件 vs 库内同根事件
  for (const it of order) {
    const ev = materialize.get(it.id);
    for (const old of draft.events) {
      if (old.root === it.root) considerPair(old, ev);
    }
  }
  // 同批内两两比较
  for (let i = 0; i < order.length; i++) {
    for (let j = i + 1; j < order.length; j++) {
      considerPair(materialize.get(order[i].id), materialize.get(order[j].id));
    }
  }

  // ---- 7) 提交：统一编号、整批进入。上面任何一步抛错都到不了这里 ----
  const batchId = 'b' + ++draft.counters.batch;
  const seqFrom = draft.counters.seq;
  const committedEvents = [];
  // 按拓扑序分配 seq，保证前驱 seq 一定更小（游标按 seq 守序）
  for (const it of order) {
    const ev = materialize.get(it.id);
    ev.seq = ++draft.counters.seq;
    ev.batchId = batchId;
    draft.events.push(ev);
    committedEvents.push(ev);
  }
  for (const q of newQueuePairs) draft.queue.push(q);
  draft.batches.push({
    id: batchId,
    ts: now,
    seqFrom: seqFrom + 1,
    seqTo: draft.counters.seq,
    eventIds: committedEvents.map((e) => e.id)
  });

  return {
    batchId,
    committed: committedEvents.map((e) => ({ id: e.id, seq: e.seq, root: e.root, partition: e.partition, clock: e.clock })),
    conflicts: newQueuePairs.map((q) => ({ id: q.id, a: q.a, b: q.b, reason: q.reason }))
  };
}

// 整批退回审计（事件一条都没进库，这只是失败记录）
export function auditAbortedBatch(draft, input, reasons, now = Date.now()) {
  const batchId = 'x' + ++draft.counters.abort;
  draft.abortedBatches.unshift({
    id: batchId,
    ts: now,
    items: (input?.items ?? []).map((it) => ({
      id: it.id ?? null,
      root: it.root ?? null,
      partition: it.partition ?? null,
      payload: it.payload ?? null,
      preds: Array.isArray(it.preds) ? it.preds : []
    })),
    reasons
  });
  if (draft.abortedBatches.length > 100) draft.abortedBatches.length = 100;
  return batchId;
}

function qKey(a, b) {
  return [a, b].sort().join('|');
}

// maybeId 是否是 ev 的（传递）前驱
function isAncestor(materialize, committed, ev, maybeId) {
  const stack = [...ev.preds];
  const seen = new Set();
  while (stack.length) {
    const pid = stack.pop();
    if (pid === maybeId) return true;
    if (seen.has(pid)) continue;
    seen.add(pid);
    const p = materialize.get(pid) ?? committed.get(pid);
    if (p) stack.push(...p.preds);
  }
  return false;
}

// ---- 并发裁决（仅对账工，后台同样限权）----
export function adjudicate(draft, queueId, winner, user, now = Date.now()) {
  const q = draft.queue.find((x) => x.id === queueId);
  if (!q) throw notFound('QUEUE_NOT_FOUND', `并发对 ${queueId} 不存在`);
  if (q.verdict) throw conflict('ALREADY_DECIDED', `并发对 ${queueId} 已裁决，不能改判`);
  if (winner !== 'a' && winner !== 'b') throw badRequest('BAD_WINNER', 'winner 必须是 a 或 b');
  q.verdict = winner;
  q.winnerEvent = winner === 'a' ? q.a : q.b;
  q.decidedBy = user.sub;
  q.decidedByName = user.name;
  q.decidedAt = now;
  return q;
}

// ---- 停写封条：封的是聚合根，不是落库通道 ----
// 通道可以开着，被封的根不能再追加；在库旧事件照常消费/裁决/导出（那三条路径完全不查封条）。
// 同一根同一时刻只有一份封条（seals 以根为键，结构上就放不下第二份）。
export function sealRoot(draft, root, user, now = Date.now()) {
  root = (root ?? '').trim();
  if (!root) throw badRequest('NO_ROOT', '缺少聚合根');
  if (!draft.channelUp) {
    throw conflict('CHANNEL_DOWN', '落库通道已断开：贴封条、揭封条都做不成；已贴上的封条继续拦对应根');
  }
  if (!draft.events.some((e) => e.root === root)) {
    throw notFound('ROOT_NOT_IN_STORE', `聚合根 ${root} 还没在库，不能贴封条（页面上也不该出现可贴的封条）`);
  }
  draft.seals ??= {};
  if (draft.seals[root]) {
    throw conflict('SEAL_EXISTS', `聚合根 ${root} 已挂封条 ${draft.seals[root].id}，同一根不能同时挂两份封条`);
  }
  const seal = {
    id: 'f' + ++draft.counters.seal,
    root,
    sealedBy: user.sub,
    sealedByName: user.name,
    sealedAt: now
  };
  draft.seals[root] = seal;
  return seal;
}

export function unsealRoot(draft, root, user, now = Date.now()) {
  root = (root ?? '').trim();
  if (!draft.channelUp) {
    throw conflict('CHANNEL_DOWN', '落库通道已断开：贴封条、揭封条都做不成；已贴上的封条继续拦对应根');
  }
  draft.seals ??= {};
  const seal = draft.seals[root];
  if (!seal) throw notFound('SEAL_NOT_FOUND', `聚合根 ${root} 没有封条可揭`);
  delete draft.seals[root];
  // 注意：揭封只恢复"以后能交"，此前被拦的批次早已整批退回，绝不在这里偷偷补进
  return { removed: seal, unsealedBy: user.sub, unsealedByName: user.name, unsealedAt: now };
}

// ---- 导出：因果齐 + 并发已裁完 ----
// 未决并发对中的事件一律不导出；被不导出事件（传递）后继的事件也不导出。
export function buildExport(draft) {
  const byId = indexEvents(draft.events);

  // 1) 因参与未决并发对而被挡住的事件
  const blocked = new Map(); // id -> reason
  for (const q of draft.queue) {
    if (!q.verdict) {
      blocked.set(q.a, `UNRESOLVED_CONFLICT:${q.id}`);
      blocked.set(q.b, `UNRESOLVED_CONFLICT:${q.id}`);
    }
  }
  // 2) 前驱缺口 / 前驱被挡 => 后继也不能走（传递闭包）
  //    正常入库不会有"缺前驱"，这里把半截依赖也防御性挡住
  const reasonOf = (id) => blocked.get(id);
  let changed = true;
  while (changed) {
    changed = false;
    for (const e of draft.events) {
      if (blocked.has(e.id)) continue;
      for (const pid of e.preds) {
        if (!byId.has(pid)) {
          blocked.set(e.id, `PREDECESSOR_GAP:${pid}`);
          changed = true;
          break;
        }
        const r = reasonOf(pid);
        if (r) {
          blocked.set(e.id, `BLOCKED_BY_PREDECESSOR:${pid}:${r}`);
          changed = true;
          break;
        }
      }
    }
  }

  const exported = [];
  const blockedList = [];
  for (const e of [...draft.events].sort((a, b) => a.seq - b.seq)) {
    const r = blocked.get(e.id);
    if (r) blockedList.push({ seq: e.seq, id: e.id, root: e.root, reason: r });
    else exported.push(e);
  }

  return {
    generatedAt: new Date().toISOString(),
    channelUp: draft.channelUp,
    counts: { total: draft.events.length, exported: exported.length, blocked: blockedList.length },
    adjudications: draft.queue.filter((q) => q.verdict).map((q) => ({
      queueId: q.id, root: q.root, winner: q.winnerEvent, loser: q.winnerEvent === q.a ? q.b : q.a,
      decidedBy: q.decidedByName, decidedAt: q.decidedAt
    })),
    events: exported,
    blocked: blockedList
  };
}

// ---- 快照：同一根不能同时打两份 ----
export function createSnapshot(draft, root, now = Date.now()) {
  root = (root ?? '').trim();
  if (!root) throw badRequest('NO_ROOT', '缺少聚合根');
  const cur = draft.snapshots[root];
  if (cur && cur.state === 'running') {
    throw conflict('SNAPSHOT_IN_PROGRESS', `聚合根 ${root} 已有一份进行中的快照 ${cur.id}，同一根不能同时打两份快照`);
  }
  const last = [...draft.events].reverse().find((e) => e.root === root);
  const snap = {
    id: 's' + ++draft.counters.snapshot,
    root,
    state: 'running',
    createdAt: now,
    finishedAt: null,
    atSeq: last ? last.seq : 0
  };
  draft.snapshots[root] = snap;
  return snap;
}

export function completeSnapshot(draft, root, now = Date.now()) {
  const snap = draft.snapshots[root];
  if (!snap) throw notFound('SNAPSHOT_NOT_FOUND', `聚合根 ${root} 没有快照`);
  if (snap.state !== 'running') return snap;
  snap.state = 'done';
  snap.finishedAt = now;
  return snap;
}

// ---- 主侧消费放行：只看"全部前驱是否已对消费者可见" ----
// 主库上，已提交即可见；游标严格按 seq 走，缺口前停下不跳过。
export function pullPrimary(draft, consumer, limit = 100, now = Date.now()) {
  const cur = draft.cursors[consumer];
  const byId = indexEvents(draft.events);
  const delivered = [];
  let blockedAt = null;
  let blockedReason = null;

  for (let n = (cur?.lastSeq ?? 0) + 1; delivered.length < limit; n++) {
    const ev = draft.events.find((e) => e.seq === n);
    if (!ev) break; // 主库 seq 连续，找不到就是没有更多
    const missing = ev.preds.filter((pid) => {
      const p = byId.get(pid);
      // 前驱必须已提交且已经被该消费者越过（可见）
      return !p || p.seq > (cur?.lastSeq ?? 0) + delivered.length;
    });
    if (missing.length) {
      blockedAt = n;
      blockedReason = missing.map((pid) => `PREDECESSOR_NOT_VISIBLE:${pid}`);
      break; // 游标停在缺口前，绝不跳过
    }
    delivered.push(ev);
  }

  const lastSeq = (cur?.lastSeq ?? 0) + delivered.length;
  draft.cursors[consumer] = {
    lastSeq,
    blockedAt: blockedAt ?? (cur?.blockedAt ?? null),
    blockedReason: blockedReason ?? (blockedAt ? null : cur?.blockedReason ?? null),
    updatedAt: now
  };
  return {
    side: 'primary',
    consumer,
    delivered,
    cursor: lastSeq,
    blockedAt,
    blockedReason,
    caughtUp: delivered.length === 0 ? !blockedAt : blockedAt === null
  };
}

// 越权防护的服务端二次断言（路由已限权，这里是兜底，防后台直调）
export function assertCanAppend(user) {
  if (user.role !== 'ingest') throw forbidden('ROLE_DENIED', '只有落库工可以追加事件');
}
export function assertCanAdjudicate(user) {
  if (user.role !== 'reconcile') throw forbidden('ROLE_DENIED', '只有对账工可以裁决并发');
}
export function assertCanConsume(user) {
  if (user.role !== 'consume') throw forbidden('ROLE_DENIED', '只有消费工可以拉动消费游标');
}
export function assertCanSeal(user) {
  if (user.role !== 'ingest') throw forbidden('ROLE_DENIED', '只有落库工可以贴/揭停写封条');
}
