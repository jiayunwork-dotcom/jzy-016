import express from 'express';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import crypto from 'node:crypto';
import { JsonStore } from '../shared/store.js';
import { HttpError, badRequest, unauthorized, forbidden, notFound } from '../shared/errors.js';
import { hashPassword, verifyPassword, issueToken, requireAuth, requireRole, roleLabel } from '../shared/auth.js';
import {
  appendBatch,
  auditAbortedBatch,
  adjudicate,
  buildExport,
  createSnapshot,
  completeSnapshot,
  pullPrimary,
  assertCanAppend,
  assertCanAdjudicate
} from './causal.js';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const PORT = process.env.PORT || 4000;
const DATA_DIR = process.env.DATA_DIR || path.join(__dirname, '..', '..', 'data', 'primary');
const JWT_SECRET = process.env.JWT_SECRET || 'dev-shared-secret-change-me';
const INTERNAL_TOKEN = process.env.INTERNAL_TOKEN || 'dev-internal-token';
const SEED_DEMO = process.env.SEED_DEMO !== 'false';
const SNAPSHOT_AUTO_MS = Number(process.env.SNAPSHOT_AUTO_MS || 3000);

const initialState = () => ({
  counters: { seq: 0, batch: 0, abort: 0, queue: 0, snapshot: 0 },
  channelUp: true,
  users: [],
  events: [],
  queue: [],
  batches: [],
  abortedBatches: [],
  cursors: {},
  snapshots: {}
});

const store = new JsonStore(path.join(DATA_DIR, 'state.json'), initialState());

// ---------- 种子：账号 + 一段可演示的因果历史 ----------
async function seed() {
  const d = store.read();
  if (d.users.length === 0) {
    const users = [
      { id: 'u-ingest', username: 'ingest', role: 'ingest', name: '落库工·小落', password: 'ing123' },
      { id: 'u-consume', username: 'consume', role: 'consume', name: '消费工·小费', password: 'con123' },
      { id: 'u-reconcile', username: 'reconcile', role: 'reconcile', name: '对账工·小对', password: 'rec123' }
    ];
    await store.mutate(async (draft) => {
      for (const u of users) {
        const { salt, hash } = await hashPassword(u.password);
        draft.users.push({ id: u.id, username: u.username, role: u.role, name: u.name, salt, hash });
      }
    });
  }
  if (SEED_DEMO && d.events.length === 0) {
    const t = Date.now();
    // 批1：两笔下单（p1）
    const r1 = await store.mutate((draft) =>
      appendBatch(draft, {
        items: [
          { id: 'e-order100-created', root: 'order-100', partition: 'p1', payload: { type: 'OrderCreated', amount: 199 }, preds: [] },
          { id: 'e-order200-created', root: 'order-200', partition: 'p1', payload: { type: 'OrderCreated', amount: 88 }, preds: [] }
        ]
      }, { now: t + 1000 })
    );
    // 批2：order-100 支付（p2，站在 created 后）；order-200 仓库预留（p2，站在 created 后）
    const r2 = await store.mutate((draft) =>
      appendBatch(draft, {
        items: [
          { id: 'e-order100-paid', root: 'order-100', partition: 'p2', payload: { type: 'OrderPaid', channel: 'card' }, preds: ['e-order100-created'] },
          { id: 'e-order200-warehouse', root: 'order-200', partition: 'p2', payload: { type: 'WarehouseReserved', depot: 'A1' }, preds: ['e-order200-created'] }
        ]
      }, { now: t + 2000 })
    );
    // 批3：order-100 物流（p3，也站在 created 后，与 paid 同根互不可比 => 进并发队列）；
    //      order-200 签收站在下单分支上（与仓库操作互不可比 => 另一对）。
    const r3 = await store.mutate((draft) =>
      appendBatch(draft, {
        items: [
          { id: 'e-order100-shipped', root: 'order-100', partition: 'p3', payload: { type: 'Shipped', carrier: 'SF' }, preds: ['e-order100-created'] },
          { id: 'e-order200-signed', root: 'order-200', partition: 'p3', payload: { type: 'SignedByBranch', branch: 'created' }, preds: ['e-order200-created'] }
        ]
      }, { now: t + 3000 })
    );
    console.log('[seed] demo 历史已落库:',
      `批 ${r1.batchId}/${r2.batchId}/${r3.batchId}`,
      `并发对 ${[...r1.conflicts, ...r2.conflicts, ...r3.conflicts].length} 对（去重后见队列）`);
  }
}

// ---------- 视图组装 ----------
function publicView() {
  const d = store.read();
  const watermarks = {};
  for (const e of d.events) {
    watermarks[e.partition] = Math.max(watermarks[e.partition] ?? 0, e.seq);
  }
  return {
    channelUp: d.channelUp,
    counts: { events: d.events.length, queueOpen: d.queue.filter((q) => !q.verdict).length, queueDecided: d.queue.filter((q) => q.verdict).length },
    partitionWatermarks: watermarks,
    events: [...d.events].sort((a, b) => a.seq - b.seq),
    queue: d.queue,
    batches: [...d.batches].reverse(),
    abortedBatches: d.abortedBatches,
    cursors: d.cursors,
    snapshots: d.snapshots
  };
}

// ---------- 快照自动完成（演示用） ----------
function scheduleSnapshotComplete(root) {
  setTimeout(() => {
    store.mutate((draft) => completeSnapshot(draft, root)).catch(() => {});
  }, SNAPSHOT_AUTO_MS).unref();
}

// ---------- App ----------
const app = express();
app.use(express.json({ limit: '1mb' }));

// 副本直连 CORS（消费台页面从 :4000 打开，去调 :4001）
app.use((req, res, next) => {
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Headers', 'Authorization, Content-Type, X-Internal-Token');
  res.setHeader('Access-Control-Allow-Methods', 'GET, POST, OPTIONS');
  if (req.method === 'OPTIONS') return res.sendStatus(204);
  next();
});

// ---- 存活探针（免鉴权，供容器健康检查） ----
app.get('/healthz', (_req, res) => res.json({ ok: true, side: 'primary', channelUp: store.read().channelUp }));

// ---- 认证 ----
app.post('/api/auth/login', async (req, res, next) => {
  try {
    const { username, password } = req.body ?? {};
    if (!username || !password) throw badRequest('BAD_LOGIN', '用户名和密码必填');
    const user = store.read().users.find((u) => u.username === username);
    if (!user || !(await verifyPassword(password, user))) {
      throw unauthorized('BAD_CREDENTIALS', '用户名或密码错误');
    }
    const token = issueToken(user, JWT_SECRET);
    res.json({ token, user: { id: user.id, username: user.username, role: user.role, name: user.name } });
  } catch (e) {
    next(e);
  }
});

app.get('/api/me', requireAuth(JWT_SECRET), (req, res) => {
  res.json({ user: { id: req.user.sub, name: req.user.name, role: req.user.role }, roleLabel: roleLabel(req.user.role) });
});

// 没登录：任何 /api 数据接口都进 requireAuth
const auth = requireAuth(JWT_SECRET);

// ---- 全局只读视图（三个工位都要看库，但只能看自己该看的操作） ----
app.get('/api/state', auth, (_req, res) => res.json(publicView()));

// ---- 落库通道开关（落库工；断开=不能新追加） ----
app.post('/api/admin/channel', auth, requireRole('ingest'), async (req, res, next) => {
  try {
    const up = req.body?.up;
    if (typeof up !== 'boolean') throw badRequest('BAD_BODY', '需要 { up: boolean }');
    await store.mutate((draft) => {
      draft.channelUp = up;
    });
    res.json({ channelUp: up, note: up ? '通道恢复，可继续追加' : '通道断开：新追加一律拒绝；在库事件按最后一次有效时钟守序' });
  } catch (e) { next(e); }
});

// ---- 整批追加：要么全进，要么整批退 ----
app.post('/api/events/batch', auth, requireRole('ingest'), async (req, res, next) => {
  try {
    assertCanAppend(req.user); // 兜底：后台也拒
    const input = req.body;
    const result = await store.mutate((draft) => appendBatch(draft, input));
    res.status(201).json({ ok: true, ...result });
  } catch (e) {
    if (e instanceof HttpError && e.status === 400) {
      // 整批退回记一笔审计（事件一条都没进库）
      try {
        const abortId = await store.mutate((draft) =>
          auditAbortedBatch(draft, req.body, [{ code: e.code, message: e.message, extra: e.extra }])
        );
        e.extra = { ...e.extra, abortedBatchId: abortId };
      } catch { /* 审计失败不掩盖原错 */ }
    }
    next(e);
  }
});

// 前驱声明不可改：任何修改事件的企图都在路由层 405
app.patch('/api/events/:id', auth, (_req, _res, next) =>
  next(new HttpError(405, 'IMMUTABLE_EVENT', '事件与前驱声明只追加、不可修改', { allow: ['GET', 'POST /api/events/batch'] })));
app.put('/api/events/:id', auth, (_req, _res, next) =>
  next(new HttpError(405, 'IMMUTABLE_EVENT', '事件与前驱声明只追加、不可修改')));

// ---- 并发队列与裁决（只有对账工） ----
app.get('/api/queue', auth, (_req, res) => res.json({ queue: store.read().queue }));

app.post('/api/queue/:id/adjudicate', auth, requireRole('reconcile'), async (req, res, next) => {
  try {
    assertCanAdjudicate(req.user);
    const q = await store.mutate((draft) =>
      adjudicate(draft, req.params.id, req.body?.winner, req.user)
    );
    res.json({ ok: true, queue: q });
  } catch (e) { next(e); }
});

// 对账工不能追加新业务事件（后台兜底，路由限权已在前面拦截）
// 消费工/落库工想裁决 => 403，由 requireRole 统一返回

// ---- 导出：只放因果齐且并发已裁完的 ----
app.get('/api/export', auth, requireRole('ingest'), (_req, res) => {
  res.json(buildExport(store.read()));
});

// ---- 快照：同根不可同时两份 ----
app.get('/api/snapshots', auth, requireRole('ingest'), (_req, res) => {
  res.json({ snapshots: store.read().snapshots });
});
app.post('/api/snapshots', auth, requireRole('ingest'), async (req, res, next) => {
  try {
    const snap = await store.mutate((draft) => createSnapshot(draft, req.body?.root));
    scheduleSnapshotComplete(snap.root);
    res.status(201).json({ ok: true, snapshot: snap, note: `快照进行中；${SNAPSHOT_AUTO_MS / 1000} 秒后自动完成，期间同根拒绝第二份` });
  } catch (e) { next(e); }
});
app.post('/api/snapshots/:root/complete', auth, requireRole('ingest'), async (req, res, next) => {
  try {
    const snap = await store.mutate((draft) => completeSnapshot(draft, req.params.root));
    res.json({ ok: true, snapshot: snap });
  } catch (e) { next(e); }
});

// ---- 主侧消费（消费工；游标只读推进，没有任何改写入口） ----
app.get('/api/consume/cursors', auth, requireRole('consume'), (_req, res) => {
  res.json({ cursors: store.read().cursors });
});
// 消费游标不可被直接改写：405，对账工也动不了
app.post('/api/consume/cursors', auth, (_req, _res, next) =>
  next(new HttpError(405, 'CURSOR_NOT_MUTABLE', '消费游标只能通过拉动放行推进，不能直接改写', { allow: ['POST /api/consume/pull'] })));
app.put('/api/consume/cursors/:consumer', auth, (_req, _res, next) =>
  next(new HttpError(405, 'CURSOR_NOT_MUTABLE', '消费游标只能通过拉动放行推进，不能直接改写')));

app.post('/api/consume/pull', auth, requireRole('consume'), async (req, res, next) => {
  try {
    const consumer = (req.body?.consumer || 'worker-default').trim();
    if (!consumer) throw badRequest('NO_CONSUMER', '缺少消费者名');
    const limit = Math.min(Number(req.body?.limit) || 50, 500);
    const result = await store.mutate((draft) => pullPrimary(draft, consumer, limit));
    res.json(result);
  } catch (e) { next(e); }
});

// ---- 越权探针：页面/后台对所有非本职工位操作一律拒绝 ----
// 落库工尝试裁决、消费工尝试追加/裁决、对账工尝试追加，均在上面的 requireRole 处 403。
// 这里给一个统一的自查入口，前端"权限自检"面板直接打真实接口看返回码。
app.get('/api/rbac/matrix', auth, (_req, res) => {
  res.json({
    role: req.user.role,
    roleLabel: roleLabel(req.user.role),
    rules: {
      'POST /api/events/batch': ['ingest'],
      'POST /api/admin/channel': ['ingest'],
      'POST /api/snapshots': ['ingest'],
      'GET /api/export': ['ingest'],
      'POST /api/queue/:id/adjudicate': ['reconcile'],
      'POST /api/consume/pull': ['consume'],
      'GET /api/consume/cursors': ['consume']
    }
  });
});

// ---- 内部：副本增量同步（仅内网 token，不给浏览器） ----
app.get('/api/internal/sync', async (req, res, next) => {
  try {
    if (req.get('x-internal-token') !== INTERNAL_TOKEN) {
      throw forbidden('BAD_INTERNAL_TOKEN', '内部同步令牌错误');
    }
    const after = Math.max(0, Number(req.query.after) || 0);
    const limit = Math.min(Number(req.query.limit) || 500, 2000);
    const d = store.read();
    const events = d.events
      .filter((e) => e.seq > after)
      .sort((a, b) => a.seq - b.seq)
      .slice(0, limit);
    res.json({
      after,
      lastSeq: d.counters.seq,
      channelUp: d.channelUp,
      events,
      queue: d.queue
    });
  } catch (e) { next(e); }
});

// ---- 静态前端 ----
const webDir = path.join(__dirname, '..', '..', 'web');
app.use(express.static(webDir));
app.get(/^(?!\/api\/).*/, (_req, res) => res.sendFile(path.join(webDir, 'index.html')));

// ---- 错误处理 ----
app.use((err, _req, res, _next) => {
  if (err instanceof HttpError) {
    return res.status(err.status).json({ error: { code: err.code, message: err.message, ...err.extra } });
  }
  console.error('[unhandled]', err);
  res.status(500).json({ error: { code: 'INTERNAL', message: err.message || '内部错误' } });
});

// ---------- 启动 ----------
store
  .load()
  .then(seed)
  .then(() =>
    app.listen(PORT, () => {
      console.log(`[primary 落库台] http://localhost:${PORT}  data=${DATA_DIR}`);
    })
  )
  .catch((e) => {
    console.error('启动失败', e);
    process.exit(1);
  });
