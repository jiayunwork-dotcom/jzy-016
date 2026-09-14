import express from 'express';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { JsonStore } from '../shared/store.js';
import { HttpError, badRequest, forbidden } from '../shared/errors.js';
import { verifyToken, requireAuth, requireRole } from '../shared/auth.js';
import { syncOnce, pullReplica, recordFailure, exportView } from './causal-replica.js';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const PORT = process.env.PORT || 4001;
const DATA_DIR = process.env.DATA_DIR || path.join(__dirname, '..', '..', 'data', 'replica');
const JWT_SECRET = process.env.JWT_SECRET || 'dev-shared-secret-change-me';
const INTERNAL_TOKEN = process.env.INTERNAL_TOKEN || 'dev-internal-token';
const PRIMARY_URL = process.env.PRIMARY_URL || 'http://primary:4000';
const SYNC_INTERVAL_MS = Number(process.env.SYNC_INTERVAL_MS || 2000);

const initial = () => ({
  sync: {
    mode: 'normal', // normal | lag | timeout | paused
    lagPartitions: ['p2'],
    catchUpSeq: 0, // 已连续应用到的 seq（滞后恢复从断点续拉）
    fetchHigh: 0, // 诊断：曾从主库窗口看到的最大 seq
    failureCounter: 0
  },
  partitions: {},
  events: [],
  queue: [],
  cursors: {},
  consumers: {},
  failures: [],
  lastSync: null
});

const store = new JsonStore(path.join(DATA_DIR, 'state.json'), initial());

async function doSync() {
  try {
    const result = await store.mutate((draft) => syncOnce(draft, PRIMARY_URL, INTERNAL_TOKEN));
    return result;
  } catch (e) {
    // 同步超时的那一次：记失败，写清是哪一侧分区没追上
    await store.mutate((draft) => {
      const stuck = draft.lastSync?.stuckAt ?? null;
      recordFailure(draft, {
        phase: 'sync',
        code: e.code || 'SYNC_ERROR',
        message: e.message,
        laggingPartition: stuck?.partition ?? (draft.sync.mode === 'timeout' ? '(同步整体超时，无法取得分区水位)' : null),
        requiredSeq: stuck?.seq ?? null,
        replicaWatermark: stuck ? (draft.partitions[stuck.partition] ?? 0) : null,
        replicaWatermarks: { ...draft.partitions }
      });
    }).catch(() => {});
    throw e;
  }
}

const app = express();
app.use(express.json({ limit: '1mb' }));

// 消费台页面从主站 :4000 打开，跨域访问本副本
app.use((req, res, next) => {
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Headers', 'Authorization, Content-Type');
  res.setHeader('Access-Control-Allow-Methods', 'GET, POST, DELETE, OPTIONS');
  if (req.method === 'OPTIONS') return res.sendStatus(204);
  next();
});

const auth = requireAuth(JWT_SECRET);

// 存活探针（免鉴权，供容器健康检查）
app.get('/healthz', (_req, res) => {
  const d = store.read();
  res.json({ ok: true, side: 'replica', mode: d.sync.mode, mirrored: d.events.length });
});

// 副本不签发登录，只验证主站同源 JWT
app.post('/api/auth/verify', auth, (req, res) => {
  res.json({ user: { id: req.user.sub, name: req.user.name, role: req.user.role } });
});

app.get('/api/replica/status', auth, (_req, res) => {
  const d = store.read();
  res.json({
    mode: d.sync.mode,
    lagPartitions: d.sync.lagPartitions,
    catchUpSeq: d.sync.catchUpSeq,
    fetchHigh: d.sync.fetchHigh,
    primaryUrl: PRIMARY_URL,
    partitionWatermarks: d.partitions,
    mirrored: d.events.length,
    cursors: d.cursors,
    lastSync: d.lastSync,
    recentFailures: d.failures.slice(0, 10)
  });
});

// 故障注入：仅落库工（运维侧）可切；消费工/对账工越权 403
app.post('/api/replica/sync-mode', auth, requireRole('ingest'), async (req, res, next) => {
  try {
    const { mode, lagPartitions } = req.body ?? {};
    await store.mutate((draft) => {
      if (!['normal', 'lag', 'timeout', 'paused'].includes(mode)) {
        throw badRequest('BAD_MODE', 'mode 必须是 normal / lag / timeout / paused');
      }
      draft.sync.mode = mode;
      if (Array.isArray(lagPartitions)) draft.sync.lagPartitions = lagPartitions.map(String);
    });
    // 切回正常后立即尝试补一次
    if (mode === 'normal') doSync().catch(() => {});
    res.json({ ok: true, ...store.read().sync });
  } catch (e) { next(e); }
});

app.post('/api/replica/sync-now', auth, requireRole('ingest'), async (_req, res, next) => {
  try {
    const result = await doSync();
    res.json({ ok: true, ...result });
  } catch (e) {
    next(new HttpError(504, 'REPLICA_SYNC_TIMEOUT', `同步失败：${e.message}`));
  }
});

// ---- 消费拉动（只有消费工） ----
app.post('/api/consume/pull', auth, requireRole('consume'), async (req, res, next) => {
  try {
    const consumer = (req.body?.consumer || 'worker-default').trim();
    const limit = Math.min(Number(req.body?.limit) || 50, 500);

    // 先尝试把主库新事件同步到本副本
    let syncError = null;
    try {
      await doSync();
    } catch (e) {
      syncError = e;
    }

    const result = await store.mutate((draft) => {
      const r = pullReplica(draft, consumer, limit);
      // 同步超时的这一次：放行记失败，并写清是哪一侧分区没追上
      if (syncError) {
        const stuck = draft.lastSync?.stuckAt;
        recordFailure(draft, {
          phase: 'release',
          code: 'DELIVERY_BLOCKED',
          consumer,
          atSeq: r.gap?.atSeq ?? draft.sync.catchUpSeq + 1,
          event: r.gap?.event ?? null,
          laggingPartition: stuck?.partition ?? '(同步整体超时，无法取得分区水位)',
          requiredSeq: r.gap?.requiredSeq ?? stuck?.seq ?? null,
          replicaWatermark: r.gap?.replicaWatermark ?? (stuck ? (draft.partitions[stuck.partition] ?? 0) : 0),
          message: `副本同步超时的这一次：前驱未齐，放行记失败，禁止把后继标为已送达（消费者 ${consumer} 停在 seq ${r.cursor}）`
        });
      }
      return { ...r, syncError: syncError ? { code: syncError.code || 'SYNC_ERROR', message: syncError.message } : null };
    });

    if (syncError) {
      const stuckPartition = store.read().lastSync?.stuckAt?.partition ?? null;
      return res.status(409).json({
        ...result,
        error: {
          code: 'DELIVERY_BLOCKED',
          message: '副本同步超时的这一次，放行记失败；前驱未齐，未标记任何送达',
          laggingPartition: result.gap?.laggingPartition ?? stuckPartition,
          requiredSeq: result.gap?.requiredSeq ?? null,
          replicaWatermark: result.gap?.replicaWatermark ?? null
        }
      });
    }
    res.json(result);
  } catch (e) { next(e); }
});

app.get('/api/consume/cursors', auth, requireRole('consume'), (_req, res) => {
  res.json({ cursors: store.read().cursors });
});

app.post('/api/consume/cursors', auth, (_req, _res, next) =>
  next(new HttpError(405, 'CURSOR_NOT_MUTABLE', '消费游标只能通过拉动放行推进，不能直接改写')));

// 副本侧导出：只含已镜像且并发已裁完的
app.get('/api/export', auth, requireRole('ingest'), (_req, res) => {
  res.json(exportView(store.read()));
});

// 消费工追加/裁决、对账工追加等越权请求：副本上根本不存在这些业务路由；
// 为便于页面演示，明确给出 405/403 而不是 404
app.post('/api/events/batch', auth, (_req, _res, next) =>
  next(new HttpError(405, 'REPLICA_READ_ONLY', '副本只读，不能追加业务事件')));
// 封条只在落库台主侧贴/揭；副本只读。消费工/对账工在主侧本就是 403，这里同样先按角色拒
const sealStub = (req, _res, next) => {
  if (req.user.role === 'ingest') {
    return next(new HttpError(405, 'REPLICA_READ_ONLY', '停写封条只在落库台主侧贴/揭，副本只读'));
  }
  return next(forbidden('ROLE_DENIED', '只有落库工可以贴/揭停写封条'));
};
app.post('/api/seals', auth, sealStub);
app.delete('/api/seals/:root', auth, sealStub);
app.post('/api/queue/:id/adjudicate', auth, (req, _res, next) => {
  if (req.user.role === 'reconcile') {
    return next(new HttpError(405, 'REPLICA_READ_ONLY', '裁决只在落库台主侧进行，副本只读'));
  }
  return next(forbidden('ROLE_DENIED', '只有对账工可以裁决并发'));
});

app.use((err, _req, res, _next) => {
  if (err instanceof HttpError) {
    return res.status(err.status).json({ error: { code: err.code, message: err.message, ...(err.extra || {}) } });
  }
  console.error('[replica unhandled]', err);
  res.status(500).json({ error: { code: 'INTERNAL', message: err.message || '内部错误' } });
});

store.load().then(() => {
  app.listen(PORT, () => console.log(`[replica 消费台] http://localhost:${PORT}  primary=${PRIMARY_URL}  data=${DATA_DIR}`));
  // 后台周期性同步；失败已在 doSync 内记账
  const tick = () => doSync().catch(() => {});
  setInterval(tick, SYNC_INTERVAL_MS).unref();
  setTimeout(tick, 400).unref();
});
