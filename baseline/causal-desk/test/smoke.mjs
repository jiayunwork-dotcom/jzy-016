// 端到端冒烟：真实拉起 primary + replica 两个进程，逐条验证因果规则。
import { spawn } from 'node:child_process';
import { mkdtempSync } from 'node:fs';
import { tmpdir } from 'node:os';
import path from 'node:path';

const ROOT = path.resolve(new URL('..', import.meta.url).pathname);
const dir = mkdtempSync(path.join(tmpdir(), 'causal-desk-'));
const P_URL = 'http://127.0.0.1:4310';
const R_URL = 'http://127.0.0.1:4311';
const JWT = 'test-secret';
const INT = 'test-internal';

let pass = 0;
let fail = 0;
function check(name, cond, extra = '') {
  if (cond) { pass++; console.log(`  ✅ ${name}`); }
  else { fail++; console.log(`  ❌ ${name} ${extra}`); }
}

const env = {
  ...process.env,
  JWT_SECRET: JWT,
  INTERNAL_TOKEN: INT,
  SEED_DEMO: 'true',
  SNAPSHOT_AUTO_MS: '200'
};

const primary = spawn('node', ['src/primary/server.js'], {
  cwd: ROOT,
  env: { ...env, PORT: '4310', DATA_DIR: path.join(dir, 'primary') },
  stdio: ['ignore', 'pipe', 'inherit']
});
const replica = spawn('node', ['src/replica/server.js'], {
  cwd: ROOT,
  env: {
    ...env,
    PORT: '4311',
    DATA_DIR: path.join(dir, 'replica'),
    PRIMARY_URL: P_URL,
    SYNC_INTERVAL_MS: '60000', // 测试里手动触发同步，避免抖动
    REPLICA_SYNC_TIMEOUT_MS: '1500'
  },
  stdio: ['ignore', 'pipe', 'inherit']
});

async function req(base, method, p, { token, body, rawToken } = {}) {
  const headers = { 'Content-Type': 'application/json' };
  const t = rawToken !== undefined ? rawToken : token;
  if (t) headers.Authorization = `Bearer ${t}`;
  const res = await fetch(base + p, { method, headers, body: body ? JSON.stringify(body) : undefined });
  const data = await res.json().catch(() => ({}));
  return { status: res.status, data };
}

async function waitFor(url, tries = 40) {
  for (let i = 0; i < tries; i++) {
    try { const r = await fetch(url); if (r.ok) return; } catch {}
    await new Promise((r) => setTimeout(r, 250));
  }
  throw new Error(`服务没起来: ${url}`);
}

async function login(username, password) {
  const r = await req(P_URL, 'POST', '/api/auth/login', { rawToken: null, body: { username, password } });
  if (r.status !== 200) throw new Error(`登录失败 ${username}: ${r.status}`);
  return r.data.token;
}

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

try {
  await waitFor(P_URL + '/api/state').catch(() => {}); // 未登录会 401，端口在就行
  // 端口探测改用登录接口
  for (let i = 0; i < 40; i++) {
    const r = await req(P_URL, 'POST', '/api/auth/login', { body: { username: 'ingest', password: 'ing123' } });
    if (r.status === 200) break;
    if (i === 39) throw new Error('primary 未就绪');
    await sleep(250);
  }
  const TI = await login('ingest', 'ing123');
  const TC = await login('consume', 'con123');
  const TR = await login('reconcile', 'rec123');

  console.log('\n== 1. 没登录谁也打不开流 ==');
  {
    const r = await req(P_URL, 'GET', '/api/state');
    check('无 token 读 /api/state => 401', r.status === 401, r.status);
    const r2 = await req(P_URL, 'POST', '/api/events/batch', { body: { items: [] } });
    check('无 token 追加 => 401', r2.status === 401, r2.status);
    const r3 = await req(R_URL, 'GET', '/api/replica/status');
    check('无 token 副本状态 => 401', r3.status === 401, r3.status);
    const bad = await req(P_URL, 'GET', '/api/state', { rawToken: 'not-a-jwt' });
    check('坏 token => 401', bad.status === 401, bad.status);
  }

  console.log('\n== 2. 种子历史与向量时钟 ==');
  let baseSeq;
  {
    const s = (await req(P_URL, 'GET', '/api/state', { token: TI })).data;
    baseSeq = s.events.length;
    check(`种子事件 6 条（实际 ${s.events.length}）`, s.events.length === 6);
    check('种子未决并发 2 对（order-100/200 各一）', s.queue.filter((q) => !q.verdict).length === 2);
    const paid = s.events.find((e) => e.id === 'e-order100-paid');
    check('paid 时钟 = p1:1 p2:1（并前驱再自增）',
      JSON.stringify(paid.clock) === JSON.stringify({ p1: 1, p2: 1 }), JSON.stringify(paid.clock));
    const shipped = s.events.find((e) => e.id === 'e-order100-shipped');
    check('shipped 时钟 = p1:1 p3:1（另一条分支，与 paid 互不可比）',
      JSON.stringify(shipped.clock) === JSON.stringify({ p1: 1, p3: 1 }), JSON.stringify(shipped.clock));
  }

  console.log('\n== 3. 追加：声明前驱不在库，整批退 ==');
  {
    const r = await req(P_URL, 'POST', '/api/events/batch', { token: TI, body: {
      items: [{ id: 'e-x1', root: 'order-100', partition: 'p1', payload: { x: 1 }, preds: ['e-ghost-not-in-store'] }]
    } });
    check('缺前驱 => 400 PREDECESSOR_MISSING', r.status === 400 && r.data.error.code === 'PREDECESSOR_MISSING', JSON.stringify(r.data.error));
    const s = (await req(P_URL, 'GET', '/api/state', { token: TI })).data;
    check('整批退回：事件数不变', s.events.length === baseSeq);
    check('退回有审计记录', s.abortedBatches.some((b) => b.reasons.some((x) => x.code === 'PREDECESSOR_MISSING')));
  }

  console.log('\n== 4. 批内对外依赖齐：批内前驱 + 整批原子提交 ==');
  {
    const r = await req(P_URL, 'POST', '/api/events/batch', { token: TI, body: {
      items: [
        { id: 'e-300-a', root: 'order-300', partition: 'p1', payload: { step: 1 }, preds: [] },
        { id: 'e-300-b', root: 'order-300', partition: 'p2', payload: { step: 2 }, preds: ['e-300-a'] }
      ]
    } });
    check('批内前驱齐 => 201', r.status === 201, r.status);
    check('两条都进', r.data.committed.length === 2);
    check('拓扑序 seq 递增，前驱 seq 更小', r.data.committed[0].id === 'e-300-a');
    check('后继时钟含 p1 最大值且 p2 自增',
      JSON.stringify(r.data.committed[1].clock) === JSON.stringify({ p1: 1, p2: 1 }),
      JSON.stringify(r.data.committed[1].clock));
    baseSeq += 2;
  }

  console.log('\n== 5. 批内成环 / 同根两笔互不可比进队列 ==');
  {
    const cyc = await req(P_URL, 'POST', '/api/events/batch', { token: TI, body: {
      items: [
        { id: 'e-c1', root: 'order-400', partition: 'p1', payload: {}, preds: ['e-c2'] },
        { id: 'e-c2', root: 'order-400', partition: 'p1', payload: {}, preds: ['e-c1'] }
      ]
    } });
    check('批内成环 => 400 CYCLE_IN_BATCH，整批退', cyc.status === 400 && cyc.data.error.code === 'CYCLE_IN_BATCH', JSON.stringify(cyc.data.error));

    const con = await req(P_URL, 'POST', '/api/events/batch', { token: TI, body: {
      items: [{ id: 'e-400-x', root: 'order-400', partition: 'p2', payload: { branch: 'x' }, preds: [] }]
    } });
    await req(P_URL, 'POST', '/api/events/batch', { token: TI, body: {
      items: [{ id: 'e-400-y', root: 'order-400', partition: 'p3', payload: { branch: 'y' }, preds: [] }]
    } });
    const s = (await req(P_URL, 'GET', '/api/state', { token: TI })).data;
    check('同根两笔时钟互不可比 => 进并发队列，不自动并条',
      s.queue.some((q) => !q.verdict && new Set([q.a, q.b]).has('e-400-x') && new Set([q.a, q.b]).has('e-400-y')));
    check('两条事件都保留为独立事件', s.events.filter((e) => ['e-400-x', 'e-400-y'].includes(e.id)).length === 2);
    baseSeq += 2;
  }

  console.log('\n== 6. 通道断开：不能新追加，在库按最后有效时钟守序 ==');
  {
    await req(P_URL, 'POST', '/api/admin/channel', { token: TI, body: { up: false } });
    const r = await req(P_URL, 'POST', '/api/events/batch', { token: TI, body: {
      items: [{ id: 'e-down1', root: 'order-500', partition: 'p1', payload: {}, preds: [] }]
    } });
    check('通道断开追加 => 409 CHANNEL_DOWN', r.status === 409 && r.data.error.code === 'CHANNEL_DOWN', r.status);
    const s = (await req(P_URL, 'GET', '/api/state', { token: TI })).data;
    check('在库事件不受影响', s.events.length === baseSeq && s.channelUp === false);
    await req(P_URL, 'POST', '/api/admin/channel', { token: TI, body: { up: true } });
  }

  console.log('\n== 7. RBAC：页面拦、后台也拒 ==');
  {
    const r1 = await req(P_URL, 'POST', '/api/events/batch', { token: TC, body: { items: [] } });
    check('消费工追加 => 403', r1.status === 403, r1.status);
    const r2 = await req(P_URL, 'POST', '/api/events/batch', { token: TR, body: { items: [] } });
    check('对账工追加业务事件 => 403', r2.status === 403, r2.status);
    const r3 = await req(P_URL, 'POST', '/api/queue/q-nonexistent/adjudicate', { token: TI, body: { winner: 'a' } });
    check('落库工裁决并发 => 403（角色先于资源校验）', r3.status === 403, r3.status);
    const r4 = await req(P_URL, 'POST', '/api/queue/q-nonexistent/adjudicate', { token: TC, body: { winner: 'a' } });
    check('消费工裁决并发 => 403（角色先于资源校验）', r4.status === 403, r4.status);
    const r5 = await req(P_URL, 'PUT', '/api/consume/cursors/worker-A', { token: TI, body: { lastSeq: 99 } });
    check('落库工改写消费游标 => 405', r5.status === 405, r5.status);
    const r6 = await req(P_URL, 'PUT', '/api/consume/cursors/worker-A', { token: TR, body: { lastSeq: 99 } });
    check('对账工动消费游标 => 405', r6.status === 405, r6.status);
    const r7 = await req(P_URL, 'PATCH', '/api/events/e-order100-created', { token: TI, body: { preds: ['x'] } });
    check('修改别人的前驱声明 => 405', r7.status === 405, r7.status);
  }

  console.log('\n== 8. 对账裁决：只有对账工；裁完才可导出 ==');
  {
    // 导出前：两个种子未决对 + e-400 对都挡住
    const before = (await req(P_URL, 'GET', '/api/export', { token: TI })).data;
    check('导出视图存在', !!before.events);
    check('未决并发两侧不导出', before.blocked.some((b) => b.id === 'e-order200-warehouse'));
    check('并发对的后继也不导出（传递挡住）', before.blocked.some((b) => b.id === 'e-order200-signed'));

    const decideAll = async (token) => {
      const q = (await req(P_URL, 'GET', '/api/queue', { token: TI })).data.queue.filter((x) => !x.verdict);
      for (const x of q) {
        const r = await req(P_URL, 'POST', `/api/queue/${x.id}/adjudicate`, { token, body: { winner: 'a' } });
        if (r.status !== 200) return r;
      }
      return { status: 200 };
    };
    // 先错误密码/角色验证已经做过；这里直接对账工裁
    const ok = await decideAll(TR);
    check('对账工裁掉全部未决对 => 全 200', ok.status === 200, ok.status);

    const q = (await req(P_URL, 'GET', '/api/queue', { token: TR })).data.queue[0];
    const again = await req(P_URL, 'POST', `/api/queue/${q.id}/adjudicate`, { token: TR, body: { winner: 'b' } });
    check('已裁决不能改判 => 409', again.status === 409, again.status);

    const after = (await req(P_URL, 'GET', '/api/export', { token: TI })).data;
    check('裁完后无阻挡，全部可导出', after.counts.blocked === 0, JSON.stringify(after.blocked));
    check('败方仍作为独立事件导出（不并条）', after.events.some((e) => e.id === 'e-order200-warehouse'));
  }

  console.log('\n== 9. 主侧消费：只看前驱可见，游标停缺口前 ==');
  {
    // 新消费者：种子事件 seq1(created100), seq2(created200) 无前驱；seq3 paid 的前驱 seq1 已越过 => 放行
    const r1 = await req(P_URL, 'POST', '/api/consume/pull', { token: TC, body: { consumer: 'w-primary', limit: 100 } });
    check('主侧首次拉取全部已齐事件', r1.status === 200 && r1.data.delivered.length === baseSeq, `${r1.status}/${r1.data.delivered?.length}`);
    check('游标到末端', r1.data.cursor === baseSeq);
    const r2 = await req(P_URL, 'POST', '/api/consume/pull', { token: TC, body: { consumer: 'w-primary', limit: 100 } });
    check('没有更多时游标不动、不报错', r2.status === 200 && r2.data.delivered.length === 0 && r2.data.cursor === baseSeq);
  }

  console.log('\n== 10. 快照：同根不能同时两份 ==');
  {
    const s1 = await req(P_URL, 'POST', '/api/snapshots', { token: TI, body: { root: 'order-100' } });
    check('第一份快照 => 201', s1.status === 201, s1.status);
    const s2 = await req(P_URL, 'POST', '/api/snapshots', { token: TI, body: { root: 'order-100' } });
    check('同根进行中再打 => 409 SNAPSHOT_IN_PROGRESS', s2.status === 409 && s2.data.error.code === 'SNAPSHOT_IN_PROGRESS', s2.status);
    const s3 = await req(P_URL, 'POST', '/api/snapshots', { token: TI, body: { root: 'order-200' } });
    check('不同根可以打', s3.status === 201, s3.status);
    await sleep(400);
    const s4 = await req(P_URL, 'POST', '/api/snapshots', { token: TI, body: { root: 'order-100' } });
    check('完成后可重新打', s4.status === 201, s4.status);
  }

  console.log('\n== 11. 副本：正常同步后消费完整可见 ==');
  {
    await req(R_URL, 'POST', '/api/replica/sync-mode', { token: TI, body: { mode: 'normal' } });
    const sync = await req(R_URL, 'POST', '/api/replica/sync-now', { token: TI });
    check('手动同步成功', sync.status === 200, sync.status);
    const st = (await req(R_URL, 'GET', '/api/replica/status', { token: TC })).data;
    check(`副本镜像 ${baseSeq} 条`, st.mirrored === baseSeq, st.mirrored);
    const pull = await req(R_URL, 'POST', '/api/consume/pull', { token: TC, body: { consumer: 'w-rep', limit: 100 } });
    check('副本消费全部放完', pull.status === 200 && pull.data.delivered.length === baseSeq, `${pull.status}/${pull.data.delivered?.length}`);
  }

  console.log('\n== 12. 跨分区滞后：p2 看不见 => p3 后继对副本消费者不可见 ==');
  {
    // 先在主库追加一条 p2 后继 + 一条依赖它的 p3 后继（order-600）
    const a = await req(P_URL, 'POST', '/api/events/batch', { token: TI, body: {
      items: [{ id: 'e-600-p2', root: 'order-600', partition: 'p2', payload: { n: 1 }, preds: [] }]
    } });
    check('p2 事件落库', a.status === 201, a.status);
    const seqP2 = a.data.committed[0].seq;
    const b = await req(P_URL, 'POST', '/api/events/batch', { token: TI, body: {
      items: [{ id: 'e-600-p3', root: 'order-600', partition: 'p3', payload: { n: 2 }, preds: ['e-600-p2'] }]
    } });
    check('p3 后继（跨分区前驱）落库', b.status === 201, b.status);
    baseSeq += 2;

    await req(R_URL, 'POST', '/api/replica/sync-mode', { token: TI, body: { mode: 'lag', lagPartitions: ['p2'] } });
    const sync = await req(R_URL, 'POST', '/api/replica/sync-now', { token: TI });
    check('同步返回 200（p2 被滞后过滤）', sync.status === 200, sync.status);

    const st = (await req(R_URL, 'GET', '/api/replica/status', { token: TC })).data;
    check(`副本连续应用到 seq ${seqP2 - 1}，p2 新事件（seq ${seqP2}）被卡点挡住`,
      st.catchUpSeq === seqP2 - 1 && (st.partitionWatermarks.p2 ?? 0) <= seqP2 - 1,
      JSON.stringify({ catchUpSeq: st.catchUpSeq, w: st.partitionWatermarks }));
    check('同步结果带卡点 stuckAt=p2', st.lastSync?.stuckAt?.partition === 'p2' && st.lastSync.stuckAt.seq === seqP2,
      JSON.stringify(st.lastSync));

    const pull = await req(R_URL, 'POST', '/api/consume/pull', { token: TC, body: { consumer: 'w-rep', limit: 100 } });
    check('副本消费 200（滞后是等待，不是假成功）', pull.status === 200, pull.status);
    check('没有把后继标成已送达（delivered 不含 p3 事件）',
      !pull.data.delivered.some((e) => e.id === 'e-600-p3'));
    check('游标停在缺口前（缺口为新 p2 事件 seq）', pull.data.blockedAt === seqP2,
      `blockedAt=${pull.data.blockedAt} expect=${seqP2}`);
    check('写明是 p2 分区没追上', pull.data.gap?.laggingPartition === 'p2', JSON.stringify(pull.data.gap));
    check('游标没有跳过缺口', pull.data.cursor === seqP2 - 1, `cursor=${pull.data.cursor}`);
  }

  console.log('\n== 13. 副本同步超时：放行记失败，写哪侧分区没追上 ==');
  {
    await req(R_URL, 'POST', '/api/replica/sync-mode', { token: TI, body: { mode: 'timeout' } });
    const pull = await req(R_URL, 'POST', '/api/consume/pull', { token: TC, body: { consumer: 'w-rep', limit: 100 } });
    check('超时这一次放行 => 409 DELIVERY_BLOCKED', pull.status === 409 && pull.data.error?.code === 'DELIVERY_BLOCKED', `${pull.status}/${pull.data.error?.code}`);
    check('响应里带没追上的分区与水位', pull.data.gap?.laggingPartition === 'p2' || pull.data.error?.laggingPartition === 'p2',
      JSON.stringify(pull.data.gap || pull.data.error));
    const st = (await req(R_URL, 'GET', '/api/replica/status', { token: TC })).data;
    check('失败记录落账（release 阶段）', st.recentFailures.some((f) => f.phase === 'release' && f.code === 'DELIVERY_BLOCKED'));
    check('仍有同步阶段失败记录', st.recentFailures.some((f) => f.phase === 'sync'));

    // 恢复：p2 补齐后游标一次跨过
    await req(R_URL, 'POST', '/api/replica/sync-mode', { token: TI, body: { mode: 'normal' } });
    const pull2 = await req(R_URL, 'POST', '/api/consume/pull', { token: TC, body: { consumer: 'w-rep', limit: 100 } });
    check('滞后恢复后修复窗口补齐，后继放行', pull2.status === 200 && pull2.data.delivered.some((e) => e.id === 'e-600-p3'),
      `${pull2.status}/${pull2.data.delivered?.map((e) => e.id).join(',')}`);
    check('游标到达末端', pull2.data.cursor === baseSeq, `${pull2.data.cursor}/${baseSeq}`);
  }

  console.log('\n== 14. 副本只读 + 跨角色在副本侧也被拒 ==');
  {
    const r1 = await req(R_URL, 'POST', '/api/events/batch', { token: TI, body: { items: [] } });
    check('落库工在副本追加 => 405 REPLICA_READ_ONLY', r1.status === 405, r1.status);
    const r2 = await req(R_URL, 'POST', '/api/replica/sync-mode', { token: TC, body: { mode: 'paused' } });
    check('消费工改同步模式 => 403', r2.status === 403, r2.status);
    const r3 = await req(R_URL, 'POST', '/api/consume/pull', { token: TR, body: { consumer: 'x' } });
    check('对账工在副本消费 => 403', r3.status === 403, r3.status);
  }

  console.log(`\n结果：${pass} 通过，${fail} 失败`);
  if (fail) process.exitCode = 1;
} catch (e) {
  console.error('测试异常:', e);
  process.exitCode = 1;
} finally {
  primary.kill();
  replica.kill();
  setTimeout(() => process.exit(process.exitCode ? 1 : 0), 300);
}
