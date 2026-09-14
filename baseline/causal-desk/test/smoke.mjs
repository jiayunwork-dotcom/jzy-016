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

  console.log('\n== 11. 停写封条：封根不封通道 ==');
  {
    // 准备：order-700 一条在库事件（贴封条的对象必须在库）
    const prep = await req(P_URL, 'POST', '/api/events/batch', { token: TI, body: {
      items: [{ id: 'e-700-created', root: 'order-700', partition: 'p1', payload: { n: 1 }, preds: [] }]
    } });
    check('准备 order-700 在库事件', prep.status === 201, prep.status);
    baseSeq += 1;

    const noAuth = await req(P_URL, 'POST', '/api/seals', { body: { root: 'order-700' } });
    check('未登录贴封条 => 401', noAuth.status === 401, noAuth.status);
    const c1 = await req(P_URL, 'POST', '/api/seals', { token: TC, body: { root: 'order-700' } });
    check('消费工贴封条 => 403', c1.status === 403, c1.status);
    const c2 = await req(P_URL, 'POST', '/api/seals', { token: TR, body: { root: 'order-700' } });
    check('对账工贴封条 => 403', c2.status === 403, c2.status);
    const c3 = await req(P_URL, 'DELETE', '/api/seals/order-700', { token: TC });
    check('消费工揭封条 => 403', c3.status === 403, c3.status);
    const c4 = await req(P_URL, 'DELETE', '/api/seals/order-700', { token: TR });
    check('对账工揭封条 => 403', c4.status === 403, c4.status);

    const ghost = await req(P_URL, 'POST', '/api/seals', { token: TI, body: { root: 'order-ghost' } });
    check('不在库的根贴封条 => 404 ROOT_NOT_IN_STORE', ghost.status === 404 && ghost.data.error.code === 'ROOT_NOT_IN_STORE', JSON.stringify(ghost.data.error));

    const seal = await req(P_URL, 'POST', '/api/seals', { token: TI, body: { root: 'order-700' } });
    check('落库工贴封条 => 201', seal.status === 201, seal.status);
    check('封条记录根与贴条人', seal.data.seal?.root === 'order-700' && !!seal.data.seal?.sealedByName, JSON.stringify(seal.data));
    const dup = await req(P_URL, 'POST', '/api/seals', { token: TI, body: { root: 'order-700' } });
    check('同一根再贴 => 409 SEAL_EXISTS（不能同时两份）', dup.status === 409 && dup.data.error.code === 'SEAL_EXISTS', JSON.stringify(dup.data.error));

    // 被封的根整批提交：拦住并写明是哪个根；没封的根照常
    const blocked = await req(P_URL, 'POST', '/api/events/batch', { token: TI, body: {
      items: [{ id: 'e-700-blocked', root: 'order-700', partition: 'p2', payload: { n: 2 }, preds: ['e-700-created'] }]
    } });
    check('被封根整批提交 => 409 ROOT_SEALED', blocked.status === 409 && blocked.data.error.code === 'ROOT_SEALED', JSON.stringify(blocked.data.error));
    check('报错写明是哪个根被封', (blocked.data.error.sealedRoots ?? []).includes('order-700') && blocked.data.error.message.includes('order-700'), JSON.stringify(blocked.data.error));
    let s = (await req(P_URL, 'GET', '/api/state', { token: TI })).data;
    check('被拦的批一条都没进库', s.events.length === baseSeq && !s.events.some((e) => e.id === 'e-700-blocked'));
    const other = await req(P_URL, 'POST', '/api/events/batch', { token: TI, body: {
      items: [{ id: 'e-300-c', root: 'order-300', partition: 'p1', payload: { n: 3 }, preds: ['e-300-b'] }]
    } });
    check('没封的根照常追加 => 201', other.status === 201, other.status);
    baseSeq += 1;

    // 在库旧事件不藏：消费照常拉动、导出照常带
    const pull = await req(P_URL, 'POST', '/api/consume/pull', { token: TC, body: { consumer: 'w-seal', limit: 100 } });
    check('被封根的旧事件照常拉动', pull.status === 200 && pull.data.delivered.some((e) => e.id === 'e-700-created'), `${pull.status}`);
    check('封条不拦消费，游标到末端', pull.data.cursor === baseSeq, `${pull.data.cursor}/${baseSeq}`);
    const exp = (await req(P_URL, 'GET', '/api/export', { token: TI })).data;
    check('导出仍带被封根的旧事件', exp.events.some((e) => e.id === 'e-700-created'));

    // 对账工还能裁被封根上已有的并发对
    await req(P_URL, 'POST', '/api/events/batch', { token: TI, body: { items: [{ id: 'e-800-x', root: 'order-800', partition: 'p2', payload: {}, preds: [] }] } });
    await req(P_URL, 'POST', '/api/events/batch', { token: TI, body: { items: [{ id: 'e-800-y', root: 'order-800', partition: 'p3', payload: {}, preds: [] }] } });
    baseSeq += 2;
    await req(P_URL, 'POST', '/api/seals', { token: TI, body: { root: 'order-800' } });
    s = (await req(P_URL, 'GET', '/api/state', { token: TI })).data;
    const pair800 = s.queue.find((q) => !q.verdict && q.root === 'order-800');
    const adj = await req(P_URL, 'POST', `/api/queue/${pair800.id}/adjudicate`, { token: TR, body: { winner: 'a' } });
    check('被封根上已有的并发对照常裁决 => 200', adj.status === 200, adj.status);

    // 通道断开：贴/揭都做不成；已贴的封条继续拦
    await req(P_URL, 'POST', '/api/admin/channel', { token: TI, body: { up: false } });
    const sDown = await req(P_URL, 'POST', '/api/seals', { token: TI, body: { root: 'order-300' } });
    check('通道断开时贴封条 => 409 CHANNEL_DOWN', sDown.status === 409 && sDown.data.error.code === 'CHANNEL_DOWN', JSON.stringify(sDown.data.error));
    const uDown = await req(P_URL, 'DELETE', '/api/seals/order-700', { token: TI });
    check('通道断开时揭封条 => 409 CHANNEL_DOWN', uDown.status === 409 && uDown.data.error.code === 'CHANNEL_DOWN', JSON.stringify(uDown.data.error));
    s = (await req(P_URL, 'GET', '/api/state', { token: TI })).data;
    check('通道断开期间已贴封条仍在', !!s.seals['order-700'] && !!s.seals['order-800'], JSON.stringify(s.seals));
    await req(P_URL, 'POST', '/api/admin/channel', { token: TI, body: { up: true } });
    const still = await req(P_URL, 'POST', '/api/events/batch', { token: TI, body: {
      items: [{ id: 'e-700-blocked2', root: 'order-700', partition: 'p2', payload: { n: 2 }, preds: ['e-700-created'] }]
    } });
    check('通道恢复后封条继续拦 => 409 ROOT_SEALED', still.status === 409 && still.data.error.code === 'ROOT_SEALED', still.status);

    // 揭掉封条才能再交；被拦的那批不偷偷补进
    const un = await req(P_URL, 'DELETE', '/api/seals/order-700', { token: TI });
    check('落库工揭封条 => 200', un.status === 200, un.status);
    s = (await req(P_URL, 'GET', '/api/state', { token: TI })).data;
    check('揭封后库里仍没有被拦过的批（不偷偷补进）', s.events.length === baseSeq && !s.events.some((e) => e.id === 'e-700-blocked' || e.id === 'e-700-blocked2'));
    const after = await req(P_URL, 'POST', '/api/events/batch', { token: TI, body: {
      items: [{ id: 'e-700-after', root: 'order-700', partition: 'p2', payload: { n: 2 }, preds: ['e-700-created'] }]
    } });
    check('揭封后该根可再交新事件 => 201', after.status === 201, after.status);
    baseSeq += 1;
    await req(P_URL, 'DELETE', '/api/seals/order-800', { token: TI }); // 收尾，别影响后面的副本演练
  }

  console.log('\n== 12. 按根暂扣：只挡放行，不挡入库 ==');
  {
    // 准备：order-900 两条链式事件（a → b）
    const p1 = await req(P_URL, 'POST', '/api/events/batch', { token: TI, body: {
      items: [
        { id: 'e-900-a', root: 'order-900', partition: 'p1', payload: { step: 'a' }, preds: [] },
        { id: 'e-900-b', root: 'order-900', partition: 'p2', payload: { step: 'b' }, preds: ['e-900-a'] }
      ]
    } });
    check('准备 order-900 两条在库事件', p1.status === 201, p1.status);
    baseSeq += 2;
    const seq900c_expect = baseSeq + 1; // 下一条 order-900 事件的 seq

    // 扣之前先放到：消费者 w-hold 把这两条拉走（已放到的不改口）
    const pre = await req(P_URL, 'POST', '/api/consume/pull', { token: TC, body: { consumer: 'w-hold', limit: 100 } });
    check('扣之前先放到 e-900-a/b', pre.status === 200 && pre.data.delivered.some((e) => e.id === 'e-900-b'), `${pre.status}`);

    // 没登录谁也动不了暂扣；落库工/对账工按不了也解不了
    const noAuth = await req(P_URL, 'POST', '/api/holds', { body: { root: 'order-900' } });
    check('未登录按暂扣 => 401', noAuth.status === 401, noAuth.status);
    const noAuth2 = await req(P_URL, 'DELETE', '/api/holds/order-900');
    check('未登录解暂扣 => 401', noAuth2.status === 401, noAuth2.status);
    const hi = await req(P_URL, 'POST', '/api/holds', { token: TI, body: { root: 'order-900' } });
    check('落库工按暂扣 => 403', hi.status === 403, hi.status);
    const hi2 = await req(P_URL, 'DELETE', '/api/holds/order-900', { token: TI });
    check('落库工解暂扣 => 403', hi2.status === 403, hi2.status);
    const hr = await req(P_URL, 'POST', '/api/holds', { token: TR, body: { root: 'order-900' } });
    check('对账工按暂扣 => 403', hr.status === 403, hr.status);
    const hr2 = await req(P_URL, 'DELETE', '/api/holds/order-900', { token: TR });
    check('对账工解暂扣 => 403', hr2.status === 403, hr2.status);
    const ghost = await req(P_URL, 'POST', '/api/holds', { token: TC, body: { root: 'order-ghost' } });
    check('不在库的根按暂扣 => 404 ROOT_NOT_IN_STORE', ghost.status === 404 && ghost.data.error.code === 'ROOT_NOT_IN_STORE', JSON.stringify(ghost.data.error));

    // 消费工按下暂扣
    const hold = await req(P_URL, 'POST', '/api/holds', { token: TC, body: { root: 'order-900' } });
    check('消费工按暂扣 => 201', hold.status === 201, JSON.stringify(hold.data));
    check('暂扣记录根与扣的人', hold.data.hold?.root === 'order-900' && !!hold.data.hold?.heldByName, JSON.stringify(hold.data));
    const dup = await req(P_URL, 'POST', '/api/holds', { token: TC, body: { root: 'order-900' } });
    check('同一根再扣 => 409 HOLD_EXISTS（同时只有一份）', dup.status === 409 && dup.data.error.code === 'HOLD_EXISTS', JSON.stringify(dup.data.error));

    // 暂扣不挡入库：这根还能继续追加；再补一条别的根（seq 更靠后）验证"别的根照常拉动"
    const p2 = await req(P_URL, 'POST', '/api/events/batch', { token: TI, body: {
      items: [{ id: 'e-900-c', root: 'order-900', partition: 'p3', payload: { step: 'c' }, preds: ['e-900-b'] }]
    } });
    check('暂扣期间该根照常追加 => 201（不挡入库）', p2.status === 201, p2.status);
    const seq900c = p2.data.committed[0].seq;
    check('新事件 seq 符合预期', seq900c === seq900c_expect, `${seq900c}/${seq900c_expect}`);
    const p3 = await req(P_URL, 'POST', '/api/events/batch', { token: TI, body: {
      items: [{ id: 'e-300-d', root: 'order-300', partition: 'p2', payload: { step: 'd' }, preds: ['e-300-c'] }]
    } });
    check('别的根也照常追加', p3.status === 201, p3.status);
    baseSeq += 2;

    // 拉动：被扣的根停在第一条未放出的事件前，不越过它装后面的；别的根照常拉动
    const pull = await req(P_URL, 'POST', '/api/consume/pull', { token: TC, body: { consumer: 'w-hold', limit: 100 } });
    check('被扣根未放出的事件不再放行', pull.status === 200 && !pull.data.delivered.some((e) => e.root === 'order-900'), JSON.stringify(pull.data.heldBack));
    check('拉动报出停在哪条前（heldBack = order-900 @ e-900-c）',
      pull.data.heldBack?.some((h) => h.root === 'order-900' && h.event === 'e-900-c' && h.atSeq === seq900c), JSON.stringify(pull.data.heldBack));
    check('不越过它装后面的：同根更晚的事件一条都没放', !pull.data.delivered.some((e) => e.id === 'e-900-c'));
    check('别的根照常拉动：seq 更靠后的 e-300-d 照常放行', pull.data.delivered.some((e) => e.id === 'e-300-d'));
    check('连续前缀水位停在被扣事件前', pull.data.cursor === seq900c - 1, `${pull.data.cursor}/${seq900c - 1}`);
    check('已经放到的不改口：e-900-a/b 不重复放行', !pull.data.delivered.some((e) => e.id === 'e-900-a' || e.id === 'e-900-b'));

    // 导出仍只带已经放到的
    const exp = (await req(P_URL, 'GET', '/api/export', { token: TI })).data;
    check('导出仍带已放到的 e-900-a/b', ['e-900-a', 'e-900-b'].every((id) => exp.events.some((e) => e.id === id)));
    check('导出挡住未放出的 e-900-c（ROOT_HELD_NOT_RELEASED）',
      exp.blocked.some((b) => b.id === 'e-900-c' && b.reason.startsWith('ROOT_HELD_NOT_RELEASED')), JSON.stringify(exp.blocked.filter((b) => b.root === 'order-900')));
    check('别的根新事件照常导出', exp.events.some((e) => e.id === 'e-300-d'));

    // 解开之后继续放行；已放到的不改口
    const un = await req(P_URL, 'DELETE', '/api/holds/order-900', { token: TC });
    check('消费工解开暂扣 => 200', un.status === 200, JSON.stringify(un.data));
    const pull2 = await req(P_URL, 'POST', '/api/consume/pull', { token: TC, body: { consumer: 'w-hold', limit: 100 } });
    check('解开后该根继续放行（e-900-c 放出）', pull2.data.delivered.some((e) => e.id === 'e-900-c'));
    check('解开后游标到末端', pull2.data.cursor === baseSeq, `${pull2.data.cursor}/${baseSeq}`);
    const exp2 = (await req(P_URL, 'GET', '/api/export', { token: TI })).data;
    check('解开后导出不再挡该根', !exp2.blocked.some((b) => b.root === 'order-900'), JSON.stringify(exp2.blocked.filter((b) => b.root === 'order-900')));

    // 解开后可再扣；副本随同步镜像暂扣，副本拉动同样拦住
    const reHold = await req(P_URL, 'POST', '/api/holds', { token: TC, body: { root: 'order-900' } });
    check('解开后可再扣 => 201', reHold.status === 201, reHold.status);
    const rp = await req(R_URL, 'POST', '/api/consume/pull', { token: TC, body: { consumer: 'w-hold-rep', limit: 200 } });
    check('副本拉动同样拦住被扣根（一条 order-900 都不放）', rp.status === 200 && !rp.data.delivered.some((e) => e.root === 'order-900'), `${rp.status}`);
    check('副本别的根照常放行', rp.data.delivered.some((e) => e.root === 'order-300'));
    check('副本也报 heldBack', rp.data.heldBack?.some((h) => h.root === 'order-900'), JSON.stringify(rp.data.heldBack));
    await req(P_URL, 'DELETE', '/api/holds/order-900', { token: TC });
    const rp2 = await req(R_URL, 'POST', '/api/consume/pull', { token: TC, body: { consumer: 'w-hold-rep', limit: 200 } });
    check('主侧解开后副本同步到，该根恢复放行', rp2.data.delivered.some((e) => e.root === 'order-900'), JSON.stringify(rp2.data.heldBack));
  }

  console.log('\n== 13. 搁置同根并发对：搁的是裁决，不是入库/拉动 ==');
  {
    // 准备：order-1100 一对未决并发（x/y），order-1200 另一对未决并发（旁边的对）
    const p1 = await req(P_URL, 'POST', '/api/events/batch', { token: TI, body: {
      items: [{ id: 'e-1100-x', root: 'order-1100', partition: 'p1', payload: { branch: 'x' }, preds: [] }]
    } });
    check('order-1100 x 落库', p1.status === 201, p1.status);
    const p2 = await req(P_URL, 'POST', '/api/events/batch', { token: TI, body: {
      items: [{ id: 'e-1100-y', root: 'order-1100', partition: 'p2', payload: { branch: 'y' }, preds: [] }]
    } });
    check('order-1100 y 落库（与 x 互不可比）', p2.status === 201, p2.status);
    const p3 = await req(P_URL, 'POST', '/api/events/batch', { token: TI, body: {
      items: [
        { id: 'e-1200-m', root: 'order-1200', partition: 'p1', payload: { branch: 'm' }, preds: [] },
        { id: 'e-1200-n', root: 'order-1200', partition: 'p2', payload: { branch: 'n' }, preds: [] }
      ]
    } });
    check('order-1200 m/n 落库（旁边一对）', p3.status === 201, p3.status);
    baseSeq += 4;
    let s = (await req(P_URL, 'GET', '/api/state', { token: TR })).data;
    const q1100 = s.queue.find((q) => q.root === 'order-1100' && new Set([q.a, q.b]).size === 2 && q.a.includes('1100'));
    const q1200 = s.queue.find((q) => q.root === 'order-1200');
    check('找到 order-1100 / order-1200 未决对', !!q1100 && !!q1200, JSON.stringify(s.queue.map((q) => q.id)));
    check('新对默认没搁置', !q1100.shelveId && !q1200.shelveId);

    // ---- 13a. 没登录谁也动不了搁置 ----
    const na1 = await req(P_URL, 'POST', `/api/queue/${q1100.id}/shelve`, { body: {} });
    check('未登录搁置 => 401', na1.status === 401, na1.status);
    const na2 = await req(P_URL, 'DELETE', `/api/queue/${q1100.id}/shelve`);
    check('未登录解开搁置 => 401', na2.status === 401, na2.status);

    // ---- 13b. 落库工、消费工工位没有这枚钮：接口也拒 ----
    const i1 = await req(P_URL, 'POST', `/api/queue/${q1100.id}/shelve`, { token: TI, body: {} });
    check('落库工搁置 => 403', i1.status === 403, i1.status);
    const i2 = await req(P_URL, 'DELETE', `/api/queue/${q1100.id}/shelve`, { token: TI });
    check('落库工解开搁置 => 403', i2.status === 403, i2.status);
    const c1 = await req(P_URL, 'POST', `/api/queue/${q1100.id}/shelve`, { token: TC, body: {} });
    check('消费工搁置 => 403', c1.status === 403, c1.status);
    const c2 = await req(P_URL, 'DELETE', `/api/queue/${q1100.id}/shelve`, { token: TC });
    check('消费工解开搁置 => 403', c2.status === 403, c2.status);

    // ---- 13c. 已有裁决结果的对不能搁置 ----
    const adj1200 = await req(P_URL, 'POST', `/api/queue/${q1200.id}/adjudicate`, { token: TR, body: { winner: 'a' } });
    check('先裁掉旁边 order-1200 对 => 200', adj1200.status === 200, adj1200.status);
    const shDecided = await req(P_URL, 'POST', `/api/queue/${q1200.id}/shelve`, { token: TR, body: {} });
    check('给已裁决的对搁置 => 409 ALREADY_DECIDED',
      shDecided.status === 409 && shDecided.data.error.code === 'ALREADY_DECIDED', JSON.stringify(shDecided.data.error));

    // ---- 13d. 对账工搁置未决对：201，一对未决同时只能搁一份 ----
    const sh1 = await req(P_URL, 'POST', `/api/queue/${q1100.id}/shelve`, { token: TR, body: {} });
    check('对账工搁置 => 201', sh1.status === 201, sh1.status);
    check('搁置记录 g 编号与搁置人', sh1.data.queue?.shelveId?.startsWith('g') && sh1.data.queue.shelvedByName, JSON.stringify(sh1.data.queue));
    const sh2 = await req(P_URL, 'POST', `/api/queue/${q1100.id}/shelve`, { token: TR, body: {} });
    check('同一对再搁 => 409 ALREADY_SHELVED', sh2.status === 409 && sh2.data.error.code === 'ALREADY_SHELVED', JSON.stringify(sh2.data.error));
    const unNothing = await req(P_URL, 'DELETE', `/api/queue/${q1200.id}/shelve`, { token: TR });
    check('解开没搁置的已裁对 => 409 NOT_SHELVED', unNothing.status === 409 && unNothing.data.error.code === 'NOT_SHELVED', unNothing.status);
    const shGhost = await req(P_URL, 'POST', '/api/queue/q-not-exist/shelve', { token: TR, body: {} });
    check('搁置不存在的对 => 404', shGhost.status === 404, shGhost.status);

    // ---- 13e. 搁着不能选胜负；旁边没搁置的对（已裁）状态不变 ----
    const adjWhileShelved = await req(P_URL, 'POST', `/api/queue/${q1100.id}/adjudicate`, { token: TR, body: { winner: 'a' } });
    check('搁置期间裁决 => 409 PAIR_SHELVED',
      adjWhileShelved.status === 409 && adjWhileShelved.data.error.code === 'PAIR_SHELVED', JSON.stringify(adjWhileShelved.data.error));
    s = (await req(P_URL, 'GET', '/api/state', { token: TR })).data;
    const q1100Now = s.queue.find((q) => q.id === q1100.id);
    const q1200Now = s.queue.find((q) => q.id === q1200.id);
    check('搁置中的对仍未裁决', !!q1100Now.shelveId && !q1100Now.verdict);
    check('视图统计搁置对数 = 1', s.counts.queueShelved === 1, s.counts.queueShelved);
    check('旁边已裁的对不受影响（verdict 仍在）', q1200Now.verdict === 'a' && !q1200Now.shelveId, q1200Now.verdict);

    // ---- 13f. 入库不受影响：所在根还能追加（后继沿两条并发边）----
    const succ = await req(P_URL, 'POST', '/api/events/batch', { token: TI, body: {
      items: [{ id: 'e-1100-z', root: 'order-1100', partition: 'p3', payload: { merge: true }, preds: ['e-1100-x', 'e-1100-y'] }]
    } });
    check('搁置期间所在根照常追加后继 => 201（入库不查搁置）', succ.status === 201, succ.status);
    check('后继没有产生新并发对（它站在两条并发边之后）',
      (succ.data.conflicts || []).length === 0, JSON.stringify(succ.data.conflicts));
    baseSeq += 1;

    // ---- 13g. 拉动不受影响：已经放到的事件照常放行 ----
    const pull = await req(P_URL, 'POST', '/api/consume/pull', { token: TC, body: { consumer: 'w-shelve', limit: 100 } });
    check('搁置期间消费照常拉动（不查搁置）', pull.status === 200, pull.status);
    check('搁置对两侧事件照常放到', ['e-1100-x', 'e-1100-y'].every((id) => pull.data.delivered.some((e) => e.id === id)));
    check('后继前驱未齐（两边并发未裁不影响拉动，这里前驱事件已放到）=> 后继也放行',
      pull.data.delivered.some((e) => e.id === 'e-1100-z'), JSON.stringify(pull.data.delivered.map((e) => e.id)));
    check('游标到末端（拉动完全不被搁置挡住）', pull.data.cursor === baseSeq, `${pull.data.cursor}/${baseSeq}`);

    // ---- 13h. 导出照旧不带这两条和顺着它们的后继 ----
    const exp = (await req(P_URL, 'GET', '/api/export', { token: TI })).data;
    check('导出列出搁置中对', exp.shelves.some((g) => g.queueId === q1100.id && g.shelveId === q1100Now.shelveId), JSON.stringify(exp.shelves));
    for (const id of ['e-1100-x', 'e-1100-y']) {
      check(`导出挡住搁置侧 ${id}（UNRESOLVED_CONFLICT）`,
        exp.blocked.some((b) => b.id === id && b.reason.startsWith('UNRESOLVED_CONFLICT')), JSON.stringify(exp.blocked.filter((b) => b.root === 'order-1100')));
    }
    check('导出挡住沿它们的后继 e-1100-z（BLOCKED_BY_PREDECESSOR 传递闭包）',
      exp.blocked.some((b) => b.id === 'e-1100-z' && b.reason.includes('BLOCKED_BY_PREDECESSOR')), JSON.stringify(exp.blocked.filter((b) => b.root === 'order-1100')));
    check('已裁的 order-1200 照常导出', ['e-1200-m', 'e-1200-n'].every((id) => exp.events.some((e) => e.id === id)));

    // ---- 13i. 解开之后才能再裁这一对 ----
    const un = await req(P_URL, 'DELETE', `/api/queue/${q1100.id}/shelve`, { token: TR });
    check('对账工解开搁置 => 200', un.status === 200, un.status);
    check('解开返回被移除的搁置痕迹', un.data.removed?.shelveId === q1100Now.shelveId, JSON.stringify(un.data.removed));
    s = (await req(P_URL, 'GET', '/api/state', { token: TR })).data;
    check('解开后搁置标记清空、统计归零', s.queue.find((q) => q.id === q1100.id).shelveId === null && s.counts.queueShelved === 0);
    const unAgain = await req(P_URL, 'DELETE', `/api/queue/${q1100.id}/shelve`, { token: TR });
    check('重复解开 => 409 NOT_SHELVED', unAgain.status === 409 && unAgain.data.error.code === 'NOT_SHELVED', unAgain.status);
    const adjAfter = await req(P_URL, 'POST', `/api/queue/${q1100.id}/adjudicate`, { token: TR, body: { winner: 'b' } });
    check('解开后可再裁这一对 => 200', adjAfter.status === 200, adjAfter.status);

    // ---- 13j. 已经裁过的对不要因为解开就改判 ----
    const q1200Final = (await req(P_URL, 'GET', '/api/state', { token: TR })).data.queue.find((q) => q.id === q1200.id);
    check('早先裁过的 order-1200 判词不变（winner=a）', q1200Final.verdict === 'a' && q1200Final.winnerEvent === 'e-1200-m', JSON.stringify(q1200Final));
    const reAdj = await req(P_URL, 'POST', `/api/queue/${q1100.id}/adjudicate`, { token: TR, body: { winner: 'a' } });
    check('裁完的对仍不能改判 => 409 ALREADY_DECIDED', reAdj.status === 409 && reAdj.data.error.code === 'ALREADY_DECIDED', reAdj.status);
    const exp2 = (await req(P_URL, 'GET', '/api/export', { token: TI })).data;
    check('裁完后搁置对及其后继全部可导出',
      ['e-1100-x', 'e-1100-y', 'e-1100-z'].every((id) => exp2.events.some((e) => e.id === id)) && exp2.shelves.length === 0,
      JSON.stringify(exp2.blocked));

    // ---- 13k. 搁置状态随同步镜像到副本；副本只读，搁置接口拒 ----
    // 再造一对搁置，用于观察副本镜像
    await req(P_URL, 'POST', '/api/events/batch', { token: TI, body: {
      items: [
        { id: 'e-1300-a2', root: 'order-1300', partition: 'p1', payload: {}, preds: [] },
        { id: 'e-1300-b2', root: 'order-1300', partition: 'p2', payload: {}, preds: [] }
      ]
    } });
    baseSeq += 2;
    s = (await req(P_URL, 'GET', '/api/state', { token: TR })).data;
    const q1300 = s.queue.find((q) => q.root === 'order-1300');
    await req(P_URL, 'POST', `/api/queue/${q1300.id}/shelve`, { token: TR, body: {} });
    await req(R_URL, 'POST', '/api/replica/sync-now', { token: TI });
    await sleep(200);
    // 副本拉动照常（搁置不挡拉动）
    const rp = await req(R_URL, 'POST', '/api/consume/pull', { token: TC, body: { consumer: 'w-shelve-rep', limit: 200 } });
    check('副本搁置期间拉动照常（搁置镜像不挡放行）',
      rp.status === 200 && rp.data.delivered.some((e) => e.id === 'e-1300-a2'), rp.status);
    // 副本上搁置/裁决/追加都拒
    const rsh1 = await req(R_URL, 'POST', `/api/queue/${q1300.id}/shelve`, { token: TR, body: {} });
    check('对账工在副本搁置 => 405 REPLICA_READ_ONLY', rsh1.status === 405, rsh1.status);
    const rsh2 = await req(R_URL, 'DELETE', `/api/queue/${q1300.id}/shelve`, { token: TC });
    check('消费工在副本解开搁置 => 403', rsh2.status === 403, rsh2.status);
    // 收尾：解开，别影响后续滞后/超时演练
    await req(P_URL, 'DELETE', `/api/queue/${q1300.id}/shelve`, { token: TR });
    await req(P_URL, 'POST', `/api/queue/${q1300.id}/adjudicate`, { token: TR, body: { winner: 'a' } });
  }

  console.log('\n== 14. 副本：正常同步后消费完整可见 ==');
  {
    await req(R_URL, 'POST', '/api/replica/sync-mode', { token: TI, body: { mode: 'normal' } });
    const sync = await req(R_URL, 'POST', '/api/replica/sync-now', { token: TI });
    check('手动同步成功', sync.status === 200, sync.status);
    const st = (await req(R_URL, 'GET', '/api/replica/status', { token: TC })).data;
    check(`副本镜像 ${baseSeq} 条`, st.mirrored === baseSeq, st.mirrored);
    const pull = await req(R_URL, 'POST', '/api/consume/pull', { token: TC, body: { consumer: 'w-rep', limit: 100 } });
    check('副本消费全部放完', pull.status === 200 && pull.data.delivered.length === baseSeq, `${pull.status}/${pull.data.delivered?.length}`);
  }

  console.log('\n== 15. 跨分区滞后：p2 看不见 => p3 后继对副本消费者不可见 ==');
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

  console.log('\n== 16. 副本同步超时：放行记失败，写哪侧分区没追上 ==');
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

  console.log('\n== 17. 副本只读 + 跨角色在副本侧也被拒 ==');
  {
    const r1 = await req(R_URL, 'POST', '/api/events/batch', { token: TI, body: { items: [] } });
    check('落库工在副本追加 => 405 REPLICA_READ_ONLY', r1.status === 405, r1.status);
    const r2 = await req(R_URL, 'POST', '/api/replica/sync-mode', { token: TC, body: { mode: 'paused' } });
    check('消费工改同步模式 => 403', r2.status === 403, r2.status);
    const r3 = await req(R_URL, 'POST', '/api/consume/pull', { token: TR, body: { consumer: 'x' } });
    check('对账工在副本消费 => 403', r3.status === 403, r3.status);
    const r4 = await req(R_URL, 'POST', '/api/seals', { token: TI, body: { root: 'order-100' } });
    check('落库工在副本贴封条 => 405 REPLICA_READ_ONLY', r4.status === 405, r4.status);
    const r5 = await req(R_URL, 'POST', '/api/seals', { token: TC, body: { root: 'order-100' } });
    check('消费工在副本贴封条 => 403', r5.status === 403, r5.status);
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
