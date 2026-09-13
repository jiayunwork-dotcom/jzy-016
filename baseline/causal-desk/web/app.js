/* 因果序落库台前端 —— 纯原生 JS */
const $ = (sel, root = document) => root.querySelector(sel);
const $$ = (sel, root = document) => [...root.querySelectorAll(sel)];

const state = {
  token: localStorage.getItem('cd_token') || null,
  user: null,
  primary: location.origin,
  replica: `${location.protocol}//${location.hostname}:4001`,
  view: null,
  batch: []
};

// ---------- API ----------
async function api(base, method, path, body) {
  const opts = {
    method,
    headers: { Authorization: `Bearer ${state.token}`, 'Content-Type': 'application/json' }
  };
  if (body !== undefined) opts.body = JSON.stringify(body);
  const res = await fetch(base + path, opts);
  let data = null;
  try { data = await res.json(); } catch { /* 无 body */ }
  if (!res.ok) {
    const err = new Error(data?.error?.message || `${res.status} ${res.statusText}`);
    err.status = res.status;
    err.code = data?.error?.code || 'HTTP_ERROR';
    err.data = data;
    throw err;
  }
  return data;
}
const P = (m, p, b) => api(state.primary, m, p, b);
const R = (m, p, b) => api(state.replica, m, p, b);

function toast(msg, kind = 'ok', ms = 4000) {
  const host = $('#toastHost');
  const el = document.createElement('div');
  el.className = `toast ${kind}`;
  el.innerHTML = msg;
  host.appendChild(el);
  setTimeout(() => el.remove(), ms);
}

function clockStr(c) {
  return Object.entries(c || {}).sort(([a], [b]) => a.localeCompare(b)).map(([k, v]) => `${k}:${v}`).join(' ');
}
function esc(s) {
  return String(s ?? '').replace(/[&<>"]/g, (ch) => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;' }[ch]));
}
function fmtTime(ts) {
  return ts ? new Date(ts).toLocaleTimeString('zh-CN', { hour12: false }) : '-';
}

// ---------- 登录/登出 ----------
function showLogin() {
  $('#login').hidden = false;
  $('#app').hidden = true;
}
function showApp() {
  $('#login').hidden = true;
  $('#app').hidden = false;
  const role = state.user.role;
  $('#whoami').textContent = `${state.user.name}（${roleLabel(role)}）`;

  // 三个 tab 始终可见，但越权 tab 内是 403 面板（"谁也打不开流" 的页面侧体现）
  $$('#tabs button').forEach((b) => b.classList.toggle('active', b.dataset.tab === roleTab(role)));
  applyRolePanels(role);
  switchTab(roleTab(role));
  refreshAll();
}
function roleTab(role) {
  return role === 'ingest' ? 'ingest' : role === 'consume' ? 'consume' : 'reconcile';
}
function roleLabel(role) {
  return { ingest: '落库工', consume: '消费工', reconcile: '对账工' }[role] || role;
}
function applyRolePanels(role) {
  $('#ingestPanel').hidden = role !== 'ingest';
  $('#ingestDenied').hidden = role === 'ingest';
  $('#consumePanel').hidden = role !== 'consume';
  $('#consumeDenied').hidden = role === 'consume';
  $('#reconcilePanel').hidden = role !== 'reconcile';
  $('#reconcileDenied').hidden = role === 'reconcile';
}
function switchTab(tab) {
  $$('.tab').forEach((s) => (s.hidden = s.id !== `tab-${tab}`));
  $$('#tabs button').forEach((b) => b.classList.toggle('active', b.dataset.tab === tab));
  refreshAll();
}

$('#loginForm').addEventListener('submit', async (e) => {
  e.preventDefault();
  $('#loginError').textContent = '';
  try {
    const r = await P('POST', '/api/auth/login', {
      username: $('#username').value.trim(),
      password: $('#password').value
    });
    state.token = r.token;
    state.user = r.user;
    localStorage.setItem('cd_token', r.token);
    showApp();
  } catch (err) {
    $('#loginError').textContent = err.message;
  }
});
$$('.demo-users button').forEach((b) =>
  b.addEventListener('click', () => {
    $('#username').value = b.dataset.u;
    $('#password').value = b.dataset.p;
  })
);
$('#logout').addEventListener('click', () => {
  localStorage.removeItem('cd_token');
  state.token = null;
  state.user = null;
  showLogin();
});
$$('#tabs button').forEach((b) => b.addEventListener('click', () => switchTab(b.dataset.tab)));

// ---------- 通用刷新 ----------
async function refreshAll() {
  if (!state.user) return;
  try {
    state.view = await P('GET', '/api/state');
    renderLibrary();
    renderIngest();
    renderReconcile();
  } catch (e) {
    if (e.status === 401) { localStorage.removeItem('cd_token'); state.token = null; showLogin(); }
  }
  if (!$('#tab-consume').hidden) await refreshReplicaStatus();
}

function renderLibrary() {
  const v = state.view;
  if (!v) return;
  $('#libCounts').textContent =
    `共 ${v.events.length} 条 · 未决并发 ${v.counts.queueOpen} 对 · 已裁 ${v.counts.queueDecided} 对`;

  const rows = v.events.map((e) => `
    <tr>
      <td class="mono">${e.seq}</td>
      <td>${esc(e.root)}</td>
      <td class="mono">${esc(e.partition)}</td>
      <td><b>${esc(e.id)}</b><div class="muted">${esc(JSON.stringify(e.payload))}</div></td>
      <td>${e.preds.length ? e.preds.map((p) => `<span class="chip">${esc(p)}</span>`).join('') : '<span class="muted">—</span>'}</td>
      <td class="mono">${clockStr(e.clock)}</td>
      <td class="mono">${esc(e.batchId)}</td>
    </tr>`).join('');
  $('#eventTable tbody').innerHTML = rows;
  $('#recEventTable tbody').innerHTML = rows;

  // 前驱多选只给"同根"候选用（跨根会被后台拒），按当前 root 输入过滤也交给后台
  const opts = v.events
    .slice()
    .sort((a, b) => a.seq - b.seq)
    .map((e) => `<option value="${esc(e.id)}">${esc(e.seq)} · ${esc(e.root)} · ${esc(e.id)} [${clockStr(e.clock)}]</option>`)
    .join('');
  $('#evPreds').innerHTML = opts;
}

// ---------- 落库台 ----------
function renderIngest() {
  const v = state.view;
  if (!v) return;
  const ch = $('#channelState');
  ch.textContent = v.channelUp ? '● 通道正常：可追加' : '● 通道断开：拒绝一切新追加';
  ch.className = `channel ${v.channelUp ? 'up' : 'down'}`;

  // 批次队列
  $('#batchList').innerHTML = state.batch
    .map(
      (it, i) => `<li>
        <b>${esc(it.root)}</b> / <span class="mono">${esc(it.partition)}</span>
        <div class="muted">${esc(it._id || it.id)} → 前驱: ${
        it.preds.length ? it.preds.map((p) => `<span class="chip">${esc(p)}</span>`).join('') : '无'
      }</div>
        <button data-rm="${i}">移出本批</button>
      </li>`
    )
    .join('');
  $$('#batchList button[data-rm]').forEach((b) =>
    b.addEventListener('click', () => {
      state.batch.splice(Number(b.dataset.rm), 1);
      renderIngest();
    })
  );

  // 快照
  $('#snapList').innerHTML = Object.values(v.snapshots)
    .sort((a, b) => b.createdAt - a.createdAt)
    .map(
      (s) => `<div class="snap-item ${s.state}">
        <span><b>${esc(s.root)}</b> · ${esc(s.id)} · 截至 seq ${s.atSeq}</span>
        <span>${s.state === 'running' ? `进行中…（${fmtTime(s.createdAt)} 起）` : `已完成 ${fmtTime(s.finishedAt)}`}</span>
      </div>`
    )
    .join('');

  // 退回记录
  $('#abortList').innerHTML = v.abortedBatches.length
    ? v.abortedBatches
        .map(
          (x) => `<div class="abort">
          <b class="mono">${esc(x.id)}</b> · ${fmtTime(x.ts)} · ${x.items.length} 条整批退回
          <div class="muted">${x.reasons.map((r) => `${esc(r.code)}: ${esc(r.message)}${r.extra?.missing ? `（缺 ${esc(r.extra.missing)}）` : ''}`).join('<br>')}</div>
        </div>`
        )
        .join('')
    : '<div class="muted">暂无退回记录</div>';

  renderQueueCounts();
}

function renderQueueCounts() {
  const v = state.view;
  $('#queueCounts').textContent =
    v ? `未决 ${v.queue.filter((q) => !q.verdict).length} 对 · 已裁 ${v.queue.filter((q) => q.verdict).length} 对` : '';
}

$('#addToBatch').addEventListener('click', () => {
  let payload;
  try {
    payload = JSON.parse($('#evPayload').value || '{}');
  } catch {
    return toast('载荷不是合法 JSON', 'err');
  }
  const root = $('#evRoot').value.trim();
  const partition = $('#evPartition').value.trim();
  if (!root || !partition) return toast('聚合根和分区必填', 'err');
  const preds = $$('#evPreds option:checked').map((o) => o.value);
  const id = 'e' + crypto.randomUUID().replace(/-/g, '').slice(0, 14);
  state.batch.push({ _id: id, root, partition, payload, preds });
  $('#evRoot').value = root; // 同批常同根，保留 root
  $('#evPayload').value = '{}';
  $$('#evPreds option').forEach((o) => (o.selected = false));
  renderIngest();
  toast(`已加入本批：${id}（尚未提交）`);
});
$('#clearBatch').addEventListener('click', () => {
  state.batch = [];
  renderIngest();
});
$('#submitBatch').addEventListener('click', async () => {
  if (state.batch.length === 0) return toast('本批是空的，先加入事件', 'err');
  const items = state.batch.map(({ root, partition, payload, preds }) => ({ root, partition, payload, preds }));
  const box = $('#batchResult');
  try {
    const r = await P('POST', '/api/events/batch', { items });
    const lines = [
      `<div class="ok-line">✔ 批次 ${r.batchId} 整 ${r.committed.length} 条全部入库（整批原子提交）</div>`,
      ...r.committed.map((c) => `<div>seq ${c.seq} · ${esc(c.root)}/${esc(c.partition)} · <b>${esc(c.id)}</b> · <span class="mono">${clockStr(c.clock)}</span></div>`)
    ];
    if (r.conflicts.length) {
      lines.push(`<div class="wait-line">⚖ 检测到 ${r.conflicts.length} 对同根并发，已进并发队列，不能自动并成一条：</div>`);
      lines.push(...r.conflicts.map((q) => `<div class="wait-line">　${esc(q.id)}: ${esc(q.a)} ⚡ ${esc(q.b)}（${q.reason === 'CLOCK_EQUAL' ? '时钟相等' : '时钟互不可比'}）</div>`));
    }
    box.innerHTML = lines.join('');
    state.batch = [];
    await refreshAll();
  } catch (e) {
    box.innerHTML = `<div class="fail-line">✘ 整批退回（一条都没落库）：${esc(e.code)} — ${esc(e.message)}</div>
      ${e.data?.error?.abortedBatchId ? `<div class="muted">退回审计：${esc(e.data.error.abortedBatchId)}</div>` : ''}`;
    await refreshAll();
  }
});

$('#channelUp').addEventListener('click', async () => {
  try { await P('POST', '/api/admin/channel', { up: true }); toast('通道已恢复'); await refreshAll(); }
  catch (e) { toast(e.message, 'err'); }
});
$('#channelDown').addEventListener('click', async () => {
  try { await P('POST', '/api/admin/channel', { up: false }); toast('通道已断开：新追加会被拒', 'err'); await refreshAll(); }
  catch (e) { toast(e.message, 'err'); }
});

$('#snapStart').addEventListener('click', async () => {
  const root = $('#snapRoot').value.trim();
  if (!root) return toast('填聚合根', 'err');
  try {
    const r = await P('POST', '/api/snapshots', { root });
    toast(`快照 ${r.snapshot.id} 进行中；${r.snapshot.root} 此时再来一份会被拒`);
    await refreshAll();
    setTimeout(refreshAll, 3200);
  } catch (e) { toast(e.message, 'err'); }
});

$('#runExport').addEventListener('click', async () => {
  try {
    const r = await P('GET', '/api/export');
    const blocked = r.blocked
      .map((b) => `<div class="fail-line">✘ seq ${b.seq} ${esc(b.id)}：${esc(b.reason)}</div>`)
      .join('');
    $('#exportBox').innerHTML = `
      <div>导出 <b class="ok-line">${r.counts.exported}</b> / ${r.counts.total}；挡住 ${r.counts.blocked} 条</div>
      <div class="muted">未决并发对两侧、以及它们的后继都不带出去。</div>
      ${blocked ? `<h3>未导出</h3>${blocked}` : ''}
      <pre>${esc(JSON.stringify(r.events.map((e) => ({ seq: e.seq, id: e.id, root: e.root, clock: e.clock })), null, 2))}</pre>`;
  } catch (e) { toast(e.message, 'err'); }
});

// ---------- 对账台 ----------
function renderReconcile() {
  const v = state.view;
  if (!v) return;
  const open = v.queue.filter((q) => !q.verdict);
  const decided = v.queue.filter((q) => q.verdict);
  const item = (q) => {
    const a = v.events.find((e) => e.id === q.a);
    const b = v.events.find((e) => e.id === q.b);
    return `<div class="queue-item ${q.verdict ? 'decided' : ''}" data-q="${q.id}">
      <div><span class="tag ${q.verdict ? 'decided' : 'open'}">${q.verdict ? '已裁决' : '未决'}</span>
        <b class="mono">${esc(q.id)}</b> · 聚合根 <b>${esc(q.root)}</b>
        · ${q.reason === 'CLOCK_EQUAL' ? '两笔时钟相等' : '两笔时钟互不可比'}</div>
      <div class="pair">
        <div class="side"><b>A · ${esc(q.a)}</b>
          <div class="muted">seq ${a?.seq ?? '?'} · ${esc(a?.partition || '')} · ${esc(JSON.stringify(a?.payload || null))}</div>
          <div class="clock">${clockStr(q.clocks?.[q.a] || a?.clock)}</div>
          ${q.verdict ? (q.winnerEvent === q.a ? '<div class="ok-line">胜方</div>' : '<div class="muted">败方（仍保留为独立事件，不并条）</div>') : ''}
        </div>
        <div class="side"><b>B · ${esc(q.b)}</b>
          <div class="muted">seq ${b?.seq ?? '?'} · ${esc(b?.partition || '')} · ${esc(JSON.stringify(b?.payload || null))}</div>
          <div class="clock">${clockStr(q.clocks?.[q.b] || b?.clock)}</div>
          ${q.verdict ? (q.winnerEvent === q.b ? '<div class="ok-line">胜方</div>' : '<div class="muted">败方（仍保留为独立事件，不并条）</div>') : ''}
        </div>
      </div>
      ${q.verdict
        ? `<div class="muted">${esc(q.decidedByName)} 于 ${fmtTime(q.decidedAt)} 裁决：${esc(q.winnerEvent)}</div>`
        : `<div class="actions">
            <button class="ok" data-win="a">判 A 为胜（${esc(q.a)}）</button>
            <button class="warn" data-win="b">判 B 为胜（${esc(q.b)}）</button>
          </div>`}
    </div>`;
  };
  $('#queueList').innerHTML =
    (open.length ? '<h3>待裁决</h3>' + open.map(item).join('') : '<div class="muted">没有未决并发</div>') +
    (decided.length ? '<h3>已裁决（不可改判）</h3>' + decided.map(item).join('') : '');

  $$('#queueList button[data-win]').forEach((b) =>
    b.addEventListener('click', async () => {
      const qid = b.closest('.queue-item').dataset.q;
      try {
        await P('POST', `/api/queue/${qid}/adjudicate`, { winner: b.dataset.win });
        toast(`并发对 ${qid} 已裁决`);
        await refreshAll();
        setTimeout(refreshReplicaStatus, 500);
      } catch (e) { toast(e.message, 'err'); }
    })
  );
}

// ---------- 消费台 ----------
async function refreshReplicaStatus() {
  try {
    const s = await R('GET', '/api/replica/status');
    state.replicaStatus = s;
    const modeText = { normal: '正常同步', lag: `分区滞后 [${s.lagPartitions.join(',')}]`, timeout: '同步超时', paused: '已暂停' }[s.mode];
    $('#repStatus').innerHTML = `
      <div class="kv">模式：<b>${modeText}</b> · 已镜像 ${s.mirrored} 条 · 连续应用到 seq ${s.catchUpSeq}${s.lastSync?.stuckAt ? ` · <span class="fail-line">卡在分区 ${esc(s.lastSync.stuckAt.partition)} seq ${s.lastSync.stuckAt.seq}（${esc(s.lastSync.stuckAt.id)}）</span>` : ''}</div>
      <div class="kv">分区水位：${Object.entries(s.partitionWatermarks).map(([k, v]) => `${k}=${v}`).join(' · ') || '（空）'}</div>
      <div class="kv">上次同步：${s.lastSync ? `${fmtTime(s.lastSync.at)}，新增 ${s.lastSync.applied}${s.lastSync.stuckAt ? '，该侧分区没追上' : ''}` : '尚未同步'}</div>`;
    $('#repDetail').innerHTML = renderReplicaDetail(s);
  } catch (e) {
    $('#repStatus').innerHTML = `<div class="fail-line">副本状态不可用：${esc(e.message)}</div>`;
  }
}

function renderReplicaDetail(s) {
  const cursors = Object.entries(s.cursors)
    .map(([name, c]) => `<div class="cursor-box">
      <b>${esc(name)}</b>：游标 seq ${c.lastSeq}
      ${c.blockedAt ? ` · <span class="fail-line">停在缺口前 seq ${c.blockedAt}</span>` : ' · 已追到本副本可见末端'}
      ${c.gap ? `<div class="muted">没追上的一侧：分区 ${esc(c.gap.laggingPartition)} 水位 ${c.gap.replicaWatermark}，需要 seq ${c.gap.requiredSeq ?? '?'}（前驱 ${esc(c.gap.predecessor)}）</div>` : ''}
    </div>`)
    .join('') || '<span class="muted">尚无消费者</span>';
  const fails = s.recentFailures
    .map((f) => `<div class="fail-item"><b>${esc(f.phase)}</b> · ${fmtTime(f.ts)} · ${esc(f.code)}
      ${f.laggingPartition ? `<div>没追上的一侧分区：<b>${esc(f.laggingPartition)}</b>（水位 ${f.replicaWatermark}，需要 ${f.requiredSeq ?? '?'}）</div>` : ''}
      <div class="muted">${esc(f.message || '')}</div></div>`)
    .join('');
  return `<h3>副本游标</h3>${cursors}<h3>放行/同步失败记录（只记失败，不会出现"前驱未齐却已完成"）</h3>${fails || '<span class="muted">无失败</span>'}`;
}

$$('.rep-mode').forEach((b) =>
  b.addEventListener('click', async () => {
    const lag = b.dataset.mode === 'lag' ? ['p2'] : null;
    try {
      await R('POST', '/api/replica/sync-mode', { mode: b.dataset.mode, ...(lag ? { lagPartitions: lag } : {}) });
      toast(`副本模式 → ${b.dataset.mode}`);
      await refreshReplicaStatus();
    } catch (e) {
      toast(`被拒绝：${e.status} ${e.message}`, 'err');
    }
  })
);
$('#repSyncNow').addEventListener('click', async () => {
  try {
    const r = await R('POST', '/api/replica/sync-now');
    toast(r.stuckAt ? `同步卡在分区 ${r.stuckAt.partition}（seq ${r.stuckAt.seq} 没追上）` : `同步完成：+${r.applied ?? 0}，连续到 seq ${r.catchUpSeq}`);
  } catch (e) { toast(`同步失败：${e.message}`, 'err'); }
  await refreshReplicaStatus();
});

$('#pPull').addEventListener('click', () => doPull('primary'));
$('#rPull').addEventListener('click', () => doPull('replica'));

async function doPull(side) {
  const consumer = $('#pConsumer').value.trim() || 'worker-A';
  const box = side === 'primary' ? $('#pPullResult') : $('#rPullResult');
  try {
    const r = side === 'primary'
      ? await P('POST', '/api/consume/pull', { consumer, limit: 20 })
      : await R('POST', '/api/consume/pull', { consumer, limit: 20 });
    const delivered = r.delivered
      .map((e) => `<div class="delivered-item">✔ seq ${e.seq} <b>${esc(e.id)}</b>（${esc(e.root)}/${esc(e.partition)}）<span class="clock mono">${clockStr(e.clock)}</span></div>`)
      .join('');
    const block = r.blockedAt
      ? `<div class="wait-line">⛔ 游标停在缺口前 seq ${r.blockedAt}，不跳过、不标已送达</div>
         ${(r.blockedReason || []).map((x) => `<div class="fail-line">${esc(x)}</div>`).join('')}`
      : (r.delivered.length === 0 ? '<div class="wait-line">没有更多已齐前驱的事件，游标不动</div>' : '');
    box.innerHTML = `<div><b>${side === 'primary' ? '主库' : '副本'}</b> 消费 ${r.delivered.length} 条，游标 → seq ${r.cursor}</div>${delivered}${block}`;
    await refreshAll();
    await refreshReplicaStatus();
  } catch (e) {
    box.innerHTML = `<div class="fail-line">✘ ${e.status ?? ''} ${esc(e.code)}：${esc(e.message)}</div>
      ${e.data?.error?.laggingPartition ? `<div class="fail-line">没追上的一侧：分区 ${esc(e.data.error.laggingPartition)}，水位 ${e.data.error.replicaWatermark}，需要 seq ${e.data.error.requiredSeq ?? '?'}</div>` : ''}
      <div class="muted">本次放行记失败；任何后继都没有被标成已送达</div>`;
    await refreshReplicaStatus();
  }
}

// 主侧游标面板轮询
async function refreshPrimaryCursors() {
  if ($('#tab-consume').hidden || !state.user || state.user.role !== 'consume') return;
  try {
    const r = await P('GET', '/api/consume/cursors');
    $('#pCursors').innerHTML = Object.entries(r.cursors)
      .map(([name, c]) => `<div class="cursor-box"><b>${esc(name)}</b>：seq ${c.lastSeq}
        ${c.blockedAt ? ` · <span class="fail-line">缺口 seq ${c.blockedAt}</span>` : ''}</div>`)
      .join('') || '<span class="muted">尚无拉动</span>';
  } catch { /* 忽略 */ }
}

// ---------- 权限自检（打真实接口，展示后台返回码） ----------
const PROBES = [
  {
    role: 'ingest',
    el: '#ingestProbes',
    calls: [
      { title: '落库工去裁决并发（POST /api/queue/q1/adjudicate）', run: () => P('POST', '/api/queue/q1/adjudicate', { winner: 'a' }), expect: 403 },
      { title: '落库工去拉动消费游标（POST /api/consume/pull）', run: () => P('POST', '/api/consume/pull', { consumer: 'probe' }), expect: 403 },
      { title: '直接改写消费游标（POST /api/consume/cursors）', run: () => P('POST', '/api/consume/cursors', { lastSeq: 999 }), expect: 405 },
      { title: '修改别人的前驱声明（PATCH /api/events/e-order100-created）', run: () => P('PATCH', '/api/events/e-order100-created', { preds: [] }), expect: 405 },
      { title: '去副本改同步模式（消费/对账该被拒，落库工应 200）', run: () => R('POST', '/api/replica/sync-mode', { mode: 'normal' }), expect: 200 }
    ]
  },
  {
    role: 'consume',
    el: '#consumeProbes',
    calls: [
      { title: '消费工去追加事件（POST /api/events/batch）', run: () => P('POST', '/api/events/batch', { items: [{ root: 'x', partition: 'p1', payload: {}, preds: [] }] }), expect: 403 },
      { title: '消费工去裁决并发（POST /api/queue/q1/adjudicate）', run: () => P('POST', '/api/queue/q1/adjudicate', { winner: 'a' }), expect: 403 },
      { title: '直接改写消费游标（POST /api/consume/cursors）', run: () => P('POST', '/api/consume/cursors', { lastSeq: 999 }), expect: 405 },
      { title: '副本上追加业务事件（POST :4001/api/events/batch）', run: () => R('POST', '/api/events/batch', { items: [] }), expect: 405 },
      { title: '改副本同步模式（仅落库工）', run: () => R('POST', '/api/replica/sync-mode', { mode: 'lag' }), expect: 403 }
    ]
  },
  {
    role: 'reconcile',
    el: '#reconcileProbes',
    calls: [
      { title: '对账工追加新业务事件（POST /api/events/batch）', run: () => P('POST', '/api/events/batch', { items: [{ root: 'x', partition: 'p1', payload: {}, preds: [] }] }), expect: 403 },
      { title: '对账工去拉动消费游标（POST /api/consume/pull）', run: () => P('POST', '/api/consume/pull', { consumer: 'probe' }), expect: 403 },
      { title: '直接改写消费游标（PUT /api/consume/cursors/x）', run: () => P('PUT', '/api/consume/cursors/probe', { lastSeq: 1 }), expect: 405 },
      { title: '修改别人的前驱声明（PATCH /api/events/e-order100-created）', run: () => P('PATCH', '/api/events/e-order100-created', { preds: [] }), expect: 405 },
      { title: '已裁决并发对改判（再次 adjudicate）', run: async () => {
          const q = state.view?.queue.find((x) => x.verdict);
          if (!q) return { status: 0, message: '当前没有已裁决队列可试' };
          return P('POST', `/api/queue/${q.id}/adjudicate`, { winner: 'a' });
        }, expect: 409 }
    ]
  }
];

async function runProbes() {
  if (!state.user) return;
  const cfg = PROBES.find((p) => p.role === state.user.role);
  if (!cfg || !$(cfg.el)) return;
  $(cfg.el).innerHTML = (
    await Promise.all(
      cfg.calls.map(async (c) => {
        let status, code, ok;
        try {
          const r = await c.run();
          status = r.status ?? 200;
          ok = status === c.expect;
          code = ok ? '符合预期' : `预期 ${c.expect}，实际 ${status}`;
        } catch (e) {
          status = e.status ?? 0;
          ok = status === c.expect;
          code = e.code;
        }
        // 无已裁决队列的占位
        if (status === 0) {
          return `<div class="probe"><span>${c.title}</span><span class="muted">跳过：${'尚无已裁决并发对'}</span></div>`;
        }
        return `<div class="probe">
          <span>${c.title}<div class="code">${esc(code)}</div></span>
          <span class="verdict ${ok ? 'ok' : 'deny'}">${status} ${ok ? '✓ 拦住' : '✗ 异常'}</span>
        </div>`;
      })
    )
  ).join('');
}

// ---------- 轮询 ----------
setInterval(() => {
  if (!state.user) return;
  refreshAll();
  refreshPrimaryCursors();
  runProbes();
}, 3000);

// ---------- 启动：有 token 先验活 ----------
(async function boot() {
  if (state.token) {
    try {
      const r = await fetch(state.primary + '/api/me', { headers: { Authorization: `Bearer ${state.token}` } });
      if (!r.ok) throw new Error();
      const data = await r.json();
      state.user = { id: data.user.id, name: data.user.name, role: data.user.role };
      showApp();
      runProbes();
      return;
    } catch {
      localStorage.removeItem('cd_token');
      state.token = null;
    }
  }
  showLogin();
})();
