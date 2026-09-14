// 向量时钟与因果比较（所有分区共享同一分量命名空间）
// 时钟用对象表示: { "p1": 2, "p2": 1 }，缺省分量视为 0

export function mergeClocks(...clocks) {
  const out = {};
  for (const c of clocks) {
    if (!c) continue;
    for (const [k, v] of Object.entries(c)) {
      if (v > (out[k] ?? 0)) out[k] = v;
    }
  }
  return out;
}

// 追加规则：先并上前驱各分量的最大值，本分区分量再加一
// （在事务内由 appendBatch 调用；等价于"本分区分量加一，再并前驱最大值"，
//   先 merge 再 +1 才能保证本事件严格排在本分区上一笔之后）
export function tickClock(prevMax, partition) {
  const c = { ...mergeClocks(prevMax) };
  c[partition] = (c[partition] ?? 0) + 1;
  return c;
}

// 比较两个时钟:
//   -1 : a 严格先于 b（a -> b）
//    1 : a 严格后于 b（b -> a）
//    0 : 相等
//   null: 并发（互不可比）
export function compareClocks(a, b) {
  const keys = new Set([...Object.keys(a ?? {}), ...Object.keys(b ?? {})]);
  let aLess = false;
  let bLess = false;
  for (const k of keys) {
    const av = a?.[k] ?? 0;
    const bv = b?.[k] ?? 0;
    if (av < bv) aLess = true;
    else if (av > bv) bLess = true;
  }
  if (aLess && bLess) return null;
  if (aLess) return -1;
  if (bLess) return 1;
  return 0;
}

// a 的全部分量是否都被 b 支配（a -> b 或 a == b）。
// 后继对其前驱必须满足: compareClocks(pred.clock, succ.clock) <= 0
export function dominatedBy(a, b) {
  const cmp = compareClocks(a, b);
  return cmp === -1 || cmp === 0;
}

export function clockEntries(c) {
  return Object.entries(c ?? {})
    .sort(([x], [y]) => (x < y ? -1 : x > y ? 1 : 0))
    .map(([k, v]) => `${k}:${v}`)
    .join(' ');
}
