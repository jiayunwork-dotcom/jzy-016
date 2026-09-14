# 因果序落库台（Causal Desk）

同一聚合根上的事件按**向量时钟因果序**落库：声明的前驱没在库、或同根两笔时钟对不上，就不能追加；消费侧后继到了、前驱没齐，就不放行。落库工还能给在库聚合根贴**停写封条**：封的是根、不是通道——通道可以开着，被封的根整批提交照样被拦；在库旧事件照常消费、裁决、导出。三个工位（落库台 / 消费台 / 对账台）+ 主/副本，Docker Compose 一键启动。

## 一键启动

```bash
docker compose up --build
# 打开 http://localhost:4000
```

容器以非 root 用户 `node`(uid 1000) 运行业务进程；入口脚本会在 root 阶段把 `/data`
属主修正为 node 后再降权启动，因此首次创建的命名卷、旧版本遗留的卷、以及外部 bind mount
都能写入 `state.json` 临时文件。若你之前跑过失败版本，无需删卷，直接
`docker compose up --build` 即可；想彻底重来可用 `docker compose down -v`。

| 服务 | 地址 | 角色 |
| --- | --- | --- |
| `primary` 落库台主侧 | http://localhost:4000 | 整批追加、时钟、并发队列、裁决、快照、导出、主侧消费；Web 页面 |
| `replica` 消费台副本 | http://localhost:4001 | 只读镜像，可演练分区滞后 / 同步超时下的缺口拦截 |

数据落在 Docker 卷 `primary-data` / `replica-data`（JSON 原子落盘，临时文件 rename，无外部数据库）。

### 演示账号（没登录任何流都打不开）

| 账号 | 密码 | 工位 | 能干 | 不能干 |
| --- | --- | --- | --- | --- |
| `ingest` | `ing123` | 落库工 | 追加事件/声明前驱/整批提交、通道开关、贴/揭停写封条、快照、导出 | 裁决、拉游标 |
| `consume` | `con123` | 消费工 | 主侧/副本拉动消费、看游标 | 追加、裁决、贴/揭封条、改同步模式 |
| `reconcile` | `rec123` | 对账工 | 同根并发裁决、只读在库 | 追加业务事件、动游标、改前驱、贴/揭封条 |

页面按角色只开放本职工位（越权显示 403 面板），后台路由用 JWT + 角色中间件**再拒一次**；每个工位底部的「权限自检」面板会打真实越权请求并显示返回码（403/405）。

## 规则与实现位置

| 需求规则 | 实现 |
| --- | --- |
| 追加同时满足：声明前驱每条已在库（或本批更早）、本批对外依赖齐 | `src/primary/causal.js` `appendBatch()` 第 3 步，缺前驱返回 `PREDECESSOR_MISSING` |
| 同批要么全进要么整批退，不许半批落库 | 深副本上计算（`JsonStore.mutate`），任一步抛错即丢弃；只有全部校验/排序/时钟算完才统一编号落盘；退回写 `abortedBatches` 审计，事件一条不进 |
| 时钟按分区：并上前驱各分量最大值，本分区再加一 | `src/shared/clock.js` `tickClock(mergeClocks(...preds), partition)` |
| 批内前驱 DAG 校验、拓扑序提交（前驱 seq 必更小） | `appendBatch()` 第 4–7 步，有环返回 `CYCLE_IN_BATCH` |
| 停写封条封根不封通道：通道开着，被封的根整批提交也拦，写明哪个根 | `appendBatch()` 第 2 步：409 `ROOT_SEALED` 带 `sealedRoots`，整批退一条不进；页面提交前也先拦一次 |
| 同一根不能同时挂两份封条；还没在库的根不能贴 | `sealRoot()`：重复贴 409 `SEAL_EXISTS`，不在库 404 `ROOT_NOT_IN_STORE`；`seals` 以根为键，结构上就放不下第二份 |
| 只有落库工能贴/揭；消费工、对账工页面没入口，后台一样拒 | `POST /api/seals`、`DELETE /api/seals/:root` 挂 `requireRole('ingest')` + `assertCanSeal()` 兜底，越权 403 |
| 揭掉封条该根才能再交；揭之前被拦的那批不偷偷补进 | `unsealRoot()`；被拦批次从不落库、无自动重放，只能由落库工重新显式提交 |
| 通道断开时贴/揭都做不成，已贴封条继续拦 | `sealRoot()/unsealRoot()` 先查 `channelUp`，409 `CHANNEL_DOWN`；`appendBatch()` 的封条检查独立于通道检查 |
| 封条不藏在库旧事件：消费照常拉动、已有并发对照常裁决、导出照常带 | `pullPrimary()` / `adjudicate()` / `buildExport()` 完全不查封条 |
| 同根两笔时钟互不可比 → 进并发队列，不能自动并成一条 | 新事件与库内/批内同根事件两两 `compareClocks`，`null`/相等即建队列对；两条事件都独立保留 |
| 对账工只处理同根并发 | `POST /api/queue/:id/adjudicate`（仅 `reconcile`），裁决后不可改判（409），败方仍是独立事件 |
| 消费放行只看：这条的全部前驱已对该消费者可见；差一条就拒，游标停在缺口前 | 主侧 `pullPrimary()`、副本 `pullReplica()`：按 seq 逐条检查，遇到不可见前驱立即 `break`，不跳过、不标送达 |
| 游标不可直接改写 / 前驱声明不可改 | `POST/PUT /api/consume/cursors*` → 405，`PATCH/PUT /api/events/:id` → 405 |
| 同一根不能同时打两份快照 | `createSnapshot()`，进行中再来一把返回 `SNAPSHOT_IN_PROGRESS`（3 秒后自动完成，不同根互不影响） |
| 导出只含因果齐 + 并发已裁完 | `buildExport()`：未决对两侧挡住，并沿前驱做传递闭包（后继也不带出去），返回 `blocked` 明细 |
| 从副本读：跨分区前驱看不见 → 后继对副本消费者不可见，禁止标已送达 | `src/replica/`：同步严格连续应用，滞后分区第一条卡住同步流；`pullReplica` 报告 `gap.laggingPartition / replicaWatermark / requiredSeq`，游标停缺口前 |
| 落库通道断了：不能新追加，在库按最后有效时钟守序 | `POST /api/admin/channel {up:false}` 后追加返回 `CHANNEL_DOWN`；历史只读、导出/消费不受影响 |
| 副本同步超时的那一次：放行记失败并写哪侧分区没追上 | 副本 `timeout` 模式：pull 返回 409 `DELIVERY_BLOCKED`，`failures` 落账（分区/水位/需要 seq），没有任何后继被标送达；恢复后从断点续拉补齐 |

## 建议演练路径

1. **登录 `ingest`**：库里已有 6 条种子事件（order-100/200 各有一对未决并发）。
2. **整批追加**：填聚合根/分区/载荷，多选已在库事件作为前驱 → 加入本批（可多条）→ 整批提交；试着引用一个不存在的前驱，看整批退回与审计记录。
3. **断开通道**再追加 → 409；恢复。
4. **停写封条**：给 `order-100` 贴封条 → 本批含该根时「整批提交」页面直接拦住并写明根名（后台同样 409 `ROOT_SEALED`）；换个没封的根照常追加；同一根再贴一份 → 409；还没在库的根列表里根本没有贴封条入口。登录 `consume` 拉动，被封根的旧事件照常放行；回 `ingest` 揭掉封条，该根才能再交——之前被拦的那批不会偷偷补进。通道断开时贴/揭都会 409，已贴的封条继续拦。
5. **退出，登录 `reconcile`**：打开对账台，把两对并发逐一裁决（A/B 选边）；看落库台「导出」从挡 4 条变成全量可导出。
6. **登录 `consume`**：主侧拉动，游标逐次前进到底。
7. 消费台副本区（故障注入按钮只有 `ingest` 能按，消费工按会收到 403——可回 `ingest` 操作）：
   - 先用 `ingest` 给某个根在 **p2** 追加一笔、再在 **p3** 追加一笔（后者声明前者为前驱）；
   - 副本切「**p2 分区滞后**」→ `consume` 拉动：游标停在 p2 缺口前，页面写明"分区 p2 水位 X，需要先追上 seq Y"，p3 后继不会被标已送达；
   - 切「**同步超时**」再拉 → 409 + 失败记录；
   - 切回「正常同步」→ 自动从断点补齐，再拉一次，后继放行、游标到底。
8. 任意工位底部「权限自检」：所有越权动作都应显示 `403 ✓` / `405 ✓`。

## API 速览

```
POST /api/auth/login                 公开
GET  /api/me                         已登录
GET  /api/state                      已登录（只读视图）
POST /api/events/batch               ingest      整批追加（原子）
PATCH/PUT /api/events/:id            任意登录     405（前驱不可改）
POST /api/admin/channel              ingest      通道开关
GET  /api/seals                      已登录      当前封条列表
POST /api/seals                      ingest      贴停写封条（根须在库；同根仅一份）
DELETE /api/seals/:root              ingest      揭封条（通道断开时贴/揭均 409）
GET  /api/export                     ingest      因果齐+已裁决
POST /api/snapshots[/...]            ingest      快照（同根互斥）
GET  /api/queue                      已登录
POST /api/queue/:id/adjudicate       reconcile   裁决（一次定终局）
GET  /api/consume/cursors            consume     只读
POST /api/consume/pull               consume     拉动放行（缺口拦截）
POST/PUT /api/consume/cursors...     任意登录     405（游标不可直接改）
GET  /api/internal/sync              仅内网 token 副本增量同步

# 副本 :4001（只验主站同源 JWT，不签发登录）
GET  /api/replica/status             已登录
POST /api/replica/sync-mode          ingest      normal | lag | timeout | paused
POST /api/replica/sync-now           ingest      立即同步
POST /api/consume/pull               consume     缺口感知放行
POST /api/events/batch               任意登录     405 REPLICA_READ_ONLY
```

## 本地开发（不用 Docker）

```bash
npm install
# 终端 1
DATA_DIR=./data/primary PORT=4000 node src/primary/server.js
# 终端 2
DATA_DIR=./data/replica PORT=4001 PRIMARY_URL=http://127.0.0.1:4000 node src/replica/server.js
# 端到端测试（真实拉起两进程，90 条断言）
npm run smoke
```

仅依赖 `express`；JWT 为内置 HS256，密码 scrypt 存储。生产部署请通过环境变量覆盖：
`JWT_SECRET`、`INTERNAL_TOKEN`（主/副必须一致）。
