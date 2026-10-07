# EdgeCast · 边缘地图包分批投放编排器

零第三方依赖（Python 3.11 标准库 + 原生 HTML/JS），为边缘地图包提供
"**按设备分组 · 发布时间窗 · 分组并发上限 · 回执阈值**"的分批投放编排，
状态由一份 **append-only 事件日志**（`data/edgecast/events.log`）驱动，
崩溃后重放日志即可完整恢复队列、计数、时间窗与"每设备已投票"状态。

- 操作员可在**网页**（SSE 实时看板）和**命令行**创建批次、暂停/恢复/撤销。
- 调度器按**分组**推进：排队 → 投放 →（暂停）→ 完成 / 撤销；
  超时设备进入该组**待重试队列**，由调度器在分组并发上限有空位时重投。
- 设备回执走**独立入口**（`POST /api/edgecast/receipts` / `edgecast_cli.py ack`），
  按 (设备, 尝试代次, 序号) **只处理一次**。
- 批次完成后输出**可下载摘要**（TXT/JSON），并可按分组/设备查看**完整时间线**。

## 启动

```bash
python3 edgecast_server.py 8090
# 打开 http://localhost:8090/edgecast/
# 可选：--data 目录  --no-scheduler（手动 tick 驱动，适合确定性演示）  --interval 秒
```

命令行与网页共用同一份日志（`EDGECAST_DATA` 环境变量或默认 `data/edgecast`）。
一键演示：`bash demo_edgecast.sh`（重置 `data/edgecast-demo` 后走完整链路）。
自动化验收：`python3 test_edgecast.py`（24 个用例，覆盖全部 8 类需求场景 + HTTP 冒烟）。

## 领域模型与不变量

| 概念 | 说明 |
|---|---|
| 包 `package` | `package_id + version`；登记更新版本覆盖，**登记旧版本直接拒绝**（`EDGE_OLD_PACKAGE`） |
| 设备 `device` | 以设备序号标识；批次名单内设备自动注册。名单外设备回执 → `EDGE_UNKNOWN_DEVICE` |
| 批次 `BC-####` | 分组名单 + 发布时间窗 `[window_start, window_end]` + 每分组并发上限 + 单设备超时 + 重试策略 + 回执阈值 |
| 尝试代次 `attempt` | 首发为 1；超时重投递增。**每代有独立的回执序号空间**（expected/processed/buffered/rejected） |
| 设备终态 | `succeeded`（终态成功票）/ `failed`（终态失败或超时用尽或窗内未投出）/ `canceled`（撤销） |
| 批次状态 | `queued 排队中 / active 投放中 / paused 已暂停 / completed 已完成 / revoked 已撤销` |
| 回执阈值 | 成功设备数（`count`）或成功比例（`ratio`）；全部设备终态后按阈值判定批次 `success/failed` |
| 事件日志 | 每行一个 JSON envelope `{id, ts, type, data}`；append 即 `fsync`，`id` 单调递增 |

### 回执只处理一次（独立入口）

回执正文为 `{device_id, package_id, package_version, attempt, seq, status, detail}`，
另带 `checksum = sha256(规范化正文)`，`status ∈ {received, installed, success, failure}`，
`success/failure` 是终态。判定顺序（全部在单把锁内串行化为追加事件）：

1. **字段 / 摘要校验**：字段非法 → `BAD_REQUEST`；摘要不匹配 → `EDGE_ACK_CHECKSUM`；
   设备不在任何名单/注册表 → `EDGE_UNKNOWN_DEVICE`。
2. **版本闸门**（优先于一切同号判定）：版本低于批次要求 → `EDGE_OLD_PACKAGE`；
   不一致 → `EDGE_VERSION_MISMATCH`。不同版本是不同正文命名空间，不会因序号相同
   而命中"重复/冲突"。
3. **同设备同代次同序号去重**：
   - 已处理过且**正文摘要相同** → 返回**首次结果**（`idempotent/duplicate=true`），不重复计票；
   - 已处理过但**正文摘要不同** → 永久拒绝 `EDGE_ACK_CONFLICT`；
   - 已被判定拒绝（旧代次迟到/撤销竞争落败）的相同正文 → 返回首次拒绝判定（`ack_stale`/
     `ack_decision` 也落日志，重启后依然判定一次终身）。
4. **批次状态闸门**：暂停不影响在飞回执；撤销/完成后的**新**回执 →
   `EDGE_BATCH_REVOKED` / `EDGE_BATCH_CLOSED`（已处理的重复回执仍回首次结果）。
5. **尝试代次闸门**：超时重投后迟到的**旧代次新回执** → `EDGE_ACK_STALE_ATTEMPT`，
   不再计票，响应携带首次超时结果；重复旧代次回执仍回首次判定。
6. **序号闸门**：`seq > 期望序号` 为乱序 → 先**暂存**（完整正文落 `ack_buffered`，
   重启不丢），等缺口补齐时按序连续冲刷；`seq = 期望序号` 才记录。

### 调度与并发

- 调度点（`tick`，调度线程默认每 0.1s 一次，或创建/恢复时触发一次）按
  **超时清扫 → 时间窗收尾 → 分组按上限补位（pump）** 推进。
- **超时清扫是两阶段的**：先把本调度点到期的在飞设备统一判定（入待重试 / 用尽失败），
  下一调度点再补位——避免先超时者在同一 tick 立即重投占满名额、挤掉同 tick 后超时者。
- 名额次序：**退避到点的待重试（先超时者优先）→ 排队设备**；退避未到点的重试不阻塞
  排队设备占用空闲名额，但任何时刻组内在飞数 **≤ 分组并发上限 cap**。
- **暂停**：冻结该批次所有补位（在飞设备继续等回执/超时）；恢复后继续。
- **撤销**与终态回执、超时同刻竞争时经单锁串行化，每个设备只有**一个确定终态**
  （成功/失败 或 撤销，不叠加）。
- **时间窗结束**：仍排队或待重试的设备判定失败（`EDGE_WINDOW_EXPIRED`）；
  在飞设备保留，由其回执/超时自然终结，批次因此一定能收敛。
- 所有设备终态且各队列清空 → 恰好一次 `batch_completed`（重放折叠不会重复产生）。

### 崩溃恢复

恢复 = 新建引擎时重放 `events.log`：包、设备、批次（分组、时间窗、阈值、超时参数）、
每设备每代次的 `expected_seq / processed / buffered / rejected / votes / final`、
待重试队列与 `next_attempt_at`、计数、批次判定全部由事件折叠重建。**终票只在
`ack_recorded(final=true)` 折叠时投出一次**，因此继续投放绝不会重复计票。
日志尾部若出现损坏行会拒绝启动（宁可停下也不带错恢复）；`events.log.lock` 用
`flock` 保证同一目录只有一个编排器实例在推进。

## API（均为 JSON，前缀 `/api/edgecast`）

| 方法 | 路径 | 作用 |
|---|---|---|
| GET | `/snapshot` | 总览：包/设备/批次精简视图/最近拒绝 |
| POST | `/packages` | 登记包 `{package_id, version, note?}`（旧版本拒绝） |
| POST | `/devices` | 显式注册设备（批次名单也会自动注册） |
| GET/POST | `/batches` | 批次列表 · 创建（见下） |
| GET | `/batches/{id}` | 批次详情（分组队列/在飞/重试/逐设备序号空间） |
| POST | `/batches/{id}/pause` · `/resume` · `/revoke` | 暂停（带 reason）· 恢复 · 撤销；重复操作返回首次结果 |
| POST | `/receipts` | **设备回执独立入口**（永远返回确定判定；重复→首次结果） |
| POST | `/tick` | 手动推进一个调度点（`--no-scheduler` 部署用） |
| GET | `/batches/{id}/timeline?group=&device=` | 完整时间线，可按分组/设备过滤 |
| GET | `/batches/{id}/summary?format=text` | 可下载批次摘要（默认 JSON；`text` 为附件 TXT） |
| GET | `/rejects` · `/events` | 拒绝记录 · 追加事件流 |
| GET | `/events/stream` | **SSE 实时事件流**（网页看板据此实时刷新） |

创建批次请求体：

```json
{
  "package_id": "MAP-EDGE-A", "package_version": "1.0.0",
  "groups": [{"name": "north", "devices": ["D1", "D2"], "cap": 2},
             {"name": "south", "devices": ["D3"], "cap": 1}],
  "window_start": "2026-10-06T12:00:00+00:00",   // 可省略；也支持 "+60" 秒
  "window_end":   "2026-10-06T12:30:00+00:00",   // 或用 window_sec 相对秒数
  "dispatch_timeout_sec": 15, "max_retries": 2, "retry_delay_sec": 1,
  "threshold_mode": "count", "ack_threshold": 2,  // 或 mode=ratio, threshold=0.8
  "note": "晚高峰边缘包"
}
```

**拒绝码**：`EDGE_OLD_PACKAGE`（旧版本包）、`EDGE_UNKNOWN_DEVICE`（未知设备）、
`EDGE_ACK_CHECKSUM`（摘要校验失败）、`EDGE_VERSION_MISMATCH`（版本不一致）、
`EDGE_ACK_CONFLICT`（同号不同正文）、`EDGE_ACK_STALE_ATTEMPT`（旧代次迟到）、
`EDGE_ACK_UNKNOWN_ATTEMPT` / `EDGE_NOT_DISPATCHED`（代次不合法）、
`EDGE_BATCH_REVOKED` / `EDGE_BATCH_CLOSED` / `EDGE_NO_OPEN_BATCH`
（批次不可受理）、`EDGE_WINDOW_EXPIRED`、`EDGE_TIMEOUT_EXHAUSTED`、
`EDGE_DEVICE_FAILURE`、`EDGE_DEVICE_BUSY`（同设备同包已在开放批次中）。

## CLI：edgecast_cli.py

```bash
# 包 / 设备
./edgecast_cli.py package MAP-EDGE-A 1.2.0 "晚高峰边缘包"
./edgecast_cli.py devices
# 创建批次（分组 JSON；分组内 cap 或全局 concurrency）
./edgecast_cli.py new MAP-EDGE-A 1.0.0 \
  '[{"name":"north","devices":["D1","D2"],"cap":2},{"name":"south","devices":["D3"],"cap":1}]' \
  --window 3600 --timeout 15 --retries 2 --retry-delay 1 --threshold 2
./edgecast_cli.py batches · show BC-0001
./edgecast_cli.py timeline BC-0001 --group north --device D1
./edgecast_cli.py pause BC-0001 "观察异常" · resume BC-0001 · revoke BC-0001 "紧急撤回"
./edgecast_cli.py tick                       # 手动调度点
./edgecast_cli.py summary BC-0001 [out.txt]  # 摘要（带路径则落文件）
# 设备回执独立入口（序号/代次显式可控，便于演示乱序、重复、冲突）
./edgecast_cli.py ack D1 MAP-EDGE-A 1.0.0 1 success
./edgecast_cli.py ack D1 MAP-EDGE-A 1.0.0 1 installed --seq 2     # 乱序
./edgecast_cli.py ack D1 MAP-EDGE-A 1.0.0 1 success --detail x    # 同号不同正文→冲突
./edgecast_cli.py ack D1 MAP-EDGE-A 1.0.0 1 success --bad-checksum
./edgecast_cli.py rejects · events [N]
```

## 页面（/edgecast/）

- **📦 投放批次**：批次卡片（成功/失败/撤销/在飞/待重试/排队分段进度条）+ 详情抽屉：
  分组名额与队列、在飞设备的代次/截止时间、待重试的下代次与最早重投时间、
  逐设备序号空间（暂存/已处理/票数）、暂停/恢复/撤销按钮、**摘要下载**、
  按分组/设备过滤的完整时间线。
- **➕ 创建批次**：包版本、时间窗、超时/重试/退避、阈值（数量或比例）、可视化分组编辑器。
- **🛰️ 回执入口**：设备回执独立表单（代次/序号/状态/正文/篡改摘要/伪装旧版本开关），
  直接展示"首次结果 / 重复 / 暂存 / 冲突 / 拒绝码"。
- **🚫 拒绝记录** 与 **🧾 事件流**：拒绝原因与 append-only 事件实时滚动（SSE）。

## 验收覆盖（`python3 test_edgecast.py`）

| 场景 | 用例 |
|---|---|
| 正常投放 | 时间窗前后投放、分组 cap、槽位补位、数量/比例阈值、完成判定、摘要 |
| 重复回执 | 进度/终态重复回首次结果、跨重启不重复、票数恒为 1 |
| 乱序回执 | 先暂存、缺口补齐按序冲刷、暂存跨重启仍可冲刷、重复暂存回首次 |
| 同号冲突 | 同代次同序号不同正文永久拒绝（含暂存条目与已成功终票） |
| 暂停撤销竞争 | 暂停冻结补位/恢复继续；终票 vs 撤销 30 轮并发压测只有一个确定终态；撤销清空队列 |
| 超时重试 | 入待重试队列、退避、重试优先补位、重投不突破 cap、用尽失败、旧代次迟到回执 |
| 重启恢复 | 队列/计数/时间窗/退避时间/已投票重建，继续投放不重复计票，窗过期可收敛 |
| 旧包拒绝 | 登记旧版本拒绝、回执旧版本拒绝（优先于同号判定）、未知设备、摘要失败、非法字段 |
| 同源 / HTTP | 时间线按分组与设备过滤；CLI 与网页同日志；HTTP 全流程 + 摘要附件下载 + SSE/UI 冒烟 |

## 文件

```
edgecast.py            事件溯源编排引擎：append-only 日志、折叠恢复、分组调度、回执去重/暂存/冲突
edgecast_server.py     标准库 HTTP API + SSE 实时流 + 静态页
edgecast_cli.py        操作员/设备 CLI（批次全生命周期 + 独立 ack 回执入口）
static_edgecast/       控制台（index.html / app.js / styles.css）
test_edgecast.py       24 个端到端验收用例（含并发竞争、重启恢复、HTTP 冒烟）
demo_edgecast.sh       一键演示（正常/乱序/重复/冲突/摘要失败/暂停恢复/时间线）
data/edgecast/         默认存储：events.log（追加日志）+ events.log.lock（单实例锁）
```
