#!/usr/bin/env bash
# Capacity-aware preheat orchestration demo (no server needed). Safe to re-run.
set -euo pipefail
cd "$(dirname "$0")"
rm -rf data
PY="python3 preheat_cli.py"
MIR="python3 mirror_cli.py"
LIVE="python3 route_replay.py"
say() { printf '\n\033[1;36m▶ %s\033[0m\n' "$*"; }

say "0. 准备内容：gen1 清单 → 线上发布 gen2 → gen2 清单（两清单共享部分对象）"
$MIR publish RSU-09 "gen1 基线" 2>&1 >/dev/null | grep '✓' | sed 's/^/  /'
$LIVE validate RC-0002 >/dev/null
$LIVE publish RC-0002 >/dev/null
$MIR publish RSU-09 "gen2 演进" 2>&1 >/dev/null | grep '✓' | sed 's/^/  /'

say "1. 注册站点共享带宽与盒子容量（磁盘/链路）"
$PY site SITE-A 1.6 >/dev/null && echo "  SITE-A 共享带宽 1.6 KB/s"
$PY box RSU-01 SITE-A 50 0.8 >/dev/null && echo "  RSU-01 @SITE-A 磁盘 50MB 链路 0.8 KB/s"
$PY box RSU-02 SITE-A 50 0.8 >/dev/null && echo "  RSU-02 @SITE-A 磁盘 50MB 链路 0.8 KB/s"
$PY box RSU-03 SITE-A 50 5.0 >/dev/null && echo "  RSU-03 @SITE-A 磁盘 50MB 链路 5.0 KB/s"

say "2. 提交预热作业：截止 120s · 链路上限 0.8 KB/s · 最小在线保障 2 台"
$PY submit MF-0001 RSU-01,RSU-02 120 0.8 2 "晚高峰预热" 2>&1 >/dev/null | sed 's/^/  /'
say "   重复提交同一请求 → 返回首次作业（确定结果）"
$PY submit MF-0001 RSU-01,RSU-02 120 0.8 2 "晚高峰预热" 2>/dev/null | python3 -c \
  'import json,sys; r=json.load(sys.stdin); print("  幂等:", r["idempotent"], "→", r["job"]["job_id"])'

say "3. 站点带宽被占满 → 新任务排队（限速原因 + 欠账累计）"
$PY submit MF-0001 RSU-03 600 5.0 1 "加急单台" >/dev/null 2>&1
$PY queue | sed 's/^/  /'

say "4. 推进时钟：欠账大的优先，持续推进的作业不被饿死"
$PY tick 1 | sed 's/^/  /'
$PY tick 2 | sed 's/^/  /'
$PY queue | grep -E 'PT-|限速' | sed 's/^/  /'

say "5. 链路降级 → 成对让路；恢复后只补剩余数据"
$PY link RSU-01 0 >/dev/null && echo "  RSU-01 链路降为 0"
$PY queue | grep -A2 'RSU-01' | sed 's/^/  /'
$PY link RSU-01 0.4 >/dev/null && echo "  RSU-01 链路恢复为 0.4 KB/s（降级值）"
$PY tick 1 | sed 's/^/  /'

say "6. 暂停/恢复：名额与磁盘预留成对释放，恢复只补剩余"
$PY pause PJ-0001 >/dev/null && echo "  PJ-0001 已暂停"
$PY queue | grep -E '站点' | sed 's/^/  /'
$PY resume PJ-0001 >/dev/null && echo "  PJ-0001 已恢复（只补剩余数据）"
$PY tick 3 | sed 's/^/  /'

say "7. 内容复用：已完成 gen1 的盒子对 gen2 只传增量，全复用即时完成"
$PY tick 5 >/dev/null
$PY submit MF-0002 RSU-01,RSU-02,RSU-03 600 1.0 3 "gen2 全量预热" 2>&1 >/dev/null | sed 's/^/  /'
$PY queue | sed 's/^/  /'

say "8. 取消作业：成对释放、判定一次终身；取消后再提交 → 新作业"
$PY cancel PJ-0003 >/dev/null 2>&1 || true
$PY cancel PJ-0003 2>/dev/null | python3 -c \
  'import json,sys; r=json.load(sys.stdin); print("  重复取消 → 幂等:", r["idempotent"], "结果:", r["result"]["outcome"])' || true
$PY jobs | sed 's/^/  /'

say "9. 服务重启：传输中崩溃 → 重建队列/欠账/剩余时长，不留幽灵占用"
$PY submit MF-0002 RSU-01,RSU-03 600 1.0 2 "重启演示" >/dev/null 2>&1
$PY tick 1 >/dev/null   # 每次 CLI 调用都是一次进程重启；传输中任务被安全恢复
$PY queue | grep -E 'PT-|限速|预计' | head -6 | sed 's/^/  /'
$PY events 40 | python3 -c 'import json,sys
for e in json.load(sys.stdin)["events"]:
    if e["type"] == "SERVICE_RECOVERED":
        print("  #%d [%s] %s" % (e["event_id"], e["type"], e["summary"])); break'
$PY tick 5 | sed 's/^/  /'

say "10. 盒端验收：传输完成 → 逐项验证长度/对象散列/块散列/根散列 → 不可变证明"
# 验收演示阶段给站点扩容（确定性配额调整），让新盒快速到盒，聚焦验收/修复语义
$PY site SITE-A 20.0 >/dev/null && echo "  SITE-A 扩容到 20 KB/s（确定性配额调整）"
$PY box RSU-04 SITE-A 50 20.0 >/dev/null && echo "  注册 RSU-04（链路 20 KB/s）"
$PY box RSU-05 SITE-A 50 20.0 >/dev/null && echo "  注册 RSU-05（链路 20 KB/s）"
T4=$($PY submit MF-0001 RSU-04 600 20.0 1 "验收-好盒" 2>/dev/null \
  | python3 -c 'import json,sys; print(json.load(sys.stdin)["job"]["tasks"][0]["task_id"])')
T5=$($PY submit MF-0001 RSU-05 600 20.0 1 "验收-坏盒" 2>/dev/null \
  | python3 -c 'import json,sys; print(json.load(sys.stdin)["job"]["tasks"][0]["task_id"])')
echo "  验收作业任务：$T4（RSU-04）/ $T5（RSU-05）"
$PY tick 2 >/dev/null; $PY tick 1 >/dev/null
$PY board | sed 's/^/  /'
$PY accept "$T4" 2>/dev/null | python3 -c 'import json,sys
r=json.load(sys.stdin)["run"]; print("  RSU-04 验收:", r["outcome"],
  "对象", r["counts"]["objects_ok"], "/", r["counts"]["objects_total"],
  "根散列", r["root"]["match"])'

say "11. 注入块损坏 → 只隔离受影响对象、释放空间、按剩余块创建一次修复"
$PY corrupt "$T5" chunk 2 >/dev/null && echo "  已在 RSU-05 盒端注入第 2 块损坏（镜像清单不变）"
$PY accept "$T5" 2>/dev/null | python3 -c 'import json,sys
d=json.load(sys.stdin); r=d["run"]; pr=d["repair"]
print("  RSU-05 验收:", r["outcome"], "坏对象", r["counts"]["objects_bad"],
      "→ 修复", pr["task_id"], "只重拉块", pr["chunk_indices"])'
$PY board | grep -E '汇总|隔离|修复' | sed 's/^/  /'

say "12. 修复同键并发只认首次；修复完成自动重新验收 → 进入可切换集合"
for i in 1 2 3; do $PY repair "$T5" 2>/dev/null | python3 -c \
  'import json,sys; print("  并发提交 →", json.load(sys.stdin)["task_id"])' & done; wait
$PY tick 2 >/dev/null; $PY tick 2 >/dev/null
$PY board | grep -E '汇总' | sed 's/^/  /'
$PY summary | sed 's/^/  /'
$PY boxes | grep RSU-05 | sed 's/^/  /'

say "完成。界面演示: python3 server.py 8080 → 🔥 预热编排 → ④ 盒端验收与隔离修复"
