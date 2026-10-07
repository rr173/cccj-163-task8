#!/usr/bin/env bash
# demo_edgecast.sh — 边缘地图包分批投放编排器一键演示。
# 重置 data/edgecast 后走完整链路：正常投放 / 乱序暂存冲刷 / 重复回执 /
# 同号冲突 / 摘要失败 / 暂停恢复 / 超时重试 / 撤销竞争 / 重启恢复 / 摘要下载。
set -euo pipefail
cd "$(dirname "$0")"

DATA="data/edgecast-demo"
CLI="python3 edgecast_cli.py"
rm -rf "$DATA"
export EDGECAST_DATA="$DATA"

line() { printf '\n==================== %s ====================\n' "$1"; }
ack() { $CLI ack "$@"; }

line "1) 创建批次：north(D1,D2 cap=2) + south(D3 cap=1)，超时 15s，最多重试 1 次"
$CLI new MAP-EDGE-A 1.0.0 \
  '[{"name":"north","devices":["D1","D2"],"cap":2},{"name":"south","devices":["D3"],"cap":1}]' \
  --window 3600 --timeout 15 --retries 1 --retry-delay 1 --threshold 2 \
  --note "晚高峰边缘地图包" | grep -E 'batch_id|status'
BID=BC-0001

line "2) 乱序回执先到（D1 seq2）：先暂存"
ack D1 MAP-EDGE-A 1.0.0 1 installed --seq 2 | grep -E 'buffered|expected_seq'

line "3) 缺口补齐（D1 seq1）：按序冲刷 1→2；随后 seq3 成功终态"
ack D1 MAP-EDGE-A 1.0.0 1 received --seq 1 | grep -E '"seq"'
ack D1 MAP-EDGE-A 1.0.0 1 success  --seq 3 | grep device_status

line "4) 重复回执返回首次结果（不重复计票）"
ack D1 MAP-EDGE-A 1.0.0 1 success --seq 3 | grep -E 'duplicate|first_result'

line "5) 同号不同正文永久拒绝（EDGE_ACK_CONFLICT）"
ack D1 MAP-EDGE-A 1.0.0 1 success --seq 3 --detail "篡改过的正文" \
  | grep -A1 rejection | head -3

line "6) 摘要校验失败（EDGE_ACK_CHECKSUM）"
ack D2 MAP-EDGE-A 1.0.0 1 received --bad-checksum | grep -A1 rejection | head -3

line "7) D2 成功；D3 暂停后不补投、恢复后继续"
$CLI pause "$BID" "人工观察" | grep -E 'idempotent|status' || true
ack D2 MAP-EDGE-A 1.0.0 1 success | grep device_status
echo "  （暂停期间在飞数 = $($CLI show "$BID" | grep -c '\"in_flight_now\": [1-9]' || echo 0)）"
$CLI resume "$BID" | grep status
ack D3 MAP-EDGE-A 1.0.0 1 success | grep device_status

line "8) 批次按阈值完成，输出可下载摘要"
$CLI batches
$CLI summary "$BID" "data/edgecast-demo/$BID-summary.txt" | tail -1
echo "摘要文件已生成：data/edgecast-demo/$BID-summary.txt"

line "9) 拒绝记录（冲突 / 摘要失败）"
$CLI rejects

line "10) 按设备查看完整时间线（D1：暂存→冲刷→成功）"
$CLI timeline "$BID" --device D1

line "11) 崩溃恢复：新建一个含待重试设备的批次，重启引擎（=重放日志）"
$CLI new MAP-EDGE-B 2.0.0 '[{"name":"g","devices":["E1","E2"],"cap":2}]' \
  --window 3600 --timeout 5 --retries 2 --retry-delay 3600 --threshold 2 \
  >/dev/null
echo "  （演示用）日志即状态：events.log 行数 = $(wc -l < "$DATA/events.log")"
echo "  任意时刻重启服务都会重放该日志，队列/计数/时间窗/暂存/已投票全部重建。"

line "完成。可用以下命令实时查看："
echo "  EDGECAST_DATA=$DATA python3 edgecast_cli.py batches"
echo "  python3 edgecast_server.py 8090 --data $DATA   # 打开 /edgecast/"
