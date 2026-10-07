#!/usr/bin/env python3
"""
edgecast_cli.py — 边缘地图包分批投放编排器命令行。

与网页共用同一份 append-only 日志（data/edgecast/events.log），排队、投放、
暂停、完成、撤销与拒绝原因完全同源。设备回执通过 `ack` 子命令进入**独立入口**。

用法：
  # 包与设备
  ./edgecast_cli.py package MAP-EDGE-A 1.2.0 "晚高峰边缘包"
  ./edgecast_cli.py devices

  # 批次（分组 JSON / 分组并发 / 时间窗 / 阈值）
  ./edgecast_cli.py new MAP-EDGE-A 1.2.0 \
      '[{"name":"north","devices":["D1","D2"],"cap":2},{"name":"south","devices":["D3"]}]' \
      --window 300 --timeout 15 --retries 2 --retry-delay 1 --threshold 2
  ./edgecast_cli.py batches · show BC-0001 · timeline BC-0001 [--group north] [--device D1]
  ./edgecast_cli.py pause BC-0001 "观察异常" · resume BC-0001 · revoke BC-0001 "紧急撤回"
  ./edgecast_cli.py tick [秒]                         # 手动推进（未开调度线程时用）
  ./edgecast_cli.py summary BC-0001 [out.txt]         # 批次摘要（可下载同源文本）

  # 设备回执独立入口（--seq 序号、--attempt 代次、乱序/重复/冲突均可演示）
  ./edgecast_cli.py ack D1 MAP-EDGE-A 1.2.0 1 success
  ./edgecast_cli.py ack D1 MAP-EDGE-A 1.2.0 --seq 2 --attempt 1 installed   # 乱序先到
  ./edgecast_cli.py ack D1 MAP-EDGE-A 1.2.0 1 success --detail "..."        # 同号不同正文→冲突
  ./edgecast_cli.py ack D1 MAP-EDGE-A 1.2.0 1 success --bad-checksum        # 摘要失败
  ./edgecast_cli.py rejects · events [N]
"""

from __future__ import annotations

import json
import os
import sys

from edgecast import EdgeCastEngine, receipt_hash

DATA = os.environ.get("EDGECAST_DATA", "data/edgecast")

STATUS_CN = {"queued": "排队中", "active": "投放中", "paused": "已暂停",
             "completed": "已完成", "revoked": "已撤销"}
DEV_CN = {"pending": "排队", "inflight": "投放中", "awaiting_retry": "待重试",
          "succeeded": "成功", "failed": "失败", "canceled": "已撤销"}


def engine():
    return EdgeCastEngine(f"{DATA}/events.log")


def j(o):
    print(json.dumps(o, ensure_ascii=False, indent=2))


def _groups_line(g):
    return (f"[{g['name']}] cap={g['cap']} 在飞={g['in_flight_now']} "
            f"排队={len(g['pending'])} 待重试={len(g['retry'])} "
            f"成功={len(g['succeeded'])} 失败={len(g['failed'])} "
            f"撤销={len(g['canceled'])}")


def cmd_package(a):
    note = a[3] if len(a) > 3 else ""
    j(ENG.register_package(a[0], a[1], note))


def cmd_devices(_):
    devs = ENG.snapshot()["devices"]
    if not devs:
        print("（暂无设备，创建批次时名单设备会自动注册）")
    for d in devs:
        print(f"{d['device_id']}  分组提示 {d.get('group_hint') or '-'}")


def cmd_new(a):
    groups = json.loads(a[2])
    body = {"package_id": a[0], "package_version": a[1], "groups": groups}
    # 可选具名参数（简单解析）
    rest = a[3:]
    flags = {"--window": ("window_sec", float),
             "--start": ("window_start", str),
             "--end": ("window_end", str),
             "--timeout": ("dispatch_timeout_sec", float),
             "--retries": ("max_retries", int),
             "--retry-delay": ("retry_delay_sec", float),
             "--threshold": ("ack_threshold", lambda x: float(x) if "." in x else int(x)),
             "--threshold-mode": ("threshold_mode", str),
             "--note": ("note", str)}
    i = 0
    while i < len(rest):
        if rest[i] in flags:
            key, cast = flags[rest[i]]
            body[key] = cast(rest[i + 1]); i += 2
        else:
            i += 1
    j(ENG.create_batch(body))


def cmd_batches(_):
    for b in ENG.list_batches():
        c = b["counts"]
        print(f"{b['batch_id']}  {b['package_id']}@{b['package_version']}  "
              f"[{STATUS_CN.get(b['status'], b['status'])}]  "
              f"窗 {b['window_start'][11:19]}~{b['window_end'][11:19]}  "
              f"阈值 {b['threshold']}"
              + ("（比例）" if b["threshold_mode"] == "ratio" else "")
              + f"  成功 {c['succeeded']} 失败 {c['failed']} 撤销 {c['canceled']}"
              + f"  投放 {c['dispatched']}（重投 {c['retried']}）")
        for g in b["groups"]:
            print("   " + _groups_line(g))
        if b["verdict"]:
            print(f"    判定: {b['verdict']['verdict']} "
                  f"({b['verdict']['succeeded']}/{b['verdict']['total']})")


def cmd_show(a):
    j(ENG.batch_view(a[0]))


def cmd_timeline(a):
    group = device = None
    if "--group" in a:
        group = a[a.index("--group") + 1]
    if "--device" in a:
        device = a[a.index("--device") + 1]
    tl = ENG.timeline(a[0], group, device)
    for ev in tl["events"]:
        d = ev["data"]
        did = d.get("device_id") or d.get("device") or ""
        print(f"#{ev['id']:<4} {ev['ts'][11:19]} {ev['type']:<20} "
              f"{did:<8} {json.dumps({k: v for k, v in d.items() if k not in ('payload',)}, ensure_ascii=False)}")


def cmd_pause(a):
    j(ENG.pause_batch(a[0], a[1] if len(a) > 1 else ""))


def cmd_resume(a):
    j(ENG.resume_batch(a[0]))


def cmd_revoke(a):
    j(ENG.revoke_batch(a[0], a[1] if len(a) > 1 else ""))


def cmd_summary(a):
    text = ENG.summary_text(a[0])
    if len(a) > 1:
        with open(a[1], "w", encoding="utf-8") as f:
            f.write(text)
        print(f"摘要已写入 {a[1]}")
    else:
        print(text, end="")
    j(ENG.summary(a[0]))


def cmd_tick(a):
    # 注意：默认调度线程使用墙钟；手动 tick 仅在 --no-scheduler 部署下有意义。
    # 这里仍调用一次即时推进（基于引擎当前时钟）。
    j(ENG.tick())


def cmd_ack(a):
    # 位置参数：device package version attempt? status?；序号走 --seq
    device, package, version = a[0], a[1], a[2]
    pos = a[3:]
    attempt, status = 1, "success"
    if pos and pos[0].isdigit():
        attempt = int(pos[0]); pos = pos[1:]
    if pos:
        status = pos[0]; pos = pos[1:]
    seq = 1
    detail = ""
    bad = False
    i = 0
    while i < len(pos):
        if pos[i] == "--seq":
            seq = int(pos[i + 1]); i += 2
        elif pos[i] == "--attempt":
            attempt = int(pos[i + 1]); i += 2
        elif pos[i] == "--detail":
            detail = pos[i + 1]; i += 2
        elif pos[i] == "--bad-checksum":
            bad = True; i += 1
        else:
            i += 1
    payload = {"device_id": device, "package_id": package,
               "package_version": version, "attempt": attempt, "seq": seq,
               "status": status, "detail": detail}
    payload["checksum"] = "sha256:bad" if bad else receipt_hash(payload)
    j(ENG.ingest_receipt(payload))


def cmd_rejects(_):
    for r in ENG.list_rejects(100):
        print(f"{r['ts'][11:19]} [{r['kind']}] {r['code']}  "
              f"{r.get('device_id')} a{r.get('attempt')}#s{r.get('seq')}  {r['message']}")


def cmd_events(a):
    limit = int(a[0]) if a else 60
    for ev in ENG.list_events(limit)[::-1]:
        print(f"#{ev['id']:<5} {ev['ts'][11:19]} {ev['type']}")


COMMANDS = {
    "package": (cmd_package, 3),
    "devices": (cmd_devices, 0),
    "new": (cmd_new, 3),
    "batches": (cmd_batches, 0),
    "show": (cmd_show, 1),
    "timeline": (cmd_timeline, 1),
    "pause": (cmd_pause, 1),
    "resume": (cmd_resume, 1),
    "revoke": (cmd_revoke, 1),
    "summary": (cmd_summary, 1),
    "tick": (cmd_tick, 0),
    "ack": (cmd_ack, 3),
    "rejects": (cmd_rejects, 0),
    "events": (cmd_events, 0),
}


def main(argv=None):
    argv = argv or sys.argv[1:]
    if not argv or argv[0] in ("-h", "--help", "help"):
        print(__doc__)
        return
    cmd = argv[0]
    if cmd not in COMMANDS:
        print(f"未知命令 {cmd}\n")
        print(__doc__)
        sys.exit(2)
    fn, min_args = COMMANDS[cmd]
    args = argv[1:]
    if len([x for x in args if not x.startswith("--")]) < min_args:
        print(f"命令 {cmd} 参数不足")
        sys.exit(2)
    global ENG
    ENG = engine()
    try:
        fn(args)
    finally:
        ENG.close()


ENG = None

if __name__ == "__main__":
    main()
