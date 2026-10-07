#!/usr/bin/env python3
"""
preheat_cli.py — capacity-aware preheat orchestration command line.

Reads/writes the SAME store as the web console (data/preheat.json): queue
order, rate-limit reasons and ETAs shown here are identical to the web page.
The mirror repository (data/mirror.json) and the live map service are only
ever read.

Units: rates in KB/s (1 KB = 1000 bytes), disk in MB.

Usage:
  ./preheat_cli.py sites
  ./preheat_cli.py site <site_id> <capacity_kbps>     # register / set shared bandwidth
  ./preheat_cli.py boxes
  ./preheat_cli.py box <box_id> <site_id> <disk_mb> <link_kbps> [used_mb]
  ./preheat_cli.py link <box_id> <kbps>               # degrade / restore the box link
  ./preheat_cli.py offline <box_id> | online <box_id>
  ./preheat_cli.py submit <manifest_id> <box,csv> <deadline_sec> <link_kbps> <min_online> ["note"]
  ./preheat_cli.py jobs
  ./preheat_cli.py show <job_id>
  ./preheat_cli.py queue                              # order / limit reasons / ETA
  ./preheat_cli.py tick <seconds>                     # advance the simulated clock
  ./preheat_cli.py pause <job_id> | resume <job_id> | cancel <job_id>
  ./preheat_cli.py board [box_id]                     # 待验收/已证明/隔离/修复中
  ./preheat_cli.py accept <task_id>                   # 盒端逐项验收（长度/块/根散列）
  ./preheat_cli.py corrupt <task_id> [chunk|object|length|missing] [chunk|object_id]
  ./preheat_cli.py repair <task_id>                   # 隔离后创建/返回唯一修复
  ./preheat_cli.py repairs [task_id]                  # 修复清单
  ./preheat_cli.py cancel-repair <repair_id>          # 取消修复（收敛无残留）
  ./preheat_cli.py runs [box_id|task_id]              # 不可变验收记录与证明
  ./preheat_cli.py summary [site_id] [manifest_id]    # 通过率/隔离容量/失败原因汇总
  ./preheat_cli.py events [limit]
"""

from __future__ import annotations

import json
import sys

from engine import MapEngine, short
from mirror import MirrorEngine
from preheat import PreheatEngine

ENGINE = MapEngine("data/mapstate.json")
MIRROR = MirrorEngine("data/mirror.json", ENGINE)
PREHEAT = PreheatEngine("data/preheat.json", MIRROR)

STATUS_CN = {"queued": "排队中", "transferring": "传输中", "paused": "已暂停",
             "done": "已证明", "canceled": "已取消",
             "transferred": "待验收", "proven": "已证明",
             "quarantined": "隔离中"}
JOB_CN = {"active": "进行中", "paused": "已暂停", "completed": "已完成",
          "canceled": "已取消"}
REPAIR_CN = {"queued": "修复排队", "transferring": "修复传输中",
             "paused": "修复暂停", "done": "修复完成",
             "canceled": "修复取消", "superseded": "旧代次修复失效"}
STAGE_CN = {"transferring": "传输中", "pending": "待验收", "proven": "已证明",
            "quarantined": "隔离中", "repairing": "修复中", "canceled": "已取消"}
REASON_CN = {"ACCEPT_OBJECT_LENGTH": "长度不符", "ACCEPT_OBJECT_HASH": "对象散列不符",
             "ACCEPT_CHUNK_MISSING": "缺块", "ACCEPT_CHUNK_HASH": "块散列不符",
             "ACCEPT_ROOT_HASH": "根散列不符", "ACCEPT_OK": "通过"}


def j(obj):
    print(json.dumps(obj, ensure_ascii=False, indent=2))


def _task_line(t):
    line = (f"  #{t.get('rank', '-'):<3}{t['task_id']}  {t['box_id']:<9} "
            f"[{STATUS_CN.get(t['status'], t['status'])}] "
            f"{t['bytes_done']:.0f}/{t['bytes_total']:.0f} B  "
            f"速率 {t['rate_kbps']:g} KB/s  欠账 {t['debt_kb']:g} KB")
    if t.get("limit"):
        line += f"\n       限速原因: [{t['limit']['code']}] {t['limit']['message']}"
    if t.get("eta"):
        line += f"\n       预计完成: {t['eta']}"
    elif t["status"] in ("queued", "paused"):
        line += "\n       预计完成: 暂不可预计（等待条件恢复）"
    if t.get("at_risk"):
        line += "  ⚠ 超截止风险"
    return line


def cmd_sites(_):
    ss = PREHEAT.list_sites()
    if not ss:
        print("（暂无站点，先用 site 注册）")
        return
    for s in ss:
        print(f"{s['site_id']}  共享带宽 {s['capacity_kbps']:g} KB/s  "
              f"已授予 {s['granted_kbps']:g} KB/s  盒子 {s['online']}/{s['boxes']} 在线")


def cmd_site(a):
    j(PREHEAT.upsert_site(a[0], float(a[1])))


def cmd_boxes(_):
    bs = PREHEAT.list_boxes()
    if not bs:
        print("（暂无盒子，先用 box 注册）")
        return
    for b in bs:
        print(f"{b['box_id']}  @{b['site_id']}  "
              f"{'在线' if b['online'] else '离线'}  链路 {b['link_cap_kbps']:g} KB/s  "
              f"磁盘 已用 {b['disk_used_mb']:g} + 预留 {b['reserved_mb']:g} / "
              f"{b['disk_total_mb']:g} MB  已证明对象 {b['proven_objects']}"
              f"（可切换清单 {len(b['switchable'])}）")


def cmd_box(a):
    used = float(a[4]) if len(a) > 4 else 0.0
    j(PREHEAT.upsert_box(a[0], a[1], float(a[2]), float(a[3]), used))


def cmd_link(a):
    j(PREHEAT.set_box_link(a[0], float(a[1])))


def cmd_offline(a):
    j(PREHEAT.set_box_online(a[0], False))


def cmd_online(a):
    j(PREHEAT.set_box_online(a[0], True))


def cmd_submit(a):
    note = a[5] if len(a) > 5 else ""
    r = PREHEAT.create_job({
        "manifest_id": a[0],
        "targets": [x.strip() for x in a[1].split(",") if x.strip()],
        "deadline_in_sec": float(a[2]),
        "link_cap_kbps": float(a[3]),
        "min_online": int(a[4]),
        "note": note})
    job = r["job"]
    g = job["guarantee"]
    print(f"✓ 作业 {job['job_id']}（{job['version']} → {len(job['targets'])} 台，"
          f"截止 {job['deadline']}，最小在线保障 {g['required']}）"
          + ("（重复提交，返回首次作业）" if r["idempotent"] else ""), file=sys.stderr)
    for t in job["tasks"]:
        print(_task_line(t), file=sys.stderr)
    j({"job": job, "idempotent": r["idempotent"]})


def cmd_jobs(_):
    js = PREHEAT.list_jobs()
    if not js:
        print("（暂无作业）")
        return
    for x in js:
        p, g = x["progress"], x["guarantee"]
        risk = "  ⚠ 保障风险" if g["at_risk"] and x["status"] == "active" else ""
        print(f"{x['job_id']}  [{JOB_CN.get(x['status'], x['status'])}]  "
              f"{x['version']} → {p['done_tasks']}/{p['total_tasks']} 台  "
              f"{p['done_bytes']:.0f}/{p['total_bytes']:.0f} B  "
              f"截止 {x['deadline']}  保障 {g['done']}/{g['required']}{risk}")


def cmd_show(a):
    j(PREHEAT.get_job(a[0]))


def cmd_queue(_):
    q = PREHEAT.queue()
    print(f"模拟时钟: {q['clock']}")
    for s in q["sites"]:
        print(f"站点 {s['site_id']}: 已授予 {s['granted_kbps']:g}/{s['capacity_kbps']:g} KB/s"
              f"（空余 {s['free_kbps']:g}）  排队作业 {s['open_tasks']}")
    if not q["queue"]:
        print("（排队队列为空）")
    print("排队次序（调度优先级 = 欠账↓ · 截止↑ · 提交序↑）:")
    for t in q["queue"]:
        print(_task_line(t))
    resting = [t for t in q["inactive"] if t["status"] == "paused"]
    if resting:
        print("已暂停:")
        for t in resting:
            print(_task_line(t))


def cmd_tick(a):
    r = PREHEAT.tick(float(a[0]))
    print(f"✓ 时钟推进至 {r['clock']}")
    for x in r["advanced"]:
        print(f"  {x['task_id']}  已传 {x['bytes_done']:.0f}/{x['bytes_total']:.0f} B")
    for tid in r["completed"]:
        print(f"  ✓ {tid} 传输完成（磁盘预留与传输名额成对释放）")


def cmd_pause(a):
    j(PREHEAT.pause_job(a[0]))


def cmd_resume(a):
    j(PREHEAT.resume_job(a[0]))


def cmd_cancel(a):
    r = PREHEAT.cancel_job(a[0])
    j(r)
    print(f"✓ 作业 {a[0]} 已取消（成对释放 {r['result']['released_pairs']} 对"
          f"磁盘预留/传输名额）"
          + ("（重复取消，返回首次结果）" if r["idempotent"] else ""), file=sys.stderr)


def cmd_events(a):
    j({"events": PREHEAT.list_events(int(a[0]) if a else 80)})


def _board_row(r):
    q = r.get("quarantine")
    line = (f"  {r['task_id']}  {r['box_id']:<9} [{STAGE_CN.get(r['stage'], r['stage'])}] "
            f"{r['manifest_id']} · {r['version']} · 第 {r['accept_seq']} 次验收")
    if q and q.get("object_count"):
        line += (f"\n       隔离 {q['object_count']} 对象 / 块 {q['chunk_indices']}，"
                 f"原因 {'/'.join(REASON_CN.get(c, c) for c in q['reasons'])}，"
                 f"已释放 {q['released_bytes']:.0f} B"
                 + (f"，修复 {q['repair_id']}" if q.get("repair_id") else ""))
    elif q and q.get("resolution"):
        line += f"\n       历史隔离（第 {q['generation']} 代）已于 {q['resolved_at']} {q['resolution']}"
    ar = r.get("active_repair")
    if ar:
        line += (f"\n       修复 {ar['task_id']}：{REPAIR_CN.get(ar['status'], ar['status'])} "
                 f"{ar['bytes_done']:.0f}/{ar['bytes_total']:.0f} B，块 {ar['chunk_indices']}")
    return line


def cmd_board(a):
    b = PREHEAT.acceptance_board(a[0] if a else None)
    print(f"模拟时钟: {b['clock']}")
    c = b["counts"]
    print(f"汇总: 待验收 {c['pending']} · 已证明 {c['proven']} · "
          f"隔离 {c['quarantined']} · 修复中 {c['repairing']} · "
          f"传输中 {c['transferring']} · 已取消 {c['canceled']}")
    for key, title in (("pending", "待验收"), ("repairing", "修复中"),
                       ("quarantined", "隔离中（无活跃修复）"),
                       ("proven", "已证明（进入可切换集合）"),
                       ("transferring", "传输中")):
        rows = b[key]
        if not rows:
            continue
        print(f"-- {title} --")
        for r in rows:
            print(_board_row(r))


def cmd_accept(a):
    r = PREHEAT.accept_task(a[0])
    run = r["run"]
    print(f"{'✓' if run['outcome'] == 'proven' else '⚠'} {run['run_id']} "
          f"（盒 {run['box_id']} 第 {run['accept_seq']} 次验收，作业 {run['job_id']}）"
          + ("  [幂等：已证明] " if r.get("idempotent") else ""), file=sys.stderr)
    print(f"  根散列: 声明 {run['root']['declared']}\n"
          f"          复核 {run['root']['computed']}  match={run['root']['match']}",
          file=sys.stderr)
    for o in run["objects"]:
        mark = "✓" if o["result"] in ("ok", "reused") else "✗"
        print(f"  {mark} {o['object_id']:<14} len={o['length']} {o['result']:<6} "
              f"{REASON_CN.get(o['reason'], o['reason'])}", file=sys.stderr)
    if r.get("repair"):
        print(f"  → 创建/返回修复 {r['repair']['task_id']}（块 "
              f"{r['repair']['chunk_indices']}，同键并发返回首次任务）", file=sys.stderr)
    j(r)


def cmd_corrupt(a):
    tid = a[0]
    mode = a[1] if len(a) > 1 else "chunk"
    target = a[2] if len(a) > 2 else None
    chunk = None
    object_id = None
    if target is not None:
        if mode in ("chunk", "missing"):
            chunk = int(target)
        else:
            object_id = target
    j(PREHEAT.simulate_corruption(tid, chunk=chunk, object_id=object_id, mode=mode))


def cmd_repair(a):
    r = PREHEAT.create_repair(a[0])
    print(f"{'✓' if not r.get('idempotent') else '='} 修复 {r['task_id']} "
          f"（父任务 {r['parent_task_id']}，第 {r['repair_generation']} 代，"
          f"块 {r['chunk_indices']}，{r['bytes_total']:.0f} B）"
          + ("（同键并发，返回首次任务）" if r.get("idempotent") else ""),
          file=sys.stderr)
    j(r)


def cmd_repairs(a):
    task = a[0] if a else None
    rs = PREHEAT.list_repairs(task_id=task)
    if not rs:
        print("（暂无修复）")
        return
    for r in rs:
        print(f"{r['task_id']}  [{REPAIR_CN.get(r['status'], r['status'])}]  "
              f"父 {r['parent_task_id']} · 第 {r['repair_generation']} 代 · "
              f"块 {r['chunk_indices']} · {r['bytes_done']:.0f}/{r['bytes_total']:.0f} B"
              + (f" · 结果 {r['result']['outcome']}" if r.get("result") else ""))


def cmd_cancel_repair(a):
    r = PREHEAT.cancel_repair(a[0])
    j(r)
    print(f"✓ 修复 {a[0]}：{r['repair']['status']}"
          + ("（重复操作，返回首次结果）" if r.get("idempotent") else
             "；预留与名额成对释放，隔离保留，无容量残留"), file=sys.stderr)


def cmd_runs(a):
    box = task = None
    if a:
        ref = a[0]
        if ref.startswith("PT-"):
            task = ref
        else:
            box = ref
    runs = PREHEAT.list_runs(box_id=box, task_id=task)
    if not runs:
        print("（暂无验收记录）")
        return
    for run in runs:
        print(f"{run['run_id']}  盒 {run['box_id']} · {run['task_id']} · "
              f"作业 {run['job_id']} · 第 {run['accept_seq']} 次 · "
              f"{run['manifest_generations']} · {run['outcome']} · {run['at']}")
        print(f"  证明 {short(run['proof_digest'])}（父 {short(run['parent_digest'])}）"
              f"  对象 {run['counts']['objects_ok']}/{run['counts']['objects_total']}"
              f"  块 {run['counts']['chunks_ok']}/{run['counts']['chunks_total']}")
        for o in run["objects"]:
            if o["result"] in ("ok", "reused"):
                continue
            print(f"    ✗ {o['object_id']}: {REASON_CN.get(o['reason'], o['reason'])}")


def cmd_summary(a):
    site = a[0] if len(a) > 0 else None
    manifest = a[1] if len(a) > 1 else None
    rep = PREHEAT.acceptance_summary(site_id=site, manifest_id=manifest)
    if not rep["groups"]:
        print("（暂无验收数据）")
        return
    for g in rep["groups"]:
        pr = "—" if g["pass_rate"] is None else f"{g['pass_rate'] * 100:.0f}%"
        opr = "—" if g["object_pass_rate"] is None else \
            f"{g['object_pass_rate'] * 100:.1f}%"
        print(f"站点 {g['site_id']} · 清单 {g['manifest_id']}（{g['version']}）")
        print(f"  任务 {g['tasks']}：已证明 {g['proven']} / 待验收 {g['pending']} / "
              f"隔离 {g['quarantined']}  任务通过率 {pr}  对象通过率 {opr}")
        print(f"  隔离容量 {g['quarantine_mb']:g} MB  修复 "
              f"开放 {g['repairs_open']} / 完成 {g['repairs_done']} / "
              f"取消 {g['repairs_canceled']} / 旧代次失效 {g['repairs_superseded']}")
        for fr in g["failure_reasons"]:
            print(f"    ✗ [{fr['code']}] {fr['message']} × {fr['count']}")
    j(rep)


CMDS = {
    "sites": (cmd_sites, 0),
    "site": (cmd_site, 2),
    "boxes": (cmd_boxes, 0),
    "box": (cmd_box, (4, 5)),
    "link": (cmd_link, 2),
    "offline": (cmd_offline, 1),
    "online": (cmd_online, 1),
    "submit": (cmd_submit, (5, 6)),
    "jobs": (cmd_jobs, 0),
    "show": (cmd_show, 1),
    "queue": (cmd_queue, 0),
    "tick": (cmd_tick, 1),
    "pause": (cmd_pause, 1),
    "resume": (cmd_resume, 1),
    "cancel": (cmd_cancel, 1),
    "board": (cmd_board, (0, 1)),
    "accept": (cmd_accept, 1),
    "corrupt": (cmd_corrupt, (1, 3)),
    "repair": (cmd_repair, 1),
    "repairs": (cmd_repairs, (0, 1)),
    "cancel-repair": (cmd_cancel_repair, 1),
    "runs": (cmd_runs, (0, 1)),
    "summary": (cmd_summary, (0, 2)),
    "events": (cmd_events, (0, 1)),
}


def main(argv):
    if not argv or argv[0] in ("-h", "--help"):
        print(__doc__)
        return 0
    fn, argc = CMDS[argv[0]]
    pos = argv[1:]
    ok = argc[0] <= len(pos) <= argc[1] if isinstance(argc, tuple) else len(pos) == argc
    if not ok:
        print(f"参数数量错误（{argv[0]} 需要 {argc} 个参数）\n\n{__doc__}")
        return 2
    try:
        fn(pos)
        return 0
    except Exception as e:
        code = getattr(e, "code", "ERROR")
        status = getattr(e, "status", 500)
        print(f"✗ [{status} {code}] {e}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
