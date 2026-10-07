#!/usr/bin/env python3
"""
test_edgecast.py — 边缘地图包分批投放编排器验收测试。

覆盖需求中列出的全部场景：
  1) 正常投放（时间窗/分组推进/并发上限/回执阈值/批次完成与摘要）
  2) 重复回执（跨重启；返回首次结果，不重复计票）
  3) 乱序回执（先暂存，缺口补齐按序冲刷）
  4) 同号冲突（同设备同代次同序号不同正文永久拒绝）
  5) 暂停 / 撤销与回执到达竞争（单确定终态，并发压测）
  6) 超时重试（进入待重试队列、重投不突破分组并发上限、迟到旧代次回执）
  7) 崩溃重启恢复（队列/计数/时间窗/暂存/已投票全部由日志重建）
  8) 旧版本包 / 未知设备 / 摘要校验失败拒绝并记录原因
另含：网页与 CLI 同源、HTTP/SSE/摘要下载冒烟。
"""

import datetime
import json
import os
import subprocess
import sys
import tempfile
import threading
import time
import unittest
import urllib.request
from datetime import timedelta

from edgecast import EdgeCastEngine, receipt_hash

BASE = datetime.datetime(2026, 10, 6, 12, 0, 0, tzinfo=datetime.timezone.utc)


class FakeClock:
    """可注入、可暂停/快进的确定性时钟。"""

    def __init__(self, t=BASE):
        self.t = t
        self.lock = threading.Lock()

    def __call__(self):
        with self.lock:
            return self.t

    def now(self):
        with self.lock:
            return self.t

    def advance(self, seconds):
        with self.lock:
            self.t += timedelta(seconds=float(seconds))


class Case(unittest.TestCase):
    def setUp(self):
        self.dir = tempfile.mkdtemp(prefix="edgecast-test-")
        self.clk = FakeClock()
        self.eng = EdgeCastEngine(self._path(), time_fn=self.clk)

    def tearDown(self):
        self.eng.close()

    def _path(self, name="events.log"):
        return os.path.join(self.dir, name)

    def reopen(self, name="events.log"):
        """模拟崩溃重启：丢弃内存态（不做优雅关闭之外的动作），重放日志。"""
        self.eng.close()
        self.eng = EdgeCastEngine(self._path(name), time_fn=self.clk)
        return self.eng

    # ------------------------------------------------------------ helpers --
    def new_batch(self, devices=("D1", "D2"), cap=2, threshold=None,
                  timeout=10, retries=2, retry_delay=1.0, window=300,
                  package="MAP-A", version="1.0.0", groups=None,
                  window_start=None, window_end=None):
        body = {
            "package_id": package, "package_version": version,
            "groups": groups or [{"name": "north", "devices": list(devices),
                                   "cap": cap}],
            "window_sec": window, "dispatch_timeout_sec": timeout,
            "max_retries": retries, "retry_delay_sec": retry_delay,
            "ack_threshold": threshold if threshold is not None else len(
                groups[0]["devices"] if groups else devices)}
        if window_start is not None:
            body["window_start"] = window_start
        if window_end is not None:
            body["window_end"] = window_end
        r = self.eng.create_batch(body)
        return r["batch"]["batch_id"]

    def receipt(self, device, seq, status="success", *, attempt=1,
                package="MAP-A", version="1.0.0", detail="", checksum=None):
        p = {"device_id": device, "package_id": package,
             "package_version": version, "attempt": attempt, "seq": seq,
             "status": status, "detail": detail}
        p["checksum"] = checksum if checksum is not None else receipt_hash(p)
        return p

    def send(self, *a, **kw):
        return self.eng.ingest_receipt(self.receipt(*a, **kw))

    def group(self, bid, gname="north"):
        b = self.eng.batch_view(bid)
        return next(g for g in b["groups"] if g["name"] == gname)

    def inflight(self, bid, gname="north"):
        return self.group(bid, gname)["in_flight_now"]

    # ========================================================= 1. 正常投放 ==
    def test_01_normal_delivery_and_threshold(self):
        # 时间窗未到：排队不投放
        bid = self.new_batch(
            devices=("D1", "D2", "D3"), cap=2, threshold=2,
            window_start=(self.clk.now() + timedelta(seconds=60)).isoformat(),
            window_end=(self.clk.now() + timedelta(seconds=360)).isoformat())
        self.assertEqual(self.eng.batch_view(bid)["status"], "queued")
        self.assertEqual(self.inflight(bid), 0)

        # 时间窗到达：按分组并发上限投放 cap=2
        self.clk.advance(60)
        self.eng.tick()
        self.assertEqual(self.inflight(bid), 2)
        pending = self.group(bid)["pending"]
        self.assertEqual(pending, ["D3"])

        # D1 成功：cap=2 未满（D2 仍在飞），D3 不补位
        self.assertEqual(self.send("D1", 1, "success")["device_status"],
                         "succeeded")
        self.assertEqual(self.inflight(bid), 1)
        # D2 成功：调度点到来时槽位释放，D3 补位
        self.assertEqual(self.send("D2", 1, "success")["device_status"],
                         "succeeded")
        self.eng.tick()
        self.assertEqual(self.inflight(bid), 1)  # D3 补位
        # 一台失败 → 全部终结；成功 2 >= 阈值 2 → verdict success
        self.assertEqual(self.send("D3", 1, "failure")["device_status"],
                         "failed")
        b = self.eng.batch_view(bid)
        self.assertEqual(b["status"], "completed")
        self.assertEqual(b["verdict"], {
            "verdict": "success", "succeeded": 2, "failed": 1,
            "threshold": 2, "threshold_mode": "count", "total": 3})

        # 摘要可下载（文本与 JSON 一致）
        s = self.eng.summary(bid)
        self.assertEqual(s["totals"]["succeeded"], 2)
        text = self.eng.summary_text(bid)
        self.assertIn("BC-0001", text)
        self.assertIn("最终判定：达标成功", text)

    def test_01b_ratio_threshold_failure(self):
        body = {"package_id": "MAP-A", "package_version": "1.0.0",
                "groups": [{"name": "g", "devices": ["D1", "D2", "D3", "D4"],
                            "cap": 4}],
                "window_sec": 300, "dispatch_timeout_sec": 10,
                "max_retries": 0, "threshold_mode": "ratio", "ack_threshold": 0.8}
        bid = self.eng.create_batch(body)["batch"]["batch_id"]
        for d in ("D1", "D2", "D3"):
            self.send(d, 1, "success")
        self.send("D4", 1, "failure")
        v = self.eng.batch_view(bid)["verdict"]
        self.assertEqual(v["verdict"], "failed")  # 3/4 = .75 < .8

    # ========================================================= 2. 重复回执 ==
    def test_02_duplicate_returns_first_and_single_vote(self):
        bid = self.new_batch(devices=("D1",), cap=1, threshold=1)
        first = self.send("D1", 1, "success")
        self.assertTrue(first["terminal"])
        dup = self.send("D1", 1, "success")
        self.assertTrue(dup["duplicate"])
        self.assertTrue(dup["idempotent"])
        self.assertEqual(dup["first_result"]["status"], "success")
        b = self.eng.batch_view(bid)
        self.assertEqual(b["devices"]["D1"]["votes"], 1)
        self.assertEqual(b["counts"]["succeeded"], 1)

        # 跨重启重复仍返回首次结果，不新增事件/票数
        self.reopen()
        dup2 = self.send("D1", 1, "success")
        self.assertTrue(dup2["duplicate"])
        b2 = self.eng.batch_view(bid)
        self.assertEqual(b2["devices"]["D1"]["votes"], 1)

    def test_02b_progress_duplicates_each_seq_once(self):
        bid = self.new_batch(devices=("D1",), cap=1, threshold=1)
        self.send("D1", 1, "received")
        self.send("D1", 2, "installed")
        d1 = self.send("D1", 1, "received")
        d2 = self.send("D1", 2, "installed")
        self.assertTrue(d1["duplicate"] and d2["duplicate"])
        st = self.eng.batch_view(bid)["devices"]["D1"]["current_state"]
        self.assertEqual(st["processed_seqs"], [1, 2])
        self.send("D1", 3, "success")
        self.assertEqual(self.eng.batch_view(bid)["devices"]["D1"]["votes"], 1)

    # ========================================================= 3. 乱序回执 ==
    def test_03_out_of_order_buffered_then_drained(self):
        bid = self.new_batch(devices=("D1",), cap=1, threshold=1)
        # seq 2、3 先到：暂存，批次不完成
        r2 = self.send("D1", 2, "installed")
        self.assertTrue(r2["buffered"])
        r3 = self.send("D1", 3, "success")
        self.assertTrue(r3["buffered"])
        self.assertEqual(self.eng.batch_view(bid)["status"], "active")
        st = self.eng.batch_view(bid)["devices"]["D1"]["current_state"]
        self.assertEqual(st["buffered_seqs"], [2, 3])
        self.assertEqual(st["expected_seq"], 1)

        # seq1 到达：连续冲刷 1→2→3，终态成功，批次完成
        r1 = self.send("D1", 1, "received")
        drained = [(x["seq"], x["status"]) for x in r1["drained"]]
        self.assertEqual(drained, [(1, "received"), (2, "installed"),
                                   (3, "success")])
        self.assertEqual(self.eng.batch_view(bid)["status"], "completed")

        # 乱序暂存重复到达返回首次（暂存）结果
        # （重启后用 seq4 乱序再验）
        bid2 = self.new_batch(devices=("E1",), cap=1, threshold=1,
                              package="MAP-B")
        self.send("E1", 5, "installed", package="MAP-B")
        dup = self.send("E1", 5, "installed", package="MAP-B")
        self.assertTrue(dup["duplicate"] and dup["buffered"])

    def test_03b_buffered_survives_restart_and_drains(self):
        bid = self.new_batch(devices=("D1",), cap=1, threshold=1)
        self.send("D1", 2, "installed")
        self.reopen()  # 暂存内容随日志恢复
        b = self.eng.batch_view(bid)
        self.assertEqual(b["devices"]["D1"]["current_state"]["buffered_seqs"],
                         [2])
        self.assertEqual(b["status"], "active")
        r1 = self.send("D1", 1, "success")  # 缺口补齐直接冲刷并终态
        self.assertTrue(r1["terminal"])
        self.assertEqual(self.eng.batch_view(bid)["status"], "completed")

    # ========================================================= 4. 同号冲突 ==
    def test_04_same_seq_different_body_conflict(self):
        bid = self.new_batch(devices=("D1",), cap=1, threshold=1)
        self.send("D1", 1, "received", detail="checksum-A")
        conflict = self.send("D1", 1, "received", detail="checksum-B")
        self.assertFalse(conflict["accepted"])
        self.assertEqual(conflict["rejection"]["code"], "EDGE_ACK_CONFLICT")

        # 终态同号冲突也拒绝；设备仍不终结
        fin = self.send("D1", 2, "success")
        self.assertTrue(fin["terminal"])
        cf2 = self.send("D1", 2, "success", detail="changed")
        self.assertEqual(cf2["rejection"]["code"], "EDGE_ACK_CONFLICT")
        self.assertEqual(self.eng.batch_view(bid)["devices"]["D1"]["votes"], 1)

        # 冲突进入拒绝记录
        codes = [r["code"] for r in self.eng.list_rejects(50)]
        self.assertIn("EDGE_ACK_CONFLICT", codes)

    # ================================================= 5. 暂停/撤销竞争 ====
    def test_05_pause_freezes_pump_resume_continues(self):
        bid = self.new_batch(devices=("D1", "D2", "D3"), cap=1, threshold=3)
        self.assertEqual(self.inflight(bid), 1)   # D1
        self.eng.pause_batch(bid, "观察")
        self.assertEqual(self.eng.batch_view(bid)["status"], "paused")
        # D1 终票后，暂停期间不补投 D2
        self.send("D1", 1, "success")
        self.assertEqual(self.inflight(bid), 0)
        self.assertEqual(self.group(bid)["pending"], ["D2", "D3"])
        # 暂停幂等
        self.assertTrue(self.eng.pause_batch(bid)["idempotent"])
        # 恢复后继续推进（resume 自带一次调度）
        self.eng.resume_batch(bid)
        self.assertEqual(self.inflight(bid), 1)
        self.send("D2", 1, "success")
        self.eng.tick()                       # D3 补位
        self.send("D3", 1, "success")
        self.assertEqual(self.eng.batch_view(bid)["status"], "completed")

    def test_05b_revoke_vs_final_ack_race_single_terminal_state(self):
        # 并发压测：终态回执与撤销同时发生，每个设备只有一个确定终态
        for trial in range(30):
            bid = self.new_batch(
                devices=("D1",), cap=1, threshold=1,
                package=f"P{trial}", window=300)
            barrier = threading.Barrier(2)
            outcomes = []

            def ack():
                barrier.wait()
                outcomes.append(("ack", self.send("D1", 1, "success")))

            def revoke():
                barrier.wait()
                try:
                    outcomes.append(("revoke", self.eng.revoke_batch(bid, "x")))
                except Exception as e:  # 已完成再撤销 → 409（确定性结果之一）
                    outcomes.append(("revoke-err", e))

            t1, t2 = threading.Thread(target=ack), threading.Thread(target=revoke)
            t1.start(); t2.start(); t1.join(); t2.join()

            b = self.eng.batch_view(bid)
            dev = b["devices"]["D1"]
            # 设备终态唯一：成功 或 撤销，不允许两者叠加
            self.assertIn(dev["status"], ("succeeded", "canceled"))
            if dev["status"] == "succeeded":
                self.assertEqual(dev["votes"], 1)
                self.assertEqual(b["status"], "completed")
            else:
                self.assertEqual(b["status"], "revoked")
                self.assertEqual(b["counts"]["canceled"], 1)
            # 无论谁赢，再次投递都只得到一个确定结果，且票数不增长：
            #  - 终票先到：重复回执返回首次（duplicate=True）
            #  - 撤销先到：首次回执已被拒绝 REVOKED，再次提交同正文同样被拒，
            #    设备状态与批次状态都不改变（撤销是单确定终态）
            again = self.send("D1", 1, "success")
            if dev["status"] == "succeeded":
                self.assertTrue(again["duplicate"])
            else:
                # 撤销先到：落败回执被判定拒绝一次（REVOKED，或在批次已不可见时
                # 为 NO_OPEN_BATCH——两者都是确定的拒绝，不产生任何票）
                self.assertIn(again["rejection"]["code"],
                              ("EDGE_BATCH_REVOKED", "EDGE_NO_OPEN_BATCH"))
                self.assertFalse(again["accepted"])
            b2 = self.eng.batch_view(bid)
            self.assertEqual(b2["devices"]["D1"]["status"], dev["status"])
            self.assertEqual(b2["devices"]["D1"]["votes"], dev["votes"])

    def test_05c_revoke_cancels_queues_and_inflight(self):
        bid = self.new_batch(devices=("D1", "D2", "D3"), cap=1, threshold=3)
        self.eng.revoke_batch(bid, "紧急撤回")
        b = self.eng.batch_view(bid)
        self.assertEqual(b["status"], "revoked")
        self.assertEqual(b["counts"]["canceled"], 3)
        self.assertEqual(self.inflight(bid), 0)
        # 撤销幂等、恢复被拒
        self.assertTrue(self.eng.revoke_batch(bid)["idempotent"])
        # 撤销后已处理过的重复回执仍返回首次结果（此处设备未回执 → 拒绝留痕）
        r = self.send("D1", 1, "success")
        self.assertFalse(r["accepted"])
        self.assertEqual(r["rejection"]["code"], "EDGE_BATCH_REVOKED")

    # ========================================================= 6. 超时重试 ==
    def test_06_timeout_retry_respects_cap_and_succeeds(self):
        # 单设备批次隔离验证重试；随后用 cap=2 批次验证重试优先补位且不占第二名额
        bid = self.new_batch(
            devices=("D1",), cap=1, threshold=1,
            timeout=10, retries=1, retry_delay=1.0)
        self.clk.advance(10)
        self.eng.tick()
        dev = self.eng.batch_view(bid)["devices"]["D1"]
        self.assertEqual(dev["status"], "awaiting_retry")
        self.assertEqual(
            [(r["device"], r["attempt"]) for r in self.group(bid)["retry"]],
            [("D1", 1)])
        self.assertEqual(self.inflight(bid), 0)
        # 退避未到（t=10）：即使再 tick 也不重投
        self.eng.tick()
        self.assertEqual(self.inflight(bid), 0)
        self.clk.advance(1)                  # t=11：退避到点
        self.eng.tick()
        self.assertEqual(
            [(x["device_id"], x["attempt"]) for x in self.group(bid)["inflight"]],
            [("D1", 2)])

        # 第 2 代成功：恰好两次投放、一次重投、一张成功票
        r = self.send("D1", 1, "success", attempt=2)
        self.assertEqual(r["device_status"], "succeeded")
        b = self.eng.batch_view(bid)
        self.assertEqual(b["counts"]["dispatched"], 2)
        self.assertEqual(b["counts"]["retried"], 1)
        self.assertEqual(b["devices"]["D1"]["votes"], 1)

    def test_06a2_retry_priority_and_cap_with_peer(self):
        # cap=2：D1/D2 在飞，D3 排队；二者超时后在同一调度点按"待重试优先"
        # 立刻重投（退避=0），D3 仍排队；成功释放名额后 D3 补位，全程在飞 ≤ 2。
        bid = self.new_batch(
            devices=("D1", "D2", "D3"), cap=2, threshold=3,
            timeout=10, retries=1, retry_delay=0.0)
        self.assertEqual(self.inflight(bid), 2)
        self.clk.advance(10)
        self.eng.tick()  # 两台超时→待重试；同 tick 不补位（名额留给待重试）
        self.assertEqual(self.inflight(bid), 0)
        self.assertEqual(len(self.group(bid)["retry"]), 2)
        self.eng.tick()  # 下一调度点（退避=0 已到点）：D1、D2 优先重投
        self.assertEqual(self.inflight(bid), 2)
        inflight = sorted((x["device_id"], x["attempt"])
                          for x in self.group(bid)["inflight"])
        self.assertEqual(inflight, [("D1", 2), ("D2", 2)])
        self.assertEqual(self.group(bid)["pending"], ["D3"])
        self.send("D1", 1, "success", attempt=2)
        self.eng.tick()  # D3 补位
        inflight = sorted(x["device_id"] for x in self.group(bid)["inflight"])
        self.assertEqual(inflight, ["D2", "D3"])
        self.send("D2", 1, "success", attempt=2)
        self.eng.tick()
        self.send("D3", 1, "success")
        b = self.eng.batch_view(bid)
        self.assertEqual(b["status"], "completed")
        self.assertEqual(b["verdict"]["verdict"], "success")
        self.assertEqual(b["counts"]["retried"], 2)

    def test_06b_stale_attempt_ack_after_redispatch(self):
        bid = self.new_batch(devices=("D1",), cap=1, threshold=1,
                             timeout=10, retries=2, retry_delay=1.0)
        self.clk.advance(10); self.eng.tick()
        self.clk.advance(1); self.eng.tick()  # D1 第 2 代
        stale = self.send("D1", 1, "received", attempt=1)
        self.assertFalse(stale["accepted"])
        self.assertEqual(stale["rejection"]["code"], "EDGE_ACK_STALE_ATTEMPT")
        self.assertEqual(stale["first_result"]["next"], "retry")
        # 相同旧代次回执重复 → 首次判定，不再留痕
        stale2 = self.send("D1", 1, "received", attempt=1)
        self.assertTrue(stale2["idempotent"])
        before = len(self.eng.list_events(999))
        self.send("D1", 1, "received", attempt=1)
        self.assertEqual(len(self.eng.list_events(999)), before)
        # 同号不同正文即使在旧代次也冲突
        cf = self.send("D1", 1, "received", attempt=1, detail="x")
        self.assertEqual(cf["rejection"]["code"], "EDGE_ACK_CONFLICT")
        # 第 2 代成功
        self.assertTrue(self.send("D1", 1, "success", attempt=2)["terminal"])

    def test_06c_retry_exhausted_marks_failed(self):
        bid = self.new_batch(devices=("D1",), cap=1, threshold=1,
                             timeout=10, retries=1, retry_delay=1.0)
        self.clk.advance(10); self.eng.tick()   # a1 超时 → 重试
        self.clk.advance(1); self.eng.tick()    # a2 投出
        self.clk.advance(10); self.eng.tick()   # a2 超时 → 次数用尽
        b = self.eng.batch_view(bid)
        self.assertEqual(b["devices"]["D1"]["status"], "failed")
        self.assertEqual(b["devices"]["D1"]["failure_code"],
                         "EDGE_TIMEOUT_EXHAUSTED")
        self.assertEqual(b["verdict"]["verdict"], "failed")
        self.assertEqual(self.inflight(bid), 0)

    def test_06d_concurrency_cap_never_breached_under_mixed_load(self):
        # 三组不同 cap；推进过程中在飞数永不超过 cap
        bid = self.new_batch(groups=[
            {"name": "g1", "devices": [f"A{i}" for i in range(4)], "cap": 2},
            {"name": "g2", "devices": [f"B{i}" for i in range(3)], "cap": 1},
            {"name": "g3", "devices": [f"C{i}" for i in range(2)], "cap": 2}],
            threshold=9, timeout=5, retries=3, retry_delay=2.0)
        caps = {"g1": 2, "g2": 1, "g3": 2}
        # 交错：每 2 秒 tick，让部分设备超时重试，偶尔回执成功
        tick_log = []
        for sec in range(0, 30, 2):
            self.clk.advance(2)
            self.eng.tick()
            b = self.eng.batch_view(bid)
            for g in b["groups"]:
                self.assertLessEqual(g["in_flight_now"], caps[g["name"]])
                tick_log.append((g["name"], g["in_flight_now"]))
            # 给在飞设备一些成功回执，逐步收敛
            for g in self.eng.batch_view(bid)["groups"]:
                for inf in g["inflight"][:1]:  # 每组每轮收一个
                    if (sec + len(inf["device_id"])) % 3 == 0:
                        self.send(inf["device_id"], 1, "success",
                                  attempt=inf["attempt"])
        # 全部最终成功（兜底给剩余在飞设备回执）
        for g in self.eng.batch_view(bid)["groups"]:
            for inf in g["inflight"]:
                self.send(inf["device_id"], 1, "success",
                          attempt=inf["attempt"])
        # 若有未完成的（重试等待中），推进到收敛
        for _ in range(30):
            self.clk.advance(5)
            self.eng.tick()
            for g in self.eng.batch_view(bid)["groups"]:
                for inf in g["inflight"]:
                    self.send(inf["device_id"], 1, "success",
                              attempt=inf["attempt"])
            if self.eng.batch_view(bid)["status"] in ("completed", "revoked"):
                break
        # 允许失败（超时用尽），但所有设备必须终态且在飞数全程未破上限
        b = self.eng.batch_view(bid)
        for d in b["devices"].values():
            self.assertIn(d["status"], ("succeeded", "failed", "canceled"))

    # ========================================================= 7. 重启恢复 ==
    def test_07_restart_rebuilds_queues_counts_window(self):
        bid = self.new_batch(
            devices=("D1", "D2", "D3"), cap=1, threshold=3,
            timeout=10, retries=2, retry_delay=2.0)
        self.clk.advance(9)
        self.send("D1", 1, "success")           # D1 释放名额（D2 尚未补位）
        self.eng.tick()                          # 调度点：D2 投出（截止 t=19）
        self.assertEqual(
            [x["device_id"] for x in self.group(bid)["inflight"]], ["D2"])
        self.clk.advance(10); self.eng.tick()   # t=20：D2 超时 → 待重试（至 t=22）
        self.assertEqual(
            self.eng.batch_view(bid)["devices"]["D2"]["status"],
            "awaiting_retry")
        # 重启：待重试队列、计数、时间窗、attempt 全部重建
        self.reopen()
        b = self.eng.batch_view(bid)
        self.assertEqual(b["counts"]["succeeded"], 1)
        self.assertEqual(b["devices"]["D2"]["status"], "awaiting_retry")
        self.assertEqual(b["groups"][0]["retry"][0]["device"], "D2")
        # t=20 退避未到：D2 仍待重试；空出的名额不浪费，排队的 D3 先补位
        self.clk.advance(1); self.eng.tick()
        b = self.eng.batch_view(bid)
        self.assertEqual(b["devices"]["D2"]["status"], "awaiting_retry")
        self.assertEqual(
            [x["device_id"] for x in self.group(bid)["inflight"]], ["D3"])

        # 时间窗仍然有效；t=22 退避到点但 cap=1 被 D3 占用，D2 继续等待
        self.clk.advance(2); self.eng.tick()
        self.assertEqual(self.eng.batch_view(bid)["devices"]["D2"]["status"],
                         "awaiting_retry")
        # D3 成功释放名额 → 下一调度点 D2 以第 2 代重投（继续投放不重复计 D1 票）
        self.send("D3", 1, "success")
        self.eng.tick()
        self.assertEqual(
            [(x["device_id"], x["attempt"])
             for x in self.group(bid)["inflight"]], [("D2", 2)])
        self.send("D2", 1, "success", attempt=2)
        b = self.eng.batch_view(bid)
        self.assertEqual(b["status"], "completed")
        self.assertEqual(b["counts"]["succeeded"], 3)
        self.assertEqual(b["verdict"]["verdict"], "success")

    def test_07b_restart_window_expiry_converges(self):
        bid = self.new_batch(devices=("D1", "D2"), cap=1, threshold=2,
                             timeout=5, window=20, retries=0)
        self.reopen()  # 空重启也应一致
        self.clk.advance(21)
        self.eng.tick()
        b = self.eng.batch_view(bid)
        # 时间窗结束未投出的设备失败，批次收敛为已完成（未达标）
        self.assertEqual(b["status"], "completed")
        self.assertEqual(b["verdict"]["verdict"], "failed")
        for d in b["devices"].values():
            self.assertEqual(d["status"], "failed")

    # ========================================================= 8. 三类拒绝 ==
    def test_08_old_package_rejected(self):
        self.eng.register_package("MAP-X", "2.0.0")
        # 直接登记旧版本 → 拒绝
        with self.assertRaises(Exception) as cm:
            self.eng.register_package("MAP-X", "1.9.0")
        self.assertEqual(cm.exception.code, "EDGE_OLD_PACKAGE")
        # 批次要求 2.0.0，回执报 1.9.0 → 拒绝并记录
        bid = self.new_batch(devices=("D1",), cap=1, threshold=1,
                             package="MAP-X", version="2.0.0")
        r = self.send("D1", 1, "success", package="MAP-X", version="1.9.0")
        self.assertEqual(r["rejection"]["code"], "EDGE_OLD_PACKAGE")
        codes = [x["code"] for x in self.eng.list_rejects(50)]
        self.assertIn("EDGE_OLD_PACKAGE", codes)
        # 正确版本可正常成功
        self.assertTrue(self.send("D1", 1, "success",
                                  package="MAP-X", version="2.0.0")["terminal"])
        # 已成功的同一序号换旧版本重提：版本闸门优先，报旧包而非冲突/重复
        old_dup = self.send("D1", 1, "success",
                            package="MAP-X", version="1.9.0")
        self.assertEqual(old_dup["rejection"]["code"], "EDGE_OLD_PACKAGE")

    def test_08b_unknown_device_rejected(self):
        body = {"device_id": "GHOST", "package_id": "MAP-A",
                "package_version": "1.0.0", "attempt": 1, "seq": 1,
                "status": "success", "detail": ""}
        body["checksum"] = receipt_hash(body)
        r = self.eng.ingest_receipt(body)
        self.assertEqual(r["rejection"]["code"], "EDGE_UNKNOWN_DEVICE")

    def test_08c_checksum_failure_rejected(self):
        bid = self.new_batch(devices=("D1",), cap=1, threshold=1)
        good = self.receipt("D1", 1, "success")
        good["checksum"] = "sha256:deadbeef"
        r = self.eng.ingest_receipt(good)
        self.assertEqual(r["rejection"]["code"], "EDGE_ACK_CHECKSUM")
        # 被拒的不计数、不终结；正确摘要随后可用
        self.assertEqual(self.eng.batch_view(bid)["devices"]["D1"]["status"],
                         "inflight")
        self.assertTrue(self.send("D1", 1, "success")["terminal"])

    def test_08d_bad_fields(self):
        self.new_batch(devices=("D1",), cap=1, threshold=1)
        bad_status = self.receipt("D1", 1, "weird")
        r = self.eng.ingest_receipt(bad_status)
        self.assertEqual(r["rejection"]["code"], "BAD_REQUEST")
        bad_seq = self.receipt("D1", 0, "success")
        self.assertEqual(self.eng.ingest_receipt(bad_seq)["rejection"]["code"],
                         "BAD_REQUEST")

    # ===================================================== 时间线 / 摘要 ====
    def test_09_timeline_filter_by_group_and_device(self):
        bid = self.new_batch(groups=[
            {"name": "north", "devices": ["N1", "N2"], "cap": 2},
            {"name": "south", "devices": ["S1"], "cap": 1}], threshold=3)
        self.send("N1", 2, "installed")   # 乱序
        self.send("N1", 1, "success")     # 冲刷并成功
        self.send("S1", 1, "success")
        self.send("N2", 1, "success")
        tl = self.eng.timeline(bid, group="north", device="N1")
        types = [e["type"] for e in tl["events"]]
        self.assertIn("ack_buffered", types)
        self.assertIn("ack_recorded", types)
        self.assertTrue(all((e["data"].get("device_id") or e["data"].get("device"))
                            == "N1" for e in tl["events"]))
        south = self.eng.timeline(bid, group="south")
        self.assertTrue(all("S1" == (e["data"].get("device_id")
                                     or e["data"].get("device"))
                            for e in south["events"]))

    # ============================================ 同设备同包跨批次回执隔离 ==
    def test_11_old_batch_receipt_not_recounted_in_new_batch(self):
        """完成批次后立即为同设备同包开新批次：

        - 旧批次已处理终态回执的重发必须返回**旧批次**的首次判定，
          绝不落到新批次再次计票，也不改变新批次状态；
        - 新批次自己（当前投放代次）的回执仍正常受理；
        - 重启重放日志后结论保持。
        """
        b1 = self.new_batch(devices=("D1",), cap=1, threshold=1, retries=0)
        first = self.send("D1", 1, "success")
        self.assertTrue(first["terminal"])
        self.assertEqual(self.eng.batch_view(b1)["status"], "completed")

        # 立即创建同设备同包的新批次：首发代次跨批次延续为第 2 代
        b2 = self.new_batch(devices=("D1",), cap=1, threshold=1, retries=0)
        inflight = self.group(b2)["inflight"]
        self.assertEqual([(x["device_id"], x["attempt"]) for x in inflight],
                         [("D1", 2)])

        # 重发旧批次（attempt=1）的终态回执：回旧批次首次判定
        old = self.send("D1", 1, "success", attempt=1)
        self.assertTrue(old["duplicate"])
        self.assertTrue(old["idempotent"])
        self.assertEqual(old["batch_id"], b1)
        self.assertEqual(old["first_result"]["status"], "success")
        v1, v2 = self.eng.batch_view(b1), self.eng.batch_view(b2)
        self.assertEqual(v1["counts"]["succeeded"], 1)   # 旧票不增
        self.assertEqual(v2["counts"]["succeeded"], 0)   # 新批次不被旧票计入
        self.assertEqual(v2["devices"]["D1"]["votes"], 0)
        self.assertEqual(v2["devices"]["D1"]["status"], "inflight")
        self.assertEqual(self.inflight(b2), 1)

        # 再次重发稳定返回同一首次判定，不产生任何新事件
        before = len(self.eng.list_events(9999))
        old2 = self.send("D1", 1, "success", attempt=1)
        self.assertEqual(old2["batch_id"], b1)
        self.assertTrue(old2["duplicate"])
        self.assertEqual(len(self.eng.list_events(9999)), before)

        # 新批次自己当前代次（attempt=2）的终态回执正常受理并完成
        new = self.send("D1", 1, "success", attempt=2)
        self.assertTrue(new["terminal"])
        self.assertEqual(new["batch_id"], b2)
        self.assertEqual(new["device_status"], "succeeded")
        v2 = self.eng.batch_view(b2)
        self.assertEqual(v2["status"], "completed")
        self.assertEqual(v2["counts"]["succeeded"], 1)

        # 重启重放日志后：旧回执仍回旧批次首次判定，两批次票数各为 1
        self.reopen()
        old3 = self.send("D1", 1, "success", attempt=1)
        self.assertEqual(old3["batch_id"], b1)
        self.assertTrue(old3["duplicate"])
        self.assertEqual(self.eng.batch_view(b1)["counts"]["succeeded"], 1)
        self.assertEqual(self.eng.batch_view(b2)["counts"]["succeeded"], 1)
        # 新批次自己的回执重复也回新批次首次判定
        new2 = self.send("D1", 1, "success", attempt=2)
        self.assertEqual(new2["batch_id"], b2)
        self.assertTrue(new2["duplicate"])

    def test_11b_old_batch_replay_before_new_ack_isolates_state(self):
        """新批次在飞期间先收到旧批次重发，再收新批次回执：状态不串。"""
        b1 = self.new_batch(devices=("D1",), cap=1, threshold=1)
        self.send("D1", 1, "success")
        b2 = self.new_batch(devices=("D1",), cap=1, threshold=1)
        new_attempt = self.group(b2)["inflight"][0]["attempt"]
        self.assertEqual(new_attempt, 2)

        # 旧批次终态重发（失败票也一样隔离）：用另一台设备走 failure 验证
        b1f = self.new_batch(devices=("D2",), cap=1, threshold=1,
                             package="MAP-F")
        self.send("D2", 1, "failure", package="MAP-F")
        b2f = self.new_batch(devices=("D2",), cap=1, threshold=1,
                             package="MAP-F")
        a2f = self.group(b2f)["inflight"][0]["attempt"]
        old_f = self.send("D2", 1, "failure", attempt=1, package="MAP-F")
        self.assertEqual(old_f["batch_id"], b1f)
        self.assertTrue(old_f["duplicate"])
        self.assertEqual(self.eng.batch_view(b2f)["counts"]["failed"], 0)
        self.assertEqual(self.eng.batch_view(b2f)["devices"]["D2"]["status"],
                         "inflight")
        # 新批次失败回执正常计票
        new_f = self.send("D2", 1, "failure", attempt=a2f, package="MAP-F")
        self.assertEqual(new_f["batch_id"], b2f)
        self.assertEqual(self.eng.batch_view(b2f)["counts"]["failed"], 1)

    def test_11c_old_batch_replay_conflict_stays_on_old_batch(self):
        """旧批次同序号不同正文的重放：永久冲突，且不改变新批次。"""
        b1 = self.new_batch(devices=("D1",), cap=1, threshold=1)
        self.send("D1", 1, "success")
        b2 = self.new_batch(devices=("D1",), cap=1, threshold=1)
        new_attempt = self.group(b2)["inflight"][0]["attempt"]
        cf = self.send("D1", 1, "success", attempt=1, detail="篡改")
        self.assertEqual(cf["rejection"]["code"], "EDGE_ACK_CONFLICT")
        self.assertEqual(cf["batch_id"], b1)
        self.assertEqual(self.eng.batch_view(b2)["counts"]["succeeded"], 0)
        self.assertEqual(self.eng.batch_view(b2)["devices"]["D1"]["status"],
                         "inflight")
        # 新批次回执不受影响
        self.assertTrue(
            self.send("D1", 1, "success", attempt=new_attempt)["terminal"])

    def test_11d_fresh_attempt_starts_at_one_for_other_device_package(self):
        """跨批次延续只按 (设备, 包) 作用：其他设备/其他包首发仍是第 1 代。"""
        ba = self.new_batch(devices=("D1",), cap=1, threshold=1,
                            package="MAP-A")
        self.send("D1", 1, "success", package="MAP-A")
        self.assertEqual(self.eng.batch_view(ba)["status"], "completed")
        # 同设备不同包：仍是第 1 代
        bx = self.new_batch(devices=("D1",), cap=1, threshold=1,
                            package="MAP-B")
        self.assertEqual(self.group(bx)["inflight"][0]["attempt"], 1)
        self.send("D1", 1, "success", package="MAP-B")
        # 不同设备同包：仍是第 1 代
        by = self.new_batch(devices=("D2",), cap=1, threshold=1,
                            package="MAP-A")
        self.assertEqual(self.group(by)["inflight"][0]["attempt"], 1)
        self.send("D2", 1, "success", package="MAP-A")
        # D1 对 MAP-A 的第二次投放（新批次）代次延续为 2
        bz = self.new_batch(devices=("D1",), cap=1, threshold=1,
                            package="MAP-A")
        self.assertEqual(self.group(bz)["inflight"][0]["attempt"], 2)

    # ============================================================ 同源性 ====
    def test_10_cli_reads_same_log(self):
        bid = self.new_batch(devices=("D1", "D2"), cap=2, threshold=2)
        self.send("D1", 1, "success")
        self.send("D2", 1, "success")
        self.eng.close()
        env = dict(os.environ, EDGECAST_DATA=self.dir)
        out = subprocess.run(
            [sys.executable, "edgecast_cli.py", "batches"],
            cwd=os.path.dirname(os.path.abspath(__file__)),
            env=env, capture_output=True, text=True, timeout=30)
        self.assertEqual(out.returncode, 0, out.stderr)
        self.assertIn(bid, out.stdout)
        self.assertIn("已完成", out.stdout)
        # 摘要文件可写出
        out2 = subprocess.run(
            [sys.executable, "edgecast_cli.py", "summary", bid,
             os.path.join(self.dir, "sum.txt")],
            cwd=os.path.dirname(os.path.abspath(__file__)),
            env=env, capture_output=True, text=True, timeout=30)
        self.assertEqual(out2.returncode, 0, out2.stderr)
        with open(os.path.join(self.dir, "sum.txt"), encoding="utf-8") as sf:
            self.assertIn("达标成功", sf.read())


# ---------------------------------------------------------------- HTTP 冒烟 --
class HttpSmoke(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.dir = tempfile.mkdtemp(prefix="edgecast-http-")
        cls.port = 8900 + (os.getpid() % 200)
        env = dict(os.environ, PYTHONPATH=os.getcwd())
        cls.proc = subprocess.Popen(
            [sys.executable, "edgecast_server.py", str(cls.port),
             "--data", cls.dir, "--no-scheduler"],
            stdout=subprocess.PIPE, stderr=subprocess.STDOUT, env=env)
        for _ in range(50):
            try:
                urllib.request.urlopen(
                    f"http://127.0.0.1:{cls.port}/api/edgecast/snapshot",
                    timeout=1)
                break
            except Exception:
                time.sleep(0.1)
        else:
            raise RuntimeError("server failed to start")

    @classmethod
    def tearDownClass(cls):
        cls.proc.terminate()
        try:
            cls.proc.wait(timeout=5)
        except subprocess.TimeoutExpired:
            cls.proc.kill()
            cls.proc.wait(timeout=5)
        if cls.proc.stdout:
            cls.proc.stdout.close()

    def call(self, method, path, body=None):
        data = json.dumps(body).encode() if body is not None else None
        req = urllib.request.Request(
            f"http://127.0.0.1:{self.port}/api/edgecast{path}", data=data,
            method=method, headers={"Content-Type": "application/json"})
        try:
            with urllib.request.urlopen(req, timeout=5) as r:
                return r.status, json.loads(r.read())
        except urllib.error.HTTPError as e:
            return e.code, json.loads(e.read())

    def test_http_full_flow_and_summary_download(self):
        st, created = self.call("POST", "/batches", {
            "package_id": "WEB-A", "package_version": "3.1.0",
            "groups": [{"name": "g", "devices": ["W1", "W2"], "cap": 2}],
            "window_sec": 300, "dispatch_timeout_sec": 30,
            "max_retries": 0, "ack_threshold": 2, "note": "http 冒烟"})
        self.assertEqual(st, 201)
        bid = created["batch"]["batch_id"]
        # 服务端在创建时立即 tick（无调度线程也会在创建/恢复时推进）
        p = {"device_id": "W1", "package_id": "WEB-A",
             "package_version": "3.1.0", "attempt": 1, "seq": 1,
             "status": "success", "detail": ""}
        # checksum 需用引擎算法；直接用 receipt_hash
        p["checksum"] = receipt_hash(p)
        st2, r1 = self.call("POST", "/receipts", p)
        self.assertEqual(st2, 200); self.assertTrue(r1["terminal"])
        p2 = dict(p, device_id="W2"); p2["checksum"] = receipt_hash(p2)
        self.call("POST", "/receipts", p2)
        _, b = self.call("GET", f"/batches/{bid}")
        self.assertEqual(b["status"], "completed")
        # 摘要下载
        with urllib.request.urlopen(
                f"http://127.0.0.1:{self.port}/api/edgecast/batches/{bid}"
                f"/summary?format=text", timeout=5) as resp:
            text = resp.read().decode()
            self.assertIn("attachment", resp.headers["Content-Disposition"])
            self.assertIn(bid, text)
        # 未知设备拒绝
        bad = {"device_id": "ZZZ", "package_id": "WEB-A",
               "package_version": "3.1.0", "attempt": 1, "seq": 1,
               "status": "success", "detail": ""}
        bad["checksum"] = receipt_hash(bad)
        st3, rb = self.call("POST", "/receipts", bad)
        self.assertEqual(rb["rejection"]["code"], "EDGE_UNKNOWN_DEVICE")
        # 时间线端点
        st4, tl = self.call("GET", f"/batches/{bid}/timeline")
        self.assertEqual(st4, 200)
        self.assertTrue(tl["events"])
        # UI 可访问
        with urllib.request.urlopen(
                f"http://127.0.0.1:{self.port}/edgecast/", timeout=5) as resp:
            self.assertIn("EdgeCast", resp.read().decode())


if __name__ == "__main__":
    unittest.main(verbosity=2)
