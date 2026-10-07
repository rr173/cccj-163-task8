"""
EdgeCast — 边缘地图包分批投放编排器（事件溯源 / append-only 日志）。

操作员按 *设备分组 · 发布时间窗 · 分组并发上限 · 回执阈值* 创建投放批次，调度器逐组
推进：排队 → 投放 →（暂停/恢复）→ 完成 / 撤销；设备回执从**独立入口**流入。

核心不变量
----------
1. 回执按 (设备, 尝试代次 attempt, 序号 seq) **只处理一次**：
   - 重复回执（正文摘要相同）永远返回首次结果（idempotent）；
   - 同号不同正文 → 永久拒绝 EDGE_ACK_CONFLICT；
   - 乱序回执先暂存（ack_buffered），所缺序号补齐后按序冲刷。
2. 暂停 / 撤销 / 回执到达 / 超时同刻竞争，全部经单把锁串行化为**追加事件**：
   先落盘后改内存，每个设备尝试只有一个确定终态（成功 / 失败 / 撤销 / 超时重试）。
3. 超时设备进入该组待重试队列，由调度器在分组并发上限有空位时重投；
   任何时刻组内在飞数 ≤ 分组并发上限。重投递增 attempt，迟到的旧代次回执不能再计票。
4. 崩溃恢复 = 重放 append-only JSONL 日志：队列、计数、时间窗、暂存回执、已投票状态
   全部由事件折叠重建；继续投放不会重复计票（计票只发生在 ack_recorded 折叠中）。
5. 旧版本包（EDGE_OLD_PACKAGE）、未知设备（EDGE_UNKNOWN_DEVICE）、
   摘要校验失败（EDGE_ACK_CHECKSUM）一律拒绝并记录原因（ack_rejected 也进日志）。

与其他子系统一样，本模块零第三方依赖，且自成一域：状态只写
``data/edgecast/events.log``，不触碰地图 / 镜像 / 预热存储。
"""

from __future__ import annotations

import fcntl
import copy
import hashlib
import json
import os
import queue
import threading
from datetime import datetime, timedelta, timezone
from typing import Any, Callable, Optional

from engine import DomainError, canonical

RECEIPT_STATUSES = ("received", "installed", "success", "failure")
RECEIPT_FINAL = ("success", "failure")
RECEIPT_KEYS = ("device_id", "package_id", "package_version",
                "attempt", "seq", "status", "detail")


def utcnow_dt() -> datetime:
    return datetime.now(timezone.utc).replace(microsecond=0)


def iso(dt: datetime) -> str:
    return dt.isoformat()


def parse_iso(s: str) -> datetime:
    dt = datetime.fromisoformat(s)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt


def version_tuple(v: str) -> tuple:
    """'1.10.2' -> (1,10,2)；非数字段退化为 (0, 原始串)，仍保持全序。"""
    parts = []
    for p in str(v).split("."):
        try:
            parts.append((1, int(p)))
        except ValueError:
            parts.append((0, p))
    return tuple(parts)


def receipt_hash(payload: dict) -> str:
    """回执正文摘要（不含 checksum 字段本身）。"""
    body = {k: payload.get(k) if k != "detail" else payload.get(k, "")
            for k in RECEIPT_KEYS}
    return "sha256:" + hashlib.sha256(canonical(body)).hexdigest()


# ----------------------------------------------------------------- 事件日志 --

class EventLog:
    """每行一个 JSON 事件 envelope：{id, ts, type, data}；append 即 fsync。"""

    def __init__(self, path: str):
        self.path = path
        self.closed_flag = False
        os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
        # 单实例锁：避免两个调度器同时推进同一份日志（进程退出自动释放）
        self._lockfh = open(path + ".lock", "a+")
        try:
            fcntl.flock(self._lockfh.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as e:
            raise RuntimeError(
                f"投放日志 {path} 已被另一个编排器实例锁定") from e

    def close(self) -> None:
        if self.closed_flag:
            return
        self.closed_flag = True
        try:
            fcntl.flock(self._lockfh.fileno(), fcntl.LOCK_UN)
        finally:
            self._lockfh.close()

    def read_all(self) -> list[dict]:
        if not os.path.exists(self.path):
            return []
        events = []
        with open(self.path, "r", encoding="utf-8") as f:
            for ln, line in enumerate(f, 1):
                line = line.strip()
                if not line:
                    continue
                try:
                    events.append(json.loads(line))
                except json.JSONDecodeError as e:
                    raise RuntimeError(f"投放日志第 {ln} 行损坏，拒绝启动: {e}") from e
        return events

    def append(self, event: dict) -> None:
        with open(self.path, "a", encoding="utf-8") as f:
            f.write(json.dumps(event, ensure_ascii=False) + "\n")
            f.flush()
            os.fsync(f.fileno())


# --------------------------------------------------------------------- 引擎 --

class EdgeCastEngine:
    def __init__(self, path: str = "data/edgecast/events.log",
                 time_fn: Optional[Callable[[], datetime]] = None):
        self.log = EventLog(path)
        self.time_fn = time_fn or utcnow_dt
        self.lock = threading.RLock()
        self.state: dict = {}
        self._subscribers: set[queue.Queue] = set()
        self._scheduler_stop: Optional[threading.Event] = None
        self._scheduler_thread: Optional[threading.Thread] = None
        self._replay()

    def close(self) -> None:
        self.stop_scheduler()
        self.log.close()

    # ============================================================ 基础设態 ==
    @staticmethod
    def _blank() -> dict:
        return {"seq": 0, "packages": {}, "devices": {}, "batches": {},
                "events": [], "rejects": []}

    def now(self) -> datetime:
        return self.time_fn()

    def now_iso(self) -> str:
        return iso(self.time_fn())

    def _replay(self):
        """从 append-only 日志折叠出完整内存态（崩溃恢复）。"""
        with self.lock:
            self.state = self._blank()
            for ev in self.log.read_all():
                self._apply(ev)

    # -- 订阅（网页 SSE 实时看板） --
    def subscribe(self) -> queue.Queue:
        q: queue.Queue = queue.Queue(maxsize=2000)
        with self.lock:
            self._subscribers.add(q)
        return q

    def unsubscribe(self, q: queue.Queue) -> None:
        with self.lock:
            self._subscribers.discard(q)

    def _publish(self, ev: dict) -> None:
        for q in list(self._subscribers):
            try:
                q.put_nowait(ev)
            except queue.Full:
                try:
                    q.get_nowait()
                except Exception:
                    pass
                try:
                    q.put_nowait(ev)
                except queue.Full:
                    pass

    def _append_apply(self, etype: str, data: dict) -> dict:
        """所有状态变更的唯一入口：先追加落盘，再折叠进内存。"""
        self.state["seq"] += 1
        ev = {"id": self.state["seq"], "ts": self.now_iso(),
              "type": etype, "data": data}
        self.log.append(ev)
        self._apply(ev)
        self._publish(ev)
        return ev

    # ================================================================ 折叠 ==
    def _apply(self, ev: dict) -> None:
        """纯折叠：只读事件内容（含 ts），不读时钟、不做新决策。"""
        etype, d, ts = ev["type"], ev["data"], ev["ts"]
        self.state["seq"] = max(self.state["seq"], ev["id"])
        # 事件流本身也是折叠态的一部分：重放与新增走同一路径，
        # 重启后 timeline / 事件看板从日志完整恢复。
        self.state["events"].append(ev)

        if etype == "package_registered":
            self.state["packages"][d["package_id"]] = {
                "package_id": d["package_id"], "version": d["version"],
                "note": d.get("note", ""), "registered_at": ts}
            return

        if etype == "device_registered":
            self.state["devices"].setdefault(d["device_id"], {
                "device_id": d["device_id"], "group_hint": d.get("group_hint"),
                "registered_at": ts})
            return

        if etype == "batch_created":
            self._apply_batch_created(d, ts)
            return

        b = self.state["batches"].get(d.get("batch_id")) if d.get("batch_id") else None

        if etype == "dispatch_started":
            g = b["groups"][d["group"]]
            src = d.get("source", "pending")
            if src == "retry":
                g["retry"] = [r for r in g["retry"] if r["device"] != d["device"]]
            else:
                g["pending"].remove(d["device"])
            g["inflight"][d["device"]] = {
                "attempt": d["attempt"], "dispatched_at": ts,
                "deadline": d["deadline"], "source": src}
            dev = b["devices"][d["device"]]
            dev["attempt"] = d["attempt"]
            # 每一代尝试拥有独立的序号空间（排队/暂存/已处理全部重新计数）
            dev["attempt_state"][str(d["attempt"])] = {
                "expected_seq": 1, "processed": {}, "buffered": {},
                "rejected": {}}
            dev["attempts_made"] = max(dev["attempts_made"], d["attempt"])
            dev["status"] = "inflight"
            dev.setdefault("history", []).append(
                {"ts": ts, "event": "dispatch_started", "attempt": d["attempt"],
                 "source": src, "deadline": d["deadline"]})
            b["counts"]["dispatched"] += 1
            if src == "retry":
                b["counts"]["retried"] += 1
            b["running"] = True
            return

        if etype == "ack_buffered":
            st = self._astate(b["devices"][d["device_id"]], d["attempt"])
            st["buffered"][str(d["seq"])] = {
                "hash": d["body_hash"], "at": ts,
                "payload": copy.deepcopy(d["payload"])}
            b["devices"][d["device_id"]]["history"].append(
                {"ts": ts, "event": "ack_buffered", "attempt": d["attempt"],
                 "seq": d["seq"]})
            return

        if etype == "ack_recorded":
            self._apply_ack_recorded(b, d, ts)
            return

        if etype == "dispatch_timeout":
            self._apply_timeout(b, d, ts)
            return

        if etype == "window_closed":
            for did in d["devices"]:
                gname = b["devices"][did]["group"]
                g = b["groups"][gname]
                if did in g["pending"]:
                    g["pending"].remove(did)
                g["retry"] = [r for r in g["retry"] if r["device"] != did]
                dev = b["devices"][did]
                dev["status"] = "failed"
                dev["failure_code"] = "EDGE_WINDOW_EXPIRED"
                dev["history"].append(
                    {"ts": ts, "event": "window_expired",
                     "message": "发布时间窗结束仍未获投放名额"})
                b["counts"]["failed"] += 1
            return

        if etype == "batch_paused":
            b["paused"] = True
            b["paused_at"] = ts
            return

        if etype == "batch_resumed":
            b["paused"] = False
            b["resumed_at"] = ts
            return

        if etype == "batch_revoked":
            self._apply_revoked(b, d, ts)
            return

        if etype == "batch_completed":
            b["closed"] = True
            b["completed_at"] = ts
            b["verdict"] = {"verdict": d["verdict"],
                            "succeeded": d["succeeded"], "failed": d["failed"],
                            "threshold": d["threshold"],
                            "threshold_mode": d["threshold_mode"],
                            "total": d["total"]}
            return

        if etype in ("ack_stale", "ack_decision"):
            # 设备某代次某序号的"拒绝判定"也只做一次（旧代次迟到 / 撤销竞争落败等）
            st = self._astate(b["devices"][d["device_id"]], d["attempt"])
            st.setdefault("rejected", {})[str(d["seq"])] = {
                "body_hash": d["body_hash"], "code": d["code"],
                "message": d["message"], "at": ts,
                "first_result": d.get("first_result")}
            self.state["rejects"].append(dict(d, ts=ts, kind=etype))
            b["devices"][d["device_id"]]["history"].append(
                {"ts": ts, "event": etype, "code": d["code"],
                 "attempt": d["attempt"], "seq": d["seq"]})
            return

        if etype in ("ack_rejected", "ack_conflict"):
            rec = dict(d, ts=ts, kind=etype)
            self.state["rejects"].append(rec)
            if b is not None:
                dev = b["devices"].get(d["device_id"])
                if dev is not None:
                    dev["history"].append(
                        {"ts": ts, "event": etype, "code": d["code"],
                         "message": d["message"], "seq": d.get("seq"),
                         "attempt": d.get("attempt")})
            return

    def _apply_batch_created(self, d: dict, ts: str) -> None:
        groups = {}
        for g in d["groups"]:
            groups[g["name"]] = {
                "name": g["name"], "cap": g["cap"],
                "devices": list(g["devices"]),
                "pending": list(g["devices"]), "retry": [], "inflight": {}}
        devices = {}
        for g in d["groups"]:
            for did in g["devices"]:
                devices[did] = {
                    "device_id": did, "group": g["name"], "status": "pending",
                    "attempt": 0, "attempts_made": 0,
                    "attempt_state": {},   # "attempt" -> 该代序号空间
                    "votes": 0,
                    "final": None, "failure_code": None, "history": []}
        b = {
            "batch_id": d["batch_id"], "note": d.get("note", ""),
            "package_id": d["package_id"], "package_version": d["version"],
            "groups": groups, "devices": devices,
            "window_start": d["window_start"], "window_end": d["window_end"],
            "timeout_sec": d["timeout_sec"], "max_retries": d["max_retries"],
            "retry_delay_sec": d["retry_delay_sec"],
            "threshold": d["threshold"], "threshold_mode": d["threshold_mode"],
            "created_at": ts, "running": False, "paused": False,
            "closed": False, "revoked": False, "revoked_at": None,
            "completed_at": None, "verdict": None,
            "counts": {"dispatched": 0, "retried": 0, "succeeded": 0,
                       "failed": 0, "canceled": 0}}
        self.state["batches"][d["batch_id"]] = b

    @staticmethod
    def _astate(dev: dict, attempt: int) -> dict:
        """取设备某代尝试的序号空间（processed/buffered/expected_seq）。"""
        return dev["attempt_state"][str(attempt)]

    def _apply_ack_recorded(self, b: dict, d: dict, ts: str) -> None:
        dev = b["devices"][d["device_id"]]
        st = self._astate(dev, d["attempt"])
        seq = d["seq"]
        rec = {"seq": seq, "status": d["status"], "body_hash": d["body_hash"],
               "attempt": d["attempt"], "at": ts, "detail": d.get("detail", "")}
        # 冲刷路径上可能是先前 buffered 的条目：processed 里有就不重复记
        if str(seq) not in st["processed"]:
            st["processed"][str(seq)] = rec
        st["buffered"].pop(str(seq), None)
        if seq == st["expected_seq"]:
            st["expected_seq"] += 1
        dev["history"].append(
            {"ts": ts, "event": "ack_recorded", "attempt": d["attempt"],
             "seq": seq, "status": d["status"], "final": d["final"]})
        if d["final"]:
            # 终票只可能在这里投出一次（日志中每代每个序号唯一）
            g = b["groups"][dev["group"]]
            g["inflight"].pop(d["device_id"], None)
            dev["final"] = {"status": d["status"], "at": ts,
                            "attempt": d["attempt"]}
            if d["status"] == "success":
                dev["status"] = "succeeded"
                b["counts"]["succeeded"] += 1
            else:
                dev["status"] = "failed"
                dev["failure_code"] = "EDGE_DEVICE_FAILURE"
                b["counts"]["failed"] += 1
            dev["votes"] += 1

    def _apply_timeout(self, b: dict, d: dict, ts: str) -> None:
        did = d["device_id"]
        dev = b["devices"][did]
        g = b["groups"][dev["group"]]
        g["inflight"].pop(did, None)
        dev["history"].append(
            {"ts": ts, "event": "dispatch_timeout", "attempt": d["attempt"],
             "next": d["next"]})
        if d["next"] == "retry":
            dev["status"] = "awaiting_retry"
            g["retry"].append({"device": did, "attempt": d["attempt"],
                               "next_attempt_at": d["next_attempt_at"]})
        else:
            dev["status"] = "failed"
            dev["failure_code"] = "EDGE_TIMEOUT_EXHAUSTED"
            b["counts"]["failed"] += 1

    def _apply_revoked(self, b: dict, d: dict, ts: str) -> None:
        b["revoked"] = True
        b["closed"] = True
        b["revoked_at"] = ts
        b["revoke_reason"] = d.get("reason", "")
        for g in b["groups"].values():
            for did in list(g["pending"]):
                dev = b["devices"][did]
                if dev["status"] in ("succeeded", "failed", "canceled"):
                    continue
                dev["status"] = "canceled"
                b["counts"]["canceled"] += 1
            g["pending"] = []
            for r in list(g["retry"]):
                dev = b["devices"][r["device"]]
                if dev["status"] not in ("succeeded", "failed", "canceled"):
                    dev["status"] = "canceled"
                    b["counts"]["canceled"] += 1
            g["retry"] = []
            for did, info in list(g["inflight"].items()):
                dev = b["devices"][did]
                # 撤销与终态回执竞争：若已在同一把锁内被终态回执终结，保留其结果
                if dev["status"] in ("succeeded", "failed", "canceled"):
                    continue
                dev["status"] = "canceled"
                b["counts"]["canceled"] += 1
                dev["history"].append(
                    {"ts": ts, "event": "canceled_by_revoke",
                     "attempt": info["attempt"]})
            g["inflight"] = {}

    # ======================================================== 注册 / 创建 =====
    def register_package(self, package_id: str, version: str,
                         note: str = "") -> dict:
        with self.lock:
            existing = self.state["packages"].get(package_id)
            if existing and existing["version"] == version:
                return {"idempotent": True, "package": existing}
            if existing and version_tuple(version) < version_tuple(existing["version"]):
                raise DomainError(
                    "EDGE_OLD_PACKAGE",
                    f"包 {package_id} 已登记更新版本 {existing['version']}，"
                    f"拒绝登记旧版本 {version}", 409)
            self._append_apply("package_registered",
                               {"package_id": package_id, "version": version,
                                "note": note})
            return {"idempotent": False,
                    "package": self.state["packages"][package_id]}

    def register_device(self, device_id: str, group_hint: Optional[str] = None) -> dict:
        with self.lock:
            existing = self.state["devices"].get(device_id)
            if existing:
                return {"idempotent": True, "device": existing}
            self._append_apply("device_registered",
                               {"device_id": device_id, "group_hint": group_hint})
            return {"idempotent": False,
                    "device": self.state["devices"][device_id]}

    def _open_batch_for(self, device_id: str, package_id: str) -> Optional[dict]:
        for b in self.state["batches"].values():
            if b["closed"] or b["package_id"] != package_id:
                continue
            if device_id in b["devices"]:
                return b
        return None

    def create_batch(self, body: dict) -> dict:
        package_id = str(body.get("package_id", "")).strip()
        version = str(body.get("package_version") or body.get("version", "")).strip()
        if not package_id or not version:
            raise DomainError("BAD_REQUEST", "package_id 与 package_version 必填", 400)

        # groups: [{name, devices, cap?}] 或 {name: [devices]}
        caps = body.get("concurrency_by_group") or {}
        default_cap = int(body.get("concurrency") or 1)
        groups_in, seen = [], set()
        raw_groups = body.get("groups")
        if isinstance(raw_groups, dict):
            raw_groups = [{"name": k, "devices": v} for k, v in raw_groups.items()]
        if not isinstance(raw_groups, list) or not raw_groups:
            raise DomainError("BAD_REQUEST", "groups 必须是非空分组数组", 400)
        for g in raw_groups:
            name = str(g.get("name", "")).strip()
            devs = g.get("devices") or []
            if not name or not isinstance(devs, list) or not devs:
                raise DomainError("BAD_REQUEST",
                                  f"分组 {name!r} 名称与设备列表均不可为空", 400)
            cap = int(g.get("cap") or caps.get(name) or default_cap)
            if cap < 1:
                raise DomainError("BAD_REQUEST", f"分组 {name} 并发上限必须 ≥ 1", 400)
            norm = []
            for did in devs:
                did = str(did).strip()
                if not did:
                    continue
                if did in seen:
                    raise DomainError("BAD_REQUEST",
                                      f"设备 {did} 在批次内重复出现在多个分组", 400)
                seen.add(did)
                norm.append(did)
            if not norm:
                raise DomainError("BAD_REQUEST", f"分组 {name} 无有效设备", 400)
            groups_in.append({"name": name, "cap": cap, "devices": norm})
        total = sum(len(g["devices"]) for g in groups_in)

        timeout_sec = float(30.0 if body.get("dispatch_timeout_sec") is None
                            else body["dispatch_timeout_sec"])
        max_retries = int(2 if body.get("max_retries") is None
                          else body["max_retries"])
        retry_delay = float(1.0 if body.get("retry_delay_sec") is None
                            else body["retry_delay_sec"])
        if timeout_sec <= 0 or max_retries < 0 or retry_delay < 0:
            raise DomainError("BAD_REQUEST", "超时/重试参数非法", 400)

        # 回执阈值：count（设备数）或 ratio（0~1）
        thr_mode = body.get("threshold_mode", "count")
        threshold = body.get("ack_threshold", body.get("threshold"))
        if threshold is None:
            threshold, thr_mode = total, "count"
        if thr_mode == "ratio":
            threshold = float(threshold)
            if not 0 < threshold <= 1:
                raise DomainError("BAD_REQUEST", "比例阈值必须在 (0,1]", 400)
        else:
            threshold = int(threshold)
            if not 0 < threshold <= total:
                raise DomainError("BAD_REQUEST",
                                  f"回执阈值必须在 1..{total} 之间（设备总数）", 400)

        now = self.now()
        window_start = self._parse_window(body.get("window_start"), now, 0.0)
        window_end = self._parse_window(
            body.get("window_end"), now, float(body.get("window_sec") or 300.0))
        if window_end <= window_start:
            raise DomainError("BAD_REQUEST", "发布时间窗结束必须晚于开始", 400)

        with self.lock:
            # 同一开放批次中的设备不得再进入同包的另一开放批次
            for g in groups_in:
                for did in g["devices"]:
                    clash = self._open_batch_for(did, package_id)
                    if clash:
                        raise DomainError(
                            "EDGE_DEVICE_BUSY",
                            f"设备 {did} 已在开放批次 {clash['batch_id']} 的 "
                            f"{package_id} 投放中，同一设备同包不可并发分批", 409)
            # 包登记（幂等：登记过同版本直接复用；更新版本覆盖）
            pkg = self.state["packages"].get(package_id)
            if pkg is None:
                self._append_apply("package_registered",
                                   {"package_id": package_id, "version": version,
                                    "note": body.get("note", "")})
            elif version_tuple(version) < version_tuple(pkg["version"]):
                raise DomainError(
                    "EDGE_OLD_PACKAGE",
                    f"包 {package_id} 已知版本 {pkg['version']}，"
                    f"不能按旧版本 {version} 创建批次", 409)
            elif version != pkg["version"]:
                self._append_apply("package_registered",
                                   {"package_id": package_id, "version": version,
                                    "note": body.get("note", "")})
            # 名单内设备即"操作员选定的已知设备"
            for g in groups_in:
                for did in g["devices"]:
                    if did not in self.state["devices"]:
                        self._append_apply(
                            "device_registered",
                            {"device_id": did, "group_hint": g["name"]})

            self.state.setdefault("seq", self.state["seq"])
            num = len(self.state["batches"]) + 1
            # 批次号取未占用最小号（撤销/完成不回收也无妨，简单递增）
            while f"BC-{num:04d}" in self.state["batches"]:
                num += 1
            bid = f"BC-{num:04d}"
            self._append_apply("batch_created", {
                "batch_id": bid, "package_id": package_id, "version": version,
                "groups": groups_in, "window_start": iso(window_start),
                "window_end": iso(window_end), "timeout_sec": timeout_sec,
                "max_retries": max_retries, "retry_delay_sec": retry_delay,
                "threshold": threshold, "threshold_mode": thr_mode,
                "note": body.get("note", "")})
            self._tick_locked()
            return {"idempotent": False, "batch": self.batch_view(bid)}

    @staticmethod
    def _parse_window(value, now: datetime, default_delta: float) -> datetime:
        if value is None:
            return now + timedelta(seconds=default_delta)
        if isinstance(value, (int, float)):
            return now + timedelta(seconds=float(value))
        s = str(value).strip()
        if s.startswith("+"):
            return now + timedelta(seconds=float(s[1:]))
        return parse_iso(s)

    # ===================================================== 暂停 / 恢复 / 撤销 ==
    def pause_batch(self, bid: str, reason: str = "") -> dict:
        with self.lock:
            b = self._require_open(bid)
            if b["paused"]:
                return {"idempotent": True, "batch": self.batch_view(bid)}
            self._append_apply("batch_paused",
                               {"batch_id": bid, "reason": reason})
            return {"idempotent": False, "batch": self.batch_view(bid)}

    def resume_batch(self, bid: str) -> dict:
        with self.lock:
            b = self._require_open(bid)
            if not b["paused"]:
                return {"idempotent": True, "batch": self.batch_view(bid)}
            self._append_apply("batch_resumed", {"batch_id": bid})
            self._tick_locked()
            return {"idempotent": False, "batch": self.batch_view(bid)}

    def revoke_batch(self, bid: str, reason: str = "") -> dict:
        """撤销：与回执/超时在同一锁内竞争，只有一个确定终态。"""
        with self.lock:
            b = self.state["batches"].get(bid)
            if not b:
                raise DomainError("NOT_FOUND", f"批次 {bid} 不存在", 404)
            if b["revoked"]:
                return {"idempotent": True, "batch": self.batch_view(bid)}
            if b["closed"]:
                raise DomainError("EDGE_BATCH_CLOSED",
                                  f"批次 {bid} 已完成，不可撤销", 409)
            self._append_apply("batch_revoked",
                               {"batch_id": bid, "reason": reason})
            return {"idempotent": False, "batch": self.batch_view(bid)}

    def _require_open(self, bid: str) -> dict:
        b = self.state["batches"].get(bid)
        if not b:
            raise DomainError("NOT_FOUND", f"批次 {bid} 不存在", 404)
        if b["revoked"]:
            raise DomainError("EDGE_BATCH_REVOKED", f"批次 {bid} 已撤销", 409)
        if b["closed"]:
            raise DomainError("EDGE_BATCH_CLOSED", f"批次 {bid} 已完成", 409)
        return b

    # ================================================================ 回执 ==
    def _dup_verdict(self, b: dict, payload: dict, body_hash: str):
        """已关闭批次上查找同序号的首次判定；无记录返回 None。"""
        device_id = payload["device_id"]
        attempt, seq = payload["attempt"], payload["seq"]
        dev = b["devices"].get(device_id)
        if dev is None:
            return None
        ast = dev["attempt_state"].get(str(attempt))
        if ast is None:
            return None
        prev = ast["processed"].get(str(seq))
        if prev is not None:
            if prev["body_hash"] != body_hash:
                return self._reject_receipt(
                    payload, "EDGE_ACK_CONFLICT",
                    f"设备 {device_id} 第 {attempt} 代序号 {seq} 已处理过不同正文，"
                    f"同号冲突，永久拒绝", batch_id=b["batch_id"])
            return {"accepted": True, "idempotent": True, "duplicate": True,
                    "batch_id": b["batch_id"], "device_id": device_id,
                    "attempt": attempt, "seq": seq,
                    "stale_attempt": attempt != dev["attempt"],
                    "first_result": self._first_result(ast, str(seq))}
        rej = ast.get("rejected", {}).get(str(seq))
        if rej is not None:
            if rej["body_hash"] != body_hash:
                return self._reject_receipt(
                    payload, "EDGE_ACK_CONFLICT",
                    f"设备 {device_id} 第 {attempt} 代序号 {seq} 曾以不同正文被拒"
                    f"（{rej['code']}），同号冲突，永久拒绝", batch_id=b["batch_id"])
            return {"accepted": False, "idempotent": True, "duplicate": True,
                    "rejection": {"code": rej["code"], "message": rej["message"]},
                    "batch_id": b["batch_id"], "device_id": device_id,
                    "attempt": attempt, "seq": seq,
                    "first_result": rej.get("first_result")}
        buf = ast["buffered"].get(str(seq))
        if buf is not None and buf["hash"] == body_hash:
            return {"accepted": True, "idempotent": True, "duplicate": True,
                    "buffered": True, "batch_id": b["batch_id"],
                    "device_id": device_id, "attempt": attempt, "seq": seq,
                    "first_result": {"buffered": True, "seq": seq}}
        return None

    def ingest_receipt(self, body: dict) -> dict:
        """设备回执独立入口。返回首次确定结果；重复调用返回同一结果。"""
        with self.lock:
            payload, code, message = self._validate_receipt(body)
            if code:
                return self._reject_receipt(payload, code, message)

            device_id = payload["device_id"]
            package_id = payload["package_id"]
            # 先找该设备针对该包的批次（含已完成/已撤销——重复回执仍需回首次结果）
            matches = [x for x in self.state["batches"].values()
                       if x["package_id"] == package_id and device_id in x["devices"]]
            target = self._open_batch_for(device_id, package_id) or \
                (matches[-1] if matches else None)
            if target is None:
                return self._reject_receipt(
                    payload, "EDGE_NO_OPEN_BATCH",
                    f"设备 {device_id} 没有针对包 {package_id} 的投放批次")
            b = target
            payload["batch_id"] = b["batch_id"]
            dev = b["devices"][device_id]
            attempt, seq = payload["attempt"], payload["seq"]
            body_hash = receipt_hash(payload)

            # 1) 版本闸门优先于一切同号判定：不同版本是不同的正文命名空间，
            #    旧版本包回执一律拒绝，绝不能因为序号相同而命中"重复/冲突"。
            if version_tuple(payload["package_version"]) < \
                    version_tuple(b["package_version"]):
                return self._reject_receipt(
                    payload, "EDGE_OLD_PACKAGE",
                    f"回执来自旧版本包 {payload['package_version']}，批次要求 "
                    f"{b['package_version']}，拒绝", batch_id=b["batch_id"])
            if payload["package_version"] != b["package_version"]:
                return self._reject_receipt(
                    payload, "EDGE_VERSION_MISMATCH",
                    f"回执版本 {payload['package_version']} 与批次版本 "
                    f"{b['package_version']} 不一致", batch_id=b["batch_id"])

            # 2) 已处理 / 已判定拒绝的回执，无论批次状态 / 代次新旧，重复即返回
            #    首次结果；同号不同正文永久冲突（必须早于批次状态闸门）。
            dup = self._dup_verdict(b, payload, body_hash)
            if dup is not None:
                return dup

            # 3) 非首次回执才受批次状态约束
            if b["revoked"] or b["closed"]:
                code = "EDGE_BATCH_REVOKED" if b["revoked"] else "EDGE_BATCH_CLOSED"
                return self._decision_reject(
                    b, payload, body_hash, code,
                    f"批次 {b['batch_id']} 已"
                    + ("撤销" if b["revoked"] else "完成") + "，回执不再受理")

            # 4) 尝试代次闸门：超时重投后迟到的旧代次新回执 → 不再计票，返回超时结果
            if attempt != dev["attempt"]:
                if dev["attempt"] == 0:
                    return self._reject_receipt(
                        payload, "EDGE_NOT_DISPATCHED",
                        f"设备 {device_id} 尚未开始投放（无生效尝试代次）",
                        batch_id=b["batch_id"])
                if attempt < dev["attempt"]:
                    first_to = self._first_timeout(dev)
                    data = {"batch_id": b["batch_id"], "device_id": device_id,
                            "package_id": payload["package_id"],
                            "package_version": payload["package_version"],
                            "attempt": attempt, "seq": seq,
                            "code": "EDGE_ACK_STALE_ATTEMPT",
                            "message": f"设备 {device_id} 第 {attempt} 代投放已超时"
                                       f"并进入第 {dev['attempt']} 代，"
                                       f"旧代次回执不再计票",
                            "body_hash": body_hash, "first_result": first_to}
                    self._append_apply("ack_stale", data)
                    return {"accepted": False, "idempotent": False,
                            "rejection": {"code": "EDGE_ACK_STALE_ATTEMPT",
                                          "message": data["message"]},
                            "batch_id": b["batch_id"], "device_id": device_id,
                            "attempt": attempt, "seq": seq,
                            "first_result": first_to}
                return self._reject_receipt(
                    payload, "EDGE_ACK_UNKNOWN_ATTEMPT",
                    f"设备 {device_id} 当前为第 {dev['attempt']} 代，"
                    f"收到第 {attempt} 代回执", batch_id=b["batch_id"])

            st = dev["attempt_state"][str(attempt)]  # 当前代次（已通过代次闸门）
            # 5) 已暂存的乱序回执重复到达：相同正文返回首次（暂存）结果，
            #    不同正文冲突。
            buf = st["buffered"].get(str(seq))
            if buf is not None:
                if buf["hash"] == body_hash:
                    return {"accepted": True, "idempotent": True,
                            "duplicate": True, "buffered": True,
                            "batch_id": b["batch_id"], "device_id": device_id,
                            "attempt": attempt, "seq": seq,
                            "first_result": {"buffered": True, "seq": seq}}
                return self._reject_receipt(
                    payload, "EDGE_ACK_CONFLICT",
                    f"设备 {device_id} 第 {attempt} 代序号 {seq} 已暂存过不同正文"
                    f"（原摘要 {buf['hash'][:18]}… / 本次 {body_hash[:18]}…），"
                    f"同号冲突，永久拒绝", batch_id=b["batch_id"])

            if seq < st["expected_seq"]:
                # 理论上 processed 已覆盖；防御性拒绝
                return self._reject_receipt(
                    payload, "EDGE_ACK_CONFLICT",
                    f"序号 {seq} 已越过设备当前期望序号 {st['expected_seq']}",
                    batch_id=b["batch_id"])

            if seq > st["expected_seq"]:
                # 乱序：先暂存完整正文（事件落盘，重启后仍可冲刷），返回首次结果
                self._append_apply("ack_buffered", {
                    "batch_id": b["batch_id"], "device_id": device_id,
                    "attempt": attempt, "seq": seq, "body_hash": body_hash,
                    "payload": copy.deepcopy(payload)})
                return {"accepted": True, "buffered": True, "idempotent": False,
                        "batch_id": b["batch_id"], "device_id": device_id,
                        "attempt": attempt, "seq": seq,
                        "expected_seq": st["expected_seq"],
                        "first_result": {"buffered": True, "seq": seq}}

            # seq == expected：记录并连续冲刷暂存区
            return self._record_and_drain(b, dev, st, payload, body_hash)

    def _validate_receipt(self, body: dict) -> tuple[dict, Optional[str], str]:
        """字段解析 → 摘要校验。失败返回 (payload, code, message)。"""
        payload = {
            "device_id": str(body.get("device_id", "")).strip(),
            "package_id": str(body.get("package_id", "")).strip(),
            "package_version": str(body.get("package_version", "")).strip(),
            "attempt": int(body.get("attempt") or 1),
            "seq": body.get("seq"),
            "status": str(body.get("status", "")).strip(),
            "detail": str(body.get("detail", "") or ""),
        }
        presented = body.get("checksum", "")
        if not payload["device_id"] or not payload["package_id"] \
                or not payload["package_version"]:
            return payload, "BAD_REQUEST", "回执缺少 device_id/package_id/package_version"
        if not isinstance(payload["seq"], int) or payload["seq"] < 1:
            return payload, "BAD_REQUEST", "回执序号 seq 必须是 ≥1 的整数"
        if payload["attempt"] < 1:
            return payload, "BAD_REQUEST", "尝试代次 attempt 必须 ≥1"
        if payload["status"] not in RECEIPT_STATUSES:
            return payload, "BAD_REQUEST", \
                f"回执状态必须是 {RECEIPT_STATUSES} 之一"
        if not presented:
            return payload, "EDGE_ACK_CHECKSUM", "回执缺少摘要 checksum"
        if receipt_hash(payload) != presented:
            return payload, "EDGE_ACK_CHECKSUM", \
                f"回执摘要校验失败：正文重算 {receipt_hash(payload)[:18]}…，" \
                f"上报 {str(presented)[:18]}…"
        if payload["device_id"] not in self.state["devices"]:
            return payload, "EDGE_UNKNOWN_DEVICE", \
                f"未知设备 {payload['device_id']}：不在任何批次名单/设备注册表中"
        return payload, None, ""

    def _reject_receipt(self, payload: dict, code: str, message: str,
                        batch_id: Optional[str] = None,
                        first_result: Any = None,
                        idempotent: bool = False) -> dict:
        rec = {"batch_id": batch_id, "device_id": payload.get("device_id"),
               "package_id": payload.get("package_id"),
               "package_version": payload.get("package_version"),
               "attempt": payload.get("attempt"), "seq": payload.get("seq"),
               "status": payload.get("status"), "code": code, "message": message}
        etype = "ack_conflict" if code == "EDGE_ACK_CONFLICT" else "ack_rejected"
        self._append_apply(etype, rec)
        out = {"accepted": False, "idempotent": idempotent,
               "rejection": {"code": code, "message": message},
               "batch_id": batch_id,
               "device_id": payload.get("device_id"),
               "attempt": payload.get("attempt"), "seq": payload.get("seq")}
        if first_result is not None:
            out["first_result"] = first_result
        return out

    def _decision_reject(self, b: dict, payload: dict, body_hash: str,
                         code: str, message: str) -> dict:
        """把一次关闭类拒绝（撤销/完成）记为该代次序号的终身判定。"""
        data = {"batch_id": b["batch_id"], "device_id": payload["device_id"],
                "package_id": payload["package_id"],
                "package_version": payload["package_version"],
                "attempt": payload["attempt"], "seq": payload["seq"],
                "code": code, "message": message, "body_hash": body_hash}
        self._append_apply("ack_decision", data)
        return {"accepted": False, "idempotent": False,
                "rejection": {"code": code, "message": message},
                "batch_id": b["batch_id"], "device_id": payload["device_id"],
                "attempt": payload["attempt"], "seq": payload["seq"]}

    def _record_and_drain(self, b: dict, dev: dict, st: dict, payload: dict,
                          body_hash: str) -> dict:
        """记录当前序号，并把暂存区中连续的后续回执按序冲刷。"""
        results = []
        current_payload, current_hash = payload, body_hash
        while True:
            seq = current_payload["seq"]
            is_final = current_payload["status"] in RECEIPT_FINAL
            ev = self._append_apply("ack_recorded", {
                "batch_id": b["batch_id"], "device_id": dev["device_id"],
                "attempt": current_payload["attempt"], "seq": seq,
                "status": current_payload["status"],
                "detail": current_payload.get("detail", ""),
                "body_hash": current_hash, "final": is_final})
            results.append({"seq": seq, "status": current_payload["status"],
                            "final": is_final, "at": ev["ts"]})
            if is_final:
                break
            nxt = str(st["expected_seq"])  # apply 已自增
            buf = st["buffered"].get(nxt)
            if buf is None:
                break
            # 补齐缺口：用先前暂存（且已落盘）的完整正文继续按序冲刷
            current_payload = buf["payload"]
            current_hash = buf["hash"]

        completed = results[-1]["final"]
        self._maybe_complete(b)
        # 槽位补位统一由调度点（_tick_locked：超时清扫→时间窗→pump）决定，
        # 保持"调度只发生在调度点"的确定时序；调度线程每 100ms 推进一次。
        return {"accepted": True, "idempotent": False, "duplicate": False,
                "batch_id": b["batch_id"], "device_id": dev["device_id"],
                "attempt": payload["attempt"], "drained": results,
                "terminal": results[-1]["final"],
                "device_status": dev["status"],
                "first_result": {"processed": True, "seq": payload["seq"],
                                 "status": payload["status"]}}

    def _first_result(self, st: dict, key: str) -> dict:
        rec = st["processed"].get(key)
        if rec:
            return {"processed": True, "seq": rec["seq"], "status": rec["status"],
                    "at": rec["at"]}
        return {"buffered": True, "seq": int(key)}

    def _first_timeout(self, dev: dict) -> dict:
        for h in reversed(dev["history"]):
            if h["event"] == "dispatch_timeout":
                return {"timeout": True, "attempt": h["attempt"],
                        "next": h["next"], "at": h["ts"]}
        return {"timeout": True}

    # ================================================================ 调度 ==
    def tick(self) -> dict:
        with self.lock:
            return self._tick_locked()

    def _tick_locked(self) -> dict:
        """一次调度推进：超时清扫 → 时间窗收尾 → 分组按上限补位。"""
        fired = {"timeouts": [], "dispatched": [], "window_closed": []}
        now = self.now()
        for b in list(self.state["batches"].values()):
            if b["closed"]:
                continue
            before = len(fired["timeouts"])
            self._sweep_timeouts(b, now, fired)
            if b["closed"]:
                continue
            self._sweep_window(b, now, fired)
            if b["closed"] or b["paused"]:
                continue
            # 本 tick 刚有设备超时入待重试队列时不在同 tick 补位：让待重试队列
            # 先稳定下来（退避到点后参与名额竞争），避免排队设备在同 tick 抢占
            # 本应保留给超时设备的名额。下一调度点恢复正常 pump。
            if len(fired["timeouts"]) > before:
                self._maybe_complete(b)   # 超时用尽/时间窗收尾后批次可能已完成
                continue
            self._pump(b, now, fired)
            self._maybe_complete(b)
        return fired

    def _sweep_timeouts(self, b: dict, now: datetime, fired: dict) -> None:
        # 阶段一：只判定本 tick 到期的所有在飞设备（折叠会把它们移出 inflight、
        # 放入 retry）。**不在此处补位**，补位统一在随后的 _pump 阶段进行，
        # 否则先超时者在同 tick 立即重投会占满名额，挤掉后超时者的重投。
        due = [(g, did, info) for g in b["groups"].values()
               for did, info in list(g["inflight"].items())
               if parse_iso(info["deadline"]) <= now]
        for g, did, info in due:
            # 已发起的投放尝试代次（首发 1）；允许的总尝试数 = max_retries + 1
            attempts_started = info["attempt"]
            next_ = ("retry" if attempts_started <= b["max_retries"]
                     else "failed")
            data = {"batch_id": b["batch_id"], "device_id": did,
                    "group": g["name"], "attempt": info["attempt"],
                    "next": next_}
            if next_ == "retry":
                data["next_attempt_at"] = iso(
                    now + timedelta(seconds=b["retry_delay_sec"]))
            self._append_apply("dispatch_timeout", data)
            fired["timeouts"].append(data)
        self._maybe_complete(b)

    def _sweep_window(self, b: dict, now: datetime, fired: dict) -> None:
        if now <= parse_iso(b["window_end"]):
            return
        # 时间窗结束：未投出的排队设备与待重试设备判定失败；在飞设备保留，
        # 其回执/超时会自然终结，批次因此可收敛。
        expired = []
        for g in b["groups"].values():
            expired.extend(g["pending"])
            expired.extend(r["device"] for r in g["retry"])
        if expired:
            self._append_apply("window_closed",
                               {"batch_id": b["batch_id"], "devices": expired})
            fired["window_closed"].append(
                {"batch_id": b["batch_id"], "devices": expired})

    def _pump(self, b: dict, now: datetime, fired: dict) -> None:
        if not (parse_iso(b["window_start"]) <= now):
            return  # 时间窗未到：排队展示，不投任何设备
        for g in b["groups"].values():
            while len(g["inflight"]) < g["cap"]:
                candidate, source, attempt = None, None, None
                # 名额次序：到点的待重试（先超时者优先）→ 排队设备；
                # 退避未到点的重试不阻塞排队设备，但绝不突破 cap。
                due = [r for r in g["retry"]
                       if parse_iso(r["next_attempt_at"]) <= now]
                if due:
                    r = due[0]
                    candidate, source, attempt = \
                        r["device"], "retry", r["attempt"] + 1
                elif g["pending"]:
                    candidate = g["pending"][0]
                    source, attempt = "pending", 1
                if candidate is None:
                    break
                deadline = now + timedelta(seconds=b["timeout_sec"])
                data = {"batch_id": b["batch_id"], "group": g["name"],
                        "device": candidate, "attempt": attempt, "source": source,
                        "deadline": iso(deadline)}
                self._append_apply("dispatch_started", data)
                fired["dispatched"].append(data)

    def _maybe_complete(self, b: dict) -> None:
        """所有设备到达终态且队列清空 → 一次性完成事件（与撤销竞争由锁保证）。"""
        if b["closed"] or b["revoked"]:
            return
        for g in b["groups"].values():
            if g["pending"] or g["retry"] or g["inflight"]:
                return
        c = b["counts"]
        total = len(b["devices"])
        succeeded, failed = c["succeeded"], c["failed"]
        if succeeded + failed != total:
            return
        if b["threshold_mode"] == "ratio":
            met = succeeded / total >= b["threshold"] - 1e-9
            thr_repr = b["threshold"]
        else:
            met = succeeded >= b["threshold"]
            thr_repr = b["threshold"]
        self._append_apply("batch_completed", {
            "batch_id": b["batch_id"], "verdict": "success" if met else "failed",
            "succeeded": succeeded, "failed": failed, "total": total,
            "threshold": thr_repr, "threshold_mode": b["threshold_mode"]})

    # ============================================================ 调度线程 ===
    def start_scheduler(self, interval: float = 0.1) -> None:
        if self._scheduler_thread:
            return
        ev = threading.Event()
        self._scheduler_stop = ev

        def loop():
            while not ev.wait(interval):
                try:
                    self.tick()
                except Exception:
                    import traceback
                    traceback.print_exc()

        t = threading.Thread(target=loop, name="edgecast-scheduler", daemon=True)
        self._scheduler_thread = t
        t.start()

    def stop_scheduler(self) -> None:
        if self._scheduler_stop:
            self._scheduler_stop.set()
        if self._scheduler_thread:
            self._scheduler_thread.join(timeout=2)
        self._scheduler_stop = None
        self._scheduler_thread = None

    # ================================================================= 视图 ==
    def _status(self, b: dict) -> str:
        if b["revoked"]:
            return "revoked"
        if b["closed"]:
            return "completed"
        if b["paused"]:
            return "paused"
        now = self.now()
        if not b["running"] and now < parse_iso(b["window_start"]):
            return "queued"
        return "active"

    def batch_view(self, bid: str) -> dict:
        with self.lock:
            b = self.state["batches"].get(bid)
            if not b:
                raise DomainError("NOT_FOUND", f"批次 {bid} 不存在", 404)
            return self._batch_view(b)

    def _batch_view(self, b: dict) -> dict:
        groups = []
        for g in b["groups"].values():
            inflight = []
            for did, info in g["inflight"].items():
                inflight.append({"device_id": did, "attempt": info["attempt"],
                                 "deadline": info["deadline"],
                                 "dispatched_at": info["dispatched_at"],
                                 "source": info["source"]})
            groups.append({
                "name": g["name"], "cap": g["cap"],
                "pending": list(g["pending"]),
                "retry": [dict(r) for r in g["retry"]],
                "inflight": inflight,
                "succeeded": [d for d in g["devices"]
                              if b["devices"][d]["status"] == "succeeded"],
                "failed": [{"device_id": d,
                            "code": b["devices"][d]["failure_code"]}
                           for d in g["devices"]
                           if b["devices"][d]["status"] == "failed"],
                "canceled": [d for d in g["devices"]
                             if b["devices"][d]["status"] == "canceled"],
                "in_flight_now": len(g["inflight"])})
        devices = {did: {
            "device_id": did, "group": d["group"], "status": d["status"],
            "attempt": d["attempt"], "attempts_made": d["attempts_made"],
            "current_state": ({
                "expected_seq":
                    self._astate(d, d["attempt"])["expected_seq"],
                "buffered_seqs":
                    sorted(int(k) for k in
                           self._astate(d, d["attempt"])["buffered"]),
                "processed_seqs":
                    sorted(int(k) for k in
                           self._astate(d, d["attempt"])["processed"])}
                if d["attempt"] else None),
            "attempts": sorted(
                ({"attempt": int(k),
                  "expected_seq": v["expected_seq"],
                  "buffered_seqs": sorted(int(x) for x in v["buffered"]),
                  "processed_seqs": sorted(int(x) for x in v["processed"])}
                 for k, v in d["attempt_state"].items()),
                key=lambda x: x["attempt"]),
            "votes": d["votes"], "final": d["final"],
            "failure_code": d["failure_code"]}
            for did, d in b["devices"].items()}
        return {
            "batch_id": b["batch_id"], "note": b["note"],
            "package_id": b["package_id"], "package_version": b["package_version"],
            "status": self._status(b), "paused": b["paused"],
            "closed": b["closed"], "revoked": b["revoked"],
            "window_start": b["window_start"], "window_end": b["window_end"],
            "timeout_sec": b["timeout_sec"], "max_retries": b["max_retries"],
            "retry_delay_sec": b["retry_delay_sec"],
            "threshold": b["threshold"], "threshold_mode": b["threshold_mode"],
            "created_at": b["created_at"], "completed_at": b["completed_at"],
            "revoked_at": b["revoked_at"], "verdict": b["verdict"],
            "counts": dict(b["counts"]), "groups": groups, "devices": devices,
            "now": self.now_iso()}

    def list_batches(self) -> list:
        with self.lock:
            out = []
            for b in self.state["batches"].values():
                v = self._batch_view(b)
                # 列表视图精简掉逐设备明细
                v.pop("devices", None)
                out.append(v)
            return sorted(out, key=lambda x: x["batch_id"], reverse=True)

    def snapshot(self) -> dict:
        with self.lock:
            return {"now": self.now_iso(),
                    "packages": sorted(self.state["packages"].values(),
                                       key=lambda p: p["package_id"]),
                    "devices": sorted(self.state["devices"].values(),
                                      key=lambda d: d["device_id"]),
                    "batches": self.list_batches(),
                    "recent_rejects": list(self.state["rejects"][-20:][::-1])}

    def list_rejects(self, limit: int = 100) -> list:
        with self.lock:
            return list(self.state["rejects"][-limit:][::-1])

    def list_events(self, limit: int = 200) -> list:
        with self.lock:
            return list(self.state["events"][-limit:][::-1])

    def timeline(self, bid: str, group: Optional[str] = None,
                 device: Optional[str] = None) -> dict:
        with self.lock:
            b = self.state["batches"].get(bid)
            if not b:
                raise DomainError("NOT_FOUND", f"批次 {bid} 不存在", 404)
            events = []
            for ev in self.state["events"]:
                d = ev["data"]
                if d.get("batch_id") != bid:
                    continue
                did = d.get("device_id") or d.get("device")
                if device and did != device:
                    continue
                if group:
                    gname = d.get("group")
                    if not gname and did:
                        gname = b["devices"].get(did, {}).get("group")
                    if gname != group:
                        continue
                events.append({"id": ev["id"], "ts": ev["ts"],
                               "type": ev["type"], "data": d})
            return {"batch_id": bid, "group": group, "device": device,
                    "events": events}

    # ================================================================ 摘要 ==
    def summary(self, bid: str) -> dict:
        v = self.batch_view(bid)
        total = len(v["devices"])
        return {"batch_id": bid, "generated_at": self.now_iso(),
                "package": {"id": v["package_id"], "version": v["package_version"]},
                "status": v["status"], "window": {"start": v["window_start"],
                                                   "end": v["window_end"]},
                "policy": {"threshold": v["threshold"],
                           "mode": v["threshold_mode"],
                           "timeout_sec": v["timeout_sec"],
                           "max_retries": v["max_retries"],
                           "retry_delay_sec": v["retry_delay_sec"]},
                "totals": {"devices": total, **v["counts"]},
                "verdict": v["verdict"],
                "groups": [{
                    "group": g["name"], "cap": g["cap"],
                    "in_flight_now": g["in_flight_now"],
                    "pending": len(g["pending"]), "retry": len(g["retry"]),
                    "succeeded": len(g["succeeded"]),
                    "failed": g["failed"], "canceled": len(g["canceled"])}
                    for g in v["groups"]],
                "devices": sorted(v["devices"].values(),
                                  key=lambda d: (d["group"], d["device_id"]))}

    def summary_text(self, bid: str) -> str:
        s = self.summary(bid)
        cn = {"queued": "排队中", "active": "投放中", "paused": "已暂停",
              "completed": "已完成", "revoked": "已撤销"}
        lines = [
            f"边缘地图包投放批次摘要 {s['batch_id']}",
            f"生成时间：{s['generated_at']}",
            f"包：{s['package']['id']} @ {s['package']['version']}",
            f"状态：{cn.get(s['status'], s['status'])}    时间窗："
            f"{s['window']['start']} ~ {s['window']['end']}",
            f"阈值：{s['policy']['threshold']}"
            + ("（比例）" if s['policy']['mode'] == 'ratio' else "（成功设备数）")
            + f"    单设备超时 {s['policy']['timeout_sec']:g}s，"
            f"最多重试 {s['policy']['max_retries']} 次",
            "-" * 64,
        ]
        t = s["totals"]
        lines.append(
            f"设备总数 {t['devices']}：成功 {t.get('succeeded', 0)}，"
            f"失败 {t.get('failed', 0)}，撤销 {t.get('canceled', 0)}，"
            f"投放次数 {t.get('dispatched', 0)}（其中重投 "
            f"{t.get('retried', 0)}）")
        if s["verdict"]:
            vd = s["verdict"]
            lines.append(
                f"最终判定：{'达标成功' if vd['verdict'] == 'success' else '未达标失败'}"
                f"（成功 {vd['succeeded']}/{vd['total']}，阈值 {vd['threshold']}"
                + ("（比例）" if vd['threshold_mode'] == 'ratio' else "") + "）")
        lines.append("-" * 64)
        for g in s["groups"]:
            lines.append(
                f"[{g['group']}] 并发上限 {g['cap']}，当前在飞 "
                f"{g['in_flight_now']}：排队 {g['pending']}，待重试 "
                f"{g['retry']}，成功 {g['succeeded']}，失败 "
                f"{len(g['failed'])}，撤销 {g['canceled']}")
            for f in g["failed"]:
                lines.append(f"    - 失败设备 {f['device_id']}：{f['code']}")
        lines.append("-" * 64)
        for d in s["devices"]:
            st = {"pending": "排队", "inflight": "投放中",
                  "awaiting_retry": "待重试", "succeeded": "成功",
                  "failed": "失败", "canceled": "已撤销"}.get(d["status"], d["status"])
            extra = f"，最终 {d['final']['status']} @ 第 {d['final']['attempt']} 代" \
                if d["final"] else ""
            if d["failure_code"]:
                extra += f"，原因 {d['failure_code']}"
            lines.append(f"  {d['device_id']:<12} [{d['group']}] {st}{extra}")
        return "\n".join(lines) + "\n"
