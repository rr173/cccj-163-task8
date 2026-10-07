"""
Capacity-aware preheat orchestration for roadside boxes (STRICTLY READ-ONLY
over the live map service and the mirror repository).

A dispatcher picks an **immutable mirror manifest** and a batch of target
boxes, sets a deadline, a per-box link rate cap and a minimum online
guarantee; the scheduler then arranges transfers according to:

  * **site shared bandwidth** — boxes are grouped into sites; the sum of
    granted rates on one site never exceeds the site capacity;
  * **box free disk** — a transfer only starts when the box can hold the
    remaining bytes; the disk reservation and the transfer slot are always
    acquired and released **as a pair**;
  * **content reuse** — objects already on the box (current mirror image or
    previously completed preheats) are never retransferred; a fully reused
    task completes instantly;
  * **fairness** — eligible-but-unserved tasks accrue debt (欠账) every tick;
    the scheduler serves the largest debt first, so a continuously eligible
    job can never starve;
  * **pause/resume** — pausing releases the (disk, slot) pair; resuming only
    sends the remaining bytes (bytes_done is never rewound).

No ghost occupancy: occupancy is *derived* from task states inside a single
atomic commit, so space shortage, link degradation, box offline, cancel or a
service crash can never leak a reservation. On startup the engine rebuilds
the queue, the debts and the remaining times from the persisted store: any
task caught mid-transfer is returned to the queue and its (disk, slot) pair
is released.

Box-end acceptance & quarantine repair (盒端验收与隔离修复)
=========================================================
A finished transfer is merely *delivered*, never trusted. Each box then runs
an item-by-item acceptance over the manifest objects:

  * object LENGTH (declared vs. authoritative bytes on the box),
  * per-object content hash,
  * compressed-CHUNK hash (块散列),
  * and finally the whole-package ROOT hash (根散列).

Every run appends an **immutable acceptance proof** carrying the manifest
generations, the job id and a per-box acceptance sequence; the proof chains
to its predecessor and can never be rewritten. Good objects enter the box's
proven-object map (the only content counted for later reuse and exposed in
the **switchable set** — 可切换集合); bad/missing objects alone are
quarantined, their already-counted space is released, and exactly ONE repair
(keyed by (task, quarantine generation)) is created over the remaining
chunks: proven objects are never retransferred. Concurrent submissions of
the same repair key return the first task; an older-generation repair that
finishes late can never overwrite newer-generation proofs. Offline boxes,
service restarts and repair cancellation all resume or converge without
leaking capacity.

Quota changes, cancel and resubmission are deterministic: resubmitting an
identical job replays the first job (unless it was canceled — then a fresh
job is created); cancel is verdict-once. The web console and preheat_cli.py
read the same store and the same pure scheduling functions, so queue order,
rate-limit reasons, ETAs and the acceptance board (pending / proven /
quarantined / repairing) are identical.

This engine writes exclusively to data/preheat.json. It never moves region
generations, never touches patches/snapshots, and never mutates manifests,
the effective map or trip snapshots.
"""

from __future__ import annotations

import base64
import copy
import hashlib
import json
import os
import threading
import zlib
from datetime import datetime, timedelta, timezone
from typing import Any, Optional

from engine import DomainError, canonical, sha256, short, utcnow_iso

KB = 1000.0                      # 1 KB = 1000 bytes (rates are KB/s)
EPS = 1e-6
TASK_OPEN = ("queued", "transferring")
JOB_OPEN = ("active", "paused")

# transfer-task acceptance lifecycle:
#   queued/transferring → transferred (delivered, awaiting acceptance)
#   → proven (all objects verified; only then "done" for the scheduler)
#     or quarantined (some objects isolated; a repair is running)
ACCEPT_PENDING = "transferred"
ACCEPT_PROVEN = "proven"
ACCEPT_QUARANTINED = "quarantined"
ACCEPT_STAGES = (ACCEPT_PENDING, ACCEPT_QUARANTINED, ACCEPT_PROVEN)
# acceptance failure reason codes (length / object hash / chunk hash / root)
ACCEPT_REASONS = {
    "ACCEPT_OK": "长度、对象散列、块散列与根散列全部一致",
    "ACCEPT_OBJECT_LENGTH": "对象长度与清单声明不符",
    "ACCEPT_OBJECT_HASH": "对象内容散列与清单声明不符",
    "ACCEPT_CHUNK_MISSING": "块缺失（传输不完整）",
    "ACCEPT_CHUNK_HASH": "压缩块散列与清单声明不符（块损坏）",
    "ACCEPT_ROOT_HASH": "已验块的整包根散列与清单根散列不符",
}


def sha256_bytes(b: bytes) -> str:
    return "sha256:" + hashlib.sha256(b).hexdigest()


def _b64e(b: bytes) -> str:
    return base64.b64encode(b).decode("ascii")


def _b64d(x: str) -> bytes:
    return base64.b64decode(x.encode("ascii"))

# limit / wait reason codes (same strings in web + CLI: same store, same code)
REASONS = {
    "BOX_OFFLINE": "盒子离线，传输名额与磁盘预留不会分配",
    "BOX_DISK_FULL": "盒内可用空间不足，等待释放或扩容",
    "LINK_DOWN": "链路速率上限为 0（链路降级），暂停分配",
    "SITE_SATURATED": "站点共享带宽被更高优先级（欠账更多）的作业占满，欠账累计中",
    "SITE_SHARED": "受站点共享带宽限制",
    "BOX_LINK": "受盒链路速率上限限制（链路降级）",
    "JOB_CAP": "受作业速率上限限制",
    "PAUSED": "作业已暂停，名额与预留已成对释放",
    "READY": "已获得传输名额与磁盘预留",
}


def _iso(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).replace(microsecond=0).isoformat()


def _parse(ts: str) -> datetime:
    return datetime.fromisoformat(ts)


def _round6(x: float) -> float:
    return round(x, 6)


class PreheatEngine:
    def __init__(self, path: str, mirror=None):
        self.path = path
        self.mirror = mirror            # MirrorEngine, used READ-ONLY
        self.lock = threading.RLock()
        self.state: dict = {}
        self._load()

    # ========================================================= persistence ==
    def _load(self):
        if os.path.exists(self.path):
            with open(self.path, "r", encoding="utf-8") as f:
                self.state = json.load(f)
            for k, v in self._blank().items():
                self.state.setdefault(k, v)
        else:
            self.state = self._blank()
        changed = self._migrate_locked()
        changed = self._recover_locked() or changed
        before = json.dumps(self.state, sort_keys=True, ensure_ascii=False)
        self._settle(self.state)  # rebuild the schedule after any restart
        if changed or not os.path.exists(self.path) or \
                json.dumps(self.state, sort_keys=True, ensure_ascii=False) != before:
            self._persist_locked()

    @staticmethod
    def _blank() -> dict:
        return {"clock": utcnow_iso(),   # simulated clock, advanced by tick()
                "sites": {},             # site_id -> shared-bandwidth site
                "boxes": {},             # box_id -> roadside box record
                "jobs": {},              # job_id -> preheat job
                "job_by_key": {},        # content key -> job_id (deterministic resubmit)
                "tasks": {},             # task_id -> transfer task OR repair task
                "acceptances": {},       # run_id  -> immutable acceptance run
                "acceptance_by_box": {}, # box_id  -> [run_id, ...] (accept seq order)
                "proofs": {},            # proof_id -> immutable per-object proof
                "repair_by_key": {},     # "PT|gen|chunks" -> repair task id
                "events": [],            # append-only orchestration journal
                "seq": {"job": 0, "task": 0, "repair": 0, "run": 0,
                        "proof": 0, "event": 0}}

    def _persist_locked(self):
        os.makedirs(os.path.dirname(self.path) or ".", exist_ok=True)
        tmp = f"{self.path}.tmp.{os.getpid()}.{threading.get_ident()}"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(self.state, f, ensure_ascii=False, indent=2)
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp, self.path)

    def commit(self, mutate) -> Any:
        """Run mutate(copy), re-settle allocation, atomically persist.

        Every committed state satisfies: per-site granted rate ≤ capacity and
        per-box reserved disk ≤ free space — concurrency can never break the
        caps because allocation is recomputed inside the same atomic commit.
        """
        with self.lock:
            draft = copy.deepcopy(self.state)
            result = mutate(draft)
            self._settle(draft)
            os.makedirs(os.path.dirname(self.path) or ".", exist_ok=True)
            tmp = f"{self.path}.tmp.{os.getpid()}.{threading.get_ident()}"
            with open(tmp, "w", encoding="utf-8") as f:
                json.dump(draft, f, ensure_ascii=False, indent=2)
                f.flush()
                os.fsync(f.fileno())
            os.replace(tmp, self.path)
            self.state = draft
            return result

    def _migrate_locked(self) -> bool:
        """Backfill acceptance/repair fields on stores written by older code.

        The pre-acceptance schema only knew transfer tasks; such tasks get the
        new fields with safe defaults and keep their scheduler status. Proofs
        and the immutable manifest/effective-map records are never touched.
        """
        s = self.state
        changed = False
        for b in s["boxes"].values():
            if "proven" not in b:
                # legacy "inventory" was derived from the current mirror image;
                # it was produced by the mirror subsystem's own verification,
                # so it seeds the proven-object map (no history is rewritten).
                proven = {h: {"object_hash": h,
                              "manifest_id": "mirror:image",
                              "manifest_hash": None,
                              "proven_at": b.get("created_at", s["clock"]),
                              "proof_id": None,
                              "source": "mirror-image"}
                          for h in b.get("inventory", [])}
                b["proven"] = proven
                changed = True
        for t in s["tasks"].values():
            if t.get("kind", "transfer") == "transfer":
                t.setdefault("kind", "transfer")
                t.setdefault("accept_stage", None)
                t.setdefault("accept_seq", 0)
                t.setdefault("last_run_id", None)
                t.setdefault("chunks_local", {})
                t.setdefault("needed_chunks", [])
                t.setdefault("quarantined", [])      # live quarantine record list
                t.setdefault("quarantine_history", [])
                t.setdefault("active_repair_id", None)
                t.setdefault("run_ids", [])
                t.setdefault("generations", {})
                t.setdefault("manifest_hash",
                             s["jobs"].get(t["job_id"], {}).get("manifest_hash"))
                changed = True
            else:
                t.setdefault("repair_generation", t.get("generation"))
                t.setdefault("manifest_id",
                             s["jobs"].get(t["job_id"], {}).get("manifest_id"))
                t.setdefault("manifest_hash",
                             s["jobs"].get(t["job_id"], {}).get("manifest_hash"))
                t.setdefault("chunk_indices", [])
                t.setdefault("result", None)
                t.setdefault("attempt", 1)
                changed = True
        return changed

    def _recover_locked(self) -> bool:
        """Rebuild queue/debts/remaining after a (crash) restart.

        Any task caught mid-transfer held a (disk reservation, transfer slot)
        pair; the service is gone, so the pair is released and the task is
        re-queued — resume only sends the remaining bytes. Debts persist.
        The caller immediately re-settles, so still-eligible tasks re-acquire
        their pairs deterministically; nothing leaks either way. Repair tasks
        are recovered exactly the same way and converge after re-acceptance.
        """
        crashed = [t for t in self.state["tasks"].values()
                   if t["status"] == "transferring"]
        if not crashed:
            return False
        for t in crashed:
            t["status"] = "queued"
            t["reserved_bytes"] = 0.0
            if t.get("kind") == "repair":
                msg = ("服务重启：修复名额与预留已成对释放（无幽灵占用），"
                       f"修复 {t['repair_key']} 重新排队，只补剩余 "
                       f"{self._remaining(t):.0f} 字节；欠账保留")
            else:
                msg = ("服务重启：传输名额与磁盘预留已成对释放（无幽灵占用），"
                       f"续传只补剩余 {self._remaining(t):.0f} 字节；欠账保留")
            self._journal(self.state, t, "RECOVERED", msg)
            t["updated_at"] = self.state["clock"]
        n_repair = sum(1 for t in crashed if t.get("kind") == "repair")
        self._event_locked(
            self.state, "SERVICE_RECOVERED",
            f"服务重启恢复：{len(crashed)} 个传输中任务重新排队"
            f"（其中修复任务 {n_repair} 个），磁盘预留与传输名额成对释放，"
            f"队列/欠账/剩余时长与隔离状态已重建，无幽灵占用",
            crashed[0]["job_id"])
        return True

    # ============================================================= helpers ==
    def _next(self, s: dict, key: str, prefix: str) -> str:
        s["seq"][key] += 1
        return f"{prefix}-{s['seq'][key]:04d}"

    def _clock(self, s: dict) -> datetime:
        return _parse(s["clock"])

    def _event_locked(self, s: dict, etype: str, summary: str, ref: str) -> dict:
        s["seq"]["event"] += 1
        ev = {"event_id": s["seq"]["event"], "ts": s["clock"],
              "type": etype, "summary": summary, "ref": ref}
        s["events"].append(ev)
        return ev

    def _journal(self, s: dict, task: dict, event: str, detail: str):
        task["journal"].append({"ts": s["clock"], "event": event, "detail": detail})

    @staticmethod
    def _remaining(t: dict) -> float:
        return max(0.0, t["bytes_total"] - t["bytes_done"])

    # ----------------------------------------------- transfer / repair sets
    @staticmethod
    def _is_repair(t: dict) -> bool:
        return t.get("kind") == "repair"

    def _transfer_tasks(self, s: dict) -> list:
        return [t for t in s["tasks"].values() if t.get("kind", "transfer") == "transfer"]

    def _open_transfer(self, s: dict) -> list:
        """Transfer tasks the scheduler may grant (disk, slot) pairs to."""
        return [t for t in self._transfer_tasks(s)
                if t["status"] in TASK_OPEN
                and s["jobs"][t["job_id"]]["status"] == "active"]

    def _open_repairs(self, s: dict) -> list:
        """Repair tasks the scheduler may serve: parent must still be quarantined."""
        out = []
        for t in s["tasks"].values():
            if t.get("kind") != "repair" or t["status"] not in ("queued", "transferring"):
                continue
            parent = s["tasks"].get(t["parent_task_id"])
            job = s["jobs"].get(t["job_id"])
            if parent is None or job is None:
                continue
            if parent["accept_stage"] != ACCEPT_QUARANTINED or job["status"] != "active":
                # canceled job / newer acceptance superseded this repair: converge
                if t["status"] == "transferring":
                    t["status"] = "queued"
                    t["reserved_bytes"] = 0.0
                continue
            out.append(t)
        return out

    def _scheduled_tasks(self, s: dict) -> list:
        return self._open_transfer(s) + self._open_repairs(s)

    def list_events(self, limit: int = 80) -> list:
        with self.lock:
            return copy.deepcopy(self.state["events"][-limit:][::-1])

    # ==================================================== mirror (read-only)
    def _manifest(self, manifest_id: str) -> dict:
        """Read-only lookup of an immutable manifest in the mirror store."""
        if self.mirror is None:
            raise DomainError("PREHEAT_MIRROR_UNAVAILABLE",
                              "镜像仓库不可用，无法读取内容清单", 409)
        with self.mirror.lock:
            man = self.mirror.state.get("manifests", {}).get(manifest_id)
            if not man:
                raise DomainError("PREHEAT_MANIFEST_UNKNOWN",
                                  f"清单 {manifest_id} 不在镜像仓库中；请先在"
                                  f"镜像分发页生成不可变清单", 404)
            return copy.deepcopy(man)

    def _mirror_blob(self, manifest_hash: str, index: int) -> Optional[bytes]:
        """READ-ONLY fetch of one compressed chunk blob from the mirror store."""
        if self.mirror is None:
            return None
        with self.mirror.lock:
            rec = self.mirror.state.get("chunk_store", {}).get(
                f"{manifest_hash}:{index}")
            if rec is None:
                return None
            return _b64d(rec["data_b64"])

    @staticmethod
    def _decode_blob(blob: bytes) -> Optional[dict]:
        """Decompress+decode a box-local/mirror chunk blob; None if undecodable."""
        try:
            return json.loads(zlib.decompress(blob).decode("utf-8"))
        except Exception:
            return None

    def _authoritative_objects(self, manifest: dict) -> dict:
        """object_id -> {hash, length} straight from the immutable mirror blobs.

        This is the box-side ground truth at acceptance time: lengths are the
        canonical byte lengths of the object content as packed in the manifest;
        they are recomputed from the repository, never from mutable views.
        """
        truth: dict[str, dict] = {}
        for c in manifest["chunks"]:
            blob = self._mirror_blob(manifest["manifest_hash"], c["index"])
            if blob is None:
                continue
            payload = self._decode_blob(blob)
            if not payload:
                continue
            for o in payload.get("objects", []):
                content = o.get("content")
                truth[o["object_id"]] = {
                    "hash": o.get("hash"),
                    "length": len(canonical(content)),
                    "chunk_index": c["index"]}
        return truth

    # ====================================================== sites & boxes ==
    def upsert_site(self, site_id: str, capacity_kbps: float) -> dict:
        site_id = (site_id or "").strip()
        if not site_id:
            raise DomainError("BAD_REQUEST", "站点编号不能为空", 400)
        try:
            cap = float(capacity_kbps)
        except (TypeError, ValueError):
            raise DomainError("BAD_REQUEST", "站点带宽必须是数字（KB/s）", 400)
        if cap <= 0:
            raise DomainError("BAD_REQUEST", "站点共享带宽必须 > 0", 400)

        def m(s):
            site = s["sites"].get(site_id)
            if site is None:
                site = {"site_id": site_id, "capacity_kbps": cap,
                        "created_at": s["clock"], "updated_at": s["clock"]}
                s["sites"][site_id] = site
                self._event_locked(s, "SITE_REGISTERED",
                                   f"注册站点 {site_id}（共享带宽 {cap:g} KB/s）", site_id)
            elif abs(site["capacity_kbps"] - cap) > EPS:
                old = site["capacity_kbps"]
                site["capacity_kbps"] = cap
                site["updated_at"] = s["clock"]
                self._event_locked(
                    s, "QUOTA_CHANGED",
                    f"站点 {site_id} 共享带宽 {old:g} → {cap:g} KB/s；"
                    f"超出部分将按优先级确定性抢占", site_id)
            return site_id

        self.commit(m)
        return self.get_site(site_id)

    def set_site_capacity(self, site_id: str, capacity_kbps: float) -> dict:
        with self.lock:
            if site_id not in self.state["sites"]:
                raise DomainError("PREHEAT_SITE_UNKNOWN",
                                  f"站点 {site_id} 未注册", 404)
        return self.upsert_site(site_id, capacity_kbps)

    def upsert_box(self, box_id: str, site_id: str, disk_total_mb: float,
                   link_cap_kbps: float, disk_used_mb: float = 0.0) -> dict:
        box_id = (box_id or "").strip()
        site_id = (site_id or "").strip()
        if not box_id or not site_id:
            raise DomainError("BAD_REQUEST", "盒子与站点编号不能为空", 400)
        try:
            disk_total = float(disk_total_mb) * 1e6
            used = float(disk_used_mb) * 1e6
            cap = float(link_cap_kbps)
        except (TypeError, ValueError):
            raise DomainError("BAD_REQUEST", "磁盘/链路参数必须是数字", 400)
        if disk_total <= 0 or used < 0 or used > disk_total or cap < 0:
            raise DomainError("BAD_REQUEST",
                              "磁盘总量必须 > 0、0 ≤ 已用 ≤ 总量、链路速率 ≥ 0", 400)
        with self.lock:
            if site_id not in self.state["sites"]:
                raise DomainError("PREHEAT_SITE_UNKNOWN",
                                  f"站点 {site_id} 未注册，请先注册站点", 404)

        def m(s):
            box = s["boxes"].get(box_id)
            if box is None:
                proven = self._seed_proven(box_id)
                box = {"box_id": box_id, "site_id": site_id,
                       "disk_total_bytes": disk_total, "disk_used_bytes": used,
                       "link_cap_kbps": cap, "online": True,
                       "inventory": sorted(proven),
                       "proven": proven,
                       "created_at": s["clock"], "updated_at": s["clock"]}
                s["boxes"][box_id] = box
                self._event_locked(
                    s, "BOX_REGISTERED",
                    f"注册盒子 {box_id} @ {site_id}（磁盘 {disk_total / 1e6:g} MB，"
                    f"链路 {cap:g} KB/s，已证明对象 {len(proven)} 个）", box_id)
            else:
                changes = []
                if box["site_id"] != site_id:
                    changes.append(f"站点 {box['site_id']} → {site_id}")
                    box["site_id"] = site_id
                if abs(box["disk_total_bytes"] - disk_total) > EPS:
                    changes.append(
                        f"磁盘 {box['disk_total_bytes'] / 1e6:g} → {disk_total / 1e6:g} MB")
                    box["disk_total_bytes"] = disk_total
                if abs(box["disk_used_bytes"] - used) > EPS:
                    changes.append(
                        f"已用 {box['disk_used_bytes'] / 1e6:g} → {used / 1e6:g} MB")
                    box["disk_used_bytes"] = used
                if abs(box["link_cap_kbps"] - cap) > EPS:
                    changes.append(
                        f"链路 {box['link_cap_kbps']:g} → {cap:g} KB/s")
                    box["link_cap_kbps"] = cap
                if changes:
                    box["updated_at"] = s["clock"]
                    self._event_locked(s, "BOX_UPDATED",
                                       f"盒子 {box_id} 配置更新：" + "，".join(changes),
                                       box_id)
            return box_id

        self.commit(m)
        return self.get_box(box_id)

    def _seed_proven(self, box_id: str) -> dict:
        """Content-reuse seed: objects of the box's current mirror image.

        The mirror subsystem only flips its pointer after every object was
        re-verified (chunk + root hash), so image objects are already proven;
        they seed the box's proven-object map but no preheat proof is forged.
        """
        if self.mirror is None:
            return {}
        with self.mirror.lock:
            dev = self.mirror.state.get("devices", {}).get(box_id)
            img = (dev or {}).get("current_image")
            if not img:
                return {}
            man = self.mirror.state.get("manifests", {}).get(img.get("manifest_id"))
            if not man:
                return {}
            at = img.get("switched_at", utcnow_iso())
            return {o["hash"]: {"object_hash": o["hash"],
                                "manifest_id": img.get("manifest_id"),
                                "manifest_hash": img.get("manifest_hash"),
                                "proven_at": at,
                                "proof_id": None,
                                "source": "mirror-image"}
                    for o in man.get("objects", [])}

    @staticmethod
    def _proven_hashes(box: dict) -> set:
        return set(box.get("proven", {}).keys())

    @staticmethod
    def _refresh_inventory(box: dict):
        """inventory (legacy reuse list) is always derived from proven map."""
        box["inventory"] = sorted(box.get("proven", {}).keys())

    def set_box_link(self, box_id: str, link_cap_kbps: float) -> dict:
        try:
            cap = float(link_cap_kbps)
        except (TypeError, ValueError):
            raise DomainError("BAD_REQUEST", "链路速率必须是数字（KB/s）", 400)
        if cap < 0:
            raise DomainError("BAD_REQUEST", "链路速率上限不能为负", 400)

        def m(s):
            box = s["boxes"].get(box_id)
            if not box:
                raise DomainError("PREHEAT_BOX_UNKNOWN", f"盒子 {box_id} 未注册", 404)
            old = box["link_cap_kbps"]
            box["link_cap_kbps"] = cap
            box["updated_at"] = s["clock"]
            kind = "LINK_DEGRADED" if cap < old else "LINK_RESTORED"
            self._event_locked(
                s, kind,
                f"盒子 {box_id} 链路速率上限 {old:g} → {cap:g} KB/s"
                + ("（链路降级，超出部分确定性让路）" if cap < old else "（链路恢复）"),
                box_id)
            return box_id

        self.commit(m)
        return self.get_box(box_id)

    def set_box_online(self, box_id: str, online: bool) -> dict:
        def m(s):
            box = s["boxes"].get(box_id)
            if not box:
                raise DomainError("PREHEAT_BOX_UNKNOWN", f"盒子 {box_id} 未注册", 404)
            if box["online"] == bool(online):
                return box_id
            box["online"] = bool(online)
            box["updated_at"] = s["clock"]
            self._event_locked(
                s, "BOX_OFFLINE" if not online else "BOX_ONLINE",
                f"盒子 {box_id} {'离线：传输名额与磁盘预留成对释放，不留幽灵占用' if not online else '恢复在线：重新排队' }",
                box_id)
            return box_id

        self.commit(m)
        return self.get_box(box_id)

    # ============================================================ content ==
    def _needed_chunks(self, manifest: dict, box: dict) -> list:
        """Chunk indices still needed: chunks holding ≥1 object not PROVEN.

        Only acceptance-proven objects are reusable; unproven/delivered
        content is never assumed present, so it gets retransferred.
        """
        proven = self._proven_hashes(box)
        out = []
        for c in manifest["chunks"]:
            objs = [o for o in manifest["objects"] if o["object_id"] in c["objects"]]
            if any(o["hash"] not in proven for o in objs):
                out.append(c["index"])
        return out

    def _needed_bytes(self, s: dict, manifest: dict, box: dict):
        """Bytes this box still needs: chunks holding ≥1 unproven object."""
        total = sum(c["size"] for c in manifest["chunks"])
        needed_idx = set(self._needed_chunks(manifest, box))
        needed = sum(c["size"] for c in manifest["chunks"]
                     if c["index"] in needed_idx)
        return needed, total

    # ============================================================== submit ==
    def create_job(self, body: dict) -> dict:
        manifest_id = (body.get("manifest_id") or "").strip()
        man = self._manifest(manifest_id)  # read-only; 404 if unknown
        targets = body.get("targets")
        if not isinstance(targets, list) or not targets or \
                not all(isinstance(t, str) and t.strip() for t in targets):
            raise DomainError("PREHEAT_NO_TARGETS",
                              "targets 必须是非空盒子编号数组", 400)
        targets = list(dict.fromkeys(t.strip() for t in targets))
        try:
            link_cap = float(body.get("link_cap_kbps", 1024))
            min_online = int(body.get("min_online", len(targets)))
        except (TypeError, ValueError):
            raise DomainError("BAD_REQUEST", "速率/最小在线参数非法", 400)
        if link_cap <= 0:
            raise DomainError("BAD_REQUEST", "作业链路速率上限必须 > 0", 400)
        min_online = max(1, min(len(targets), min_online))
        note = body.get("note", "")
        created_by = body.get("created_by", "dispatcher")

        with self.lock:
            missing = [t for t in targets if t not in self.state["boxes"]]
            if missing:
                raise DomainError(
                    "PREHEAT_BOX_UNKNOWN",
                    f"目标盒子未注册：{', '.join(missing)}；请先注册盒子", 404)
            clock = self._clock(self.state)
            if body.get("deadline"):
                try:
                    deadline = _parse(body["deadline"])
                except ValueError:
                    raise DomainError("BAD_REQUEST", "deadline 必须是 ISO 时间", 400)
            else:
                secs = float(body.get("deadline_in_sec", 3600))
                if secs <= 0:
                    raise DomainError("BAD_REQUEST", "截止时间必须在未来", 400)
                deadline = clock + timedelta(seconds=secs)
            key = sha256({"manifest_hash": man["manifest_hash"],
                          "targets": sorted(targets),
                          "deadline": _iso(deadline),
                          "link_cap_kbps": link_cap,
                          "min_online": min_online, "note": note})
            existing = self.state["job_by_key"].get(key)
            if existing and self.state["jobs"][existing]["status"] != "canceled":
                # deterministic resubmit: identical request replays the first job
                return {"job": self._job_view(self.state, self.state["jobs"][existing],
                                              idempotent=True),
                        "idempotent": True}

        def m(s):
            jid = self._next(s, "job", "PJ")
            job = {"job_id": jid, "seq": s["seq"]["job"], "job_key": key,
                   "manifest_id": man["manifest_id"],
                   "manifest_hash": man["manifest_hash"],
                   "version": man["source"]["version"],
                   "targets": list(targets), "deadline": _iso(deadline),
                   "link_cap_kbps": link_cap, "min_online": min_online,
                   "note": note, "status": "active", "cancel_result": None,
                   "created_by": created_by,
                   "created_at": s["clock"], "updated_at": s["clock"]}
            s["jobs"][jid] = job
            s["job_by_key"][key] = jid
            tasks = []
            for bid in targets:
                tid = self._next(s, "task", "PT")
                box = s["boxes"][bid]
                needed, total = self._needed_bytes(s, man, box)
                needed_idx = self._needed_chunks(man, box)
                task = {"task_id": tid, "seq": s["seq"]["task"], "job_id": jid,
                        "box_id": bid, "site_id": box["site_id"],
                        "kind": "transfer",
                        "manifest_id": man["manifest_id"],
                        "manifest_hash": man["manifest_hash"],
                        "generations": dict(man["generations"]),
                        "status": "queued",
                        "accept_stage": None,       # transferred|quarantined|proven
                        "accept_seq": 0,
                        "last_run_id": None,
                        "run_ids": [],
                        "chunks_local": {},         # index -> b64 blob actually on the box
                        "needed_chunks": needed_idx,
                        "quarantined": [],          # live quarantine object records
                        "quarantine_history": [],
                        "active_repair_id": None,
                        "bytes_total": needed, "bytes_done": 0.0,
                        "reserved_bytes": 0.0, "debt_kb": 0.0,
                        "reuse_ratio": (1.0 - needed / total) if total > EPS else 1.0,
                        "journal": [], "created_at": s["clock"],
                        "updated_at": s["clock"], "done_at": None}
                self._journal(s, task, "TASK_QUEUED",
                              f"作业 {jid} 下发：需传输 {needed:.0f}/{total:.0f} 字节"
                              f"（内容复用率 {(task['reuse_ratio'] * 100):.0f}%）")
                s["tasks"][tid] = task
                tasks.append(tid)
            job["tasks"] = tasks
            self._event_locked(
                s, "JOB_SUBMITTED",
                f"预热作业 {jid}：清单 {man['manifest_id']}（{man['source']['version']}）"
                f"→ {len(targets)} 台盒子，截止 {job['deadline']}，"
                f"链路 ≤{link_cap:g} KB/s，最小在线保障 {min_online}/{len(targets)}",
                jid)
            return jid

        jid = self.commit(m)
        with self.lock:
            return {"job": self._job_view(self.state, self.state["jobs"][jid],
                                          idempotent=False),
                    "idempotent": False}

    # ====================================================== job lifecycle ==
    def _get_job(self, s: dict, job_id: str) -> dict:
        job = s["jobs"].get(job_id)
        if not job:
            raise DomainError("PREHEAT_JOB_UNKNOWN", f"作业 {job_id} 不存在", 404)
        return job

    def cancel_job(self, job_id: str, by: str = "dispatcher") -> dict:
        """Verdict-once: re-canceling replays the first cancel result."""
        with self.lock:
            job0 = self._get_job(self.state, job_id)
            if job0["status"] == "canceled":
                return {"job_id": job_id, "status": "canceled",
                        "result": copy.deepcopy(job0["cancel_result"]),
                        "idempotent": True}
            if job0["status"] == "completed":
                raise DomainError("PREHEAT_JOB_CLOSED",
                                  f"作业 {job_id} 已完成，不能取消", 409)

        def m(s):
            job = s["jobs"][job_id]
            released = 0
            refunded_bytes = 0.0
            repairs_canceled = 0
            for tid in job["tasks"]:
                t = s["tasks"][tid]
                # cancel any repair tasks spawned by this transfer task
                for r in list(s["tasks"].values()):
                    if r.get("kind") != "repair" or r["parent_task_id"] != tid:
                        continue
                    if r["status"] in ("queued", "transferring", "paused"):
                        if r["reserved_bytes"] > EPS:
                            released += 1
                        r["status"] = "canceled"
                        r["reserved_bytes"] = 0.0
                        r["updated_at"] = s["clock"]
                        repairs_canceled += 1
                        self._journal(s, r, "REPAIR_CANCELED",
                                      "父作业取消：修复名额与预留成对释放，"
                                      "隔离状态确定收敛，不留容量占用")
                if t["status"] in TASK_OPEN + ("paused",):
                    if t["reserved_bytes"] > EPS:
                        released += 1
                    t["status"] = "canceled"
                    t["reserved_bytes"] = 0.0
                    t["updated_at"] = s["clock"]
                    self._journal(s, t, "TASK_CANCELED",
                                  "作业取消：磁盘预留与传输名额成对释放，"
                                  "部分数据已清除，不留幽灵占用")
                elif t["accept_stage"] in (ACCEPT_PENDING, ACCEPT_QUARANTINED):
                    # delivered-but-unproven bytes are not reusable: refund the
                    # space of chunks not backed by any proven object
                    refunded = self._release_unproven_locked(s, t)
                    if refunded > EPS:
                        refunded_bytes += refunded
                    t["status"] = "canceled"
                    t["active_repair_id"] = None
                    t["updated_at"] = s["clock"]
                    self._journal(
                        s, t, "TASK_CANCELED",
                        f"作业取消：待验收/隔离内容清除，释放未证明空间 "
                        f"{refunded:.0f} 字节；已证明对象保留可复用，不留幽灵占用")
                t["active_repair_id"] = None
            job["status"] = "canceled"
            job["updated_at"] = s["clock"]
            result = {"outcome": "canceled", "by": by, "at": s["clock"],
                      "released_pairs": released,
                      "refunded_unproven_bytes": round(refunded_bytes, 1),
                      "repairs_canceled": repairs_canceled}
            job["cancel_result"] = result
            self._event_locked(
                s, "JOB_CANCELED",
                f"作业 {job_id} 已取消：{released} 对预留/名额成对释放，"
                f"{repairs_canceled} 个修复取消，释放未证明空间 "
                f"{refunded_bytes:.0f} 字节", job_id)
            return copy.deepcopy(result)

        result = self.commit(m)
        return {"job_id": job_id, "status": "canceled",
                "result": result, "idempotent": False}

    def pause_job(self, job_id: str) -> dict:
        return self._pause_resume(job_id, pause=True)

    def resume_job(self, job_id: str) -> dict:
        return self._pause_resume(job_id, pause=False)

    def _pause_resume(self, job_id: str, pause: bool) -> dict:
        with self.lock:
            job0 = self._get_job(self.state, job_id)
            want = "paused" if pause else "active"
            if job0["status"] == want:
                return {"job_id": job_id, "status": want, "idempotent": True}
            if job0["status"] in ("canceled", "completed"):
                raise DomainError(
                    "PREHEAT_JOB_CLOSED",
                    f"作业 {job_id} 已终结（{job0['status']}），"
                    f"{'暂停' if pause else '恢复'}被拒绝", 409)

        def m(s):
            job = s["jobs"][job_id]
            job["status"] = "paused" if pause else "active"
            job["updated_at"] = s["clock"]
            repair_tasks = [r for r in s["tasks"].values()
                            if r.get("kind") == "repair" and r["job_id"] == job_id]
            for t in [s["tasks"][tid] for tid in job["tasks"]] + repair_tasks:
                if pause and t["status"] in TASK_OPEN:
                    t["status"] = "paused"
                    t["reserved_bytes"] = 0.0
                    t["updated_at"] = s["clock"]
                    what = "修复" if t.get("kind") == "repair" else "作业"
                    self._journal(s, t, "TASK_PAUSED",
                                  f"{what}暂停：磁盘预留与传输名额成对释放；"
                                  "恢复时只补剩余数据")
                elif not pause and t["status"] == "paused":
                    t["status"] = "queued"
                    t["updated_at"] = s["clock"]
                    self._journal(s, t, "TASK_RESUMED",
                                  f"恢复：重新排队，只补剩余 "
                                  f"{self._remaining(t):.0f} 字节")
            self._event_locked(
                s, "JOB_PAUSED" if pause else "JOB_RESUMED",
                f"作业 {job_id} {'已暂停（传输与修复的名额/预留均成对释放）' if pause else '已恢复（只补剩余数据）'}",
                job_id)
            return job["status"]

        status = self.commit(m)
        return {"job_id": job_id, "status": status, "idempotent": False}

    # ============================================================ the plan ==
    def _open_tasks(self, s: dict) -> list:
        """Every scheduler-eligible task: transfers plus live repairs."""
        return self._scheduled_tasks(s)

    def _want(self, s: dict, t: dict) -> float:
        job = s["jobs"][t["job_id"]]
        box = s["boxes"][t["box_id"]]
        return min(job["link_cap_kbps"], box["link_cap_kbps"])

    @staticmethod
    def _priority(s: dict):
        def key(t):
            job = s["jobs"][t["job_id"]]
            return (-t["debt_kb"], job["deadline"], job["seq"], t["seq"])
        return key

    def _plan(self, s: dict, remaining: Optional[dict] = None) -> dict:
        """Pure scheduling function: {task_id: {grant_kbps, code, message}}.

        Greedy by (debt desc, deadline, seq) within each site; the disk
        reservation and the transfer slot are granted together, never apart.
        """
        plan: dict[str, dict] = {}
        by_site: dict[str, list] = {}
        for t in self._open_tasks(s):
            by_site.setdefault(t["site_id"], []).append(t)
        for site_id, tasks in by_site.items():
            site = s["sites"].get(site_id)
            if site is None:
                continue
            budget = site["capacity_kbps"]
            reserved: dict[str, float] = {}
            for t in sorted(tasks, key=self._priority(s)):
                tid = t["task_id"]
                box = s["boxes"][t["box_id"]]
                rem = (remaining.get(tid, 0.0) if remaining is not None
                       else self._remaining(t))
                if rem <= EPS:
                    plan[tid] = {"grant_kbps": 0.0, "code": "READY",
                                 "message": REASONS["READY"]}
                    continue
                if not box["online"]:
                    plan[tid] = {"grant_kbps": 0.0, "code": "BOX_OFFLINE",
                                 "message": REASONS["BOX_OFFLINE"]}
                    continue
                free = (box["disk_total_bytes"] - box["disk_used_bytes"]
                        - reserved.get(t["box_id"], 0.0))
                if rem > free + EPS:
                    plan[tid] = {"grant_kbps": 0.0, "code": "BOX_DISK_FULL",
                                 "message": REASONS["BOX_DISK_FULL"]}
                    continue
                want = self._want(s, t)
                if want <= EPS:
                    plan[tid] = {"grant_kbps": 0.0, "code": "LINK_DOWN",
                                 "message": REASONS["LINK_DOWN"]}
                    continue
                grant = min(want, budget)
                if grant <= EPS:
                    plan[tid] = {"grant_kbps": 0.0, "code": "SITE_SATURATED",
                                 "message": REASONS["SITE_SATURATED"]}
                    continue
                budget -= grant
                reserved[t["box_id"]] = reserved.get(t["box_id"], 0.0) + rem
                if grant < want - EPS:
                    code = "SITE_SHARED"
                elif box["link_cap_kbps"] < s["jobs"][t["job_id"]]["link_cap_kbps"] - EPS:
                    code = "BOX_LINK"
                else:
                    code = "JOB_CAP"
                plan[tid] = {"grant_kbps": _round6(grant), "code": code,
                             "message": REASONS[code]}
        return plan

    def _settle(self, s: dict):
        """Apply the plan to the state: acquire/release (disk, slot) pairs."""
        # content reuse: unstarted TRANSFER tasks shrink as the box's proven
        # map grows; a fully-proven task is proven instantly (no transfer);
        # repairs are never auto-resized (their scope is fixed at creation).
        changed = True
        while changed:
            changed = False
            for t in sorted(self._open_transfer(s), key=lambda x: x["seq"]):
                if t["bytes_done"] > EPS or t["status"] != "queued":
                    continue
                man = self._manifest_ref(s, t)
                if man is None:
                    continue
                box = s["boxes"][t["box_id"]]
                needed, total = self._needed_bytes(s, man, box)
                new_idx = self._needed_chunks(man, box)
                if abs(needed - t["bytes_total"]) > EPS or \
                        new_idx != t["needed_chunks"]:
                    t["bytes_total"] = needed
                    t["needed_chunks"] = new_idx
                    t["reuse_ratio"] = (1.0 - needed / total) if total > EPS else 1.0
                    t["updated_at"] = s["clock"]
                    changed = True
                if needed <= EPS:
                    self._auto_prove_reused(s, t, man)
                    changed = True
        plan = self._plan(s)
        for t in self._open_tasks(s):
            p = plan.get(t["task_id"])
            granted = p is not None and p["grant_kbps"] > EPS
            if granted and t["status"] != "transferring":
                t["status"] = "transferring"
                t["reserved_bytes"] = self._remaining(t)
                t["updated_at"] = s["clock"]
                kind = "修复" if self._is_repair(t) else "传输"
                self._journal(s, t, "PAIR_ACQUIRED",
                              f"成对取得磁盘预留 {self._remaining(t):.0f} 字节 + "
                              f"{kind}名额（{p['grant_kbps']:g} KB/s，{p['message']}）")
            elif granted:
                t["reserved_bytes"] = self._remaining(t)
            elif not granted and t["status"] == "transferring":
                t["status"] = "queued"
                t["reserved_bytes"] = 0.0
                t["updated_at"] = s["clock"]
                self._journal(s, t, "PAIR_RELEASED",
                              f"成对释放磁盘预留与传输名额（{p['message'] if p else '计划外'}）；"
                              f"已传 {t['bytes_done']:.0f} 字节保留，续传只补剩余")
        # job completion: only fully PROVEN transfer tasks count
        for job in s["jobs"].values():
            if job["status"] in JOB_OPEN and job.get("tasks") and \
                    all(s["tasks"][tid]["status"] == "done" for tid in job["tasks"]):
                job["status"] = "completed"
                job["updated_at"] = s["clock"]
                self._event_locked(
                    s, "JOB_COMPLETED",
                    f"作业 {job['job_id']} 全部 {len(job['tasks'])} 台盒子完成"
                    f"盒端验收并证明，内容进入可切换集合",
                    job["job_id"])

    def _manifest_ref(self, s: dict, t: dict) -> Optional[dict]:
        """Manifest for a task, read-only from the mirror store."""
        if self.mirror is None:
            return None
        job = s["jobs"][t["job_id"]]
        with self.mirror.lock:
            return self.mirror.state.get("manifests", {}).get(job["manifest_id"])

    # ----------------------------------------------- delivery / completion
    def _stage_delivered(self, s: dict, t: dict, manifest: dict):
        """A transfer just finished: copy delivered chunk blobs onto the box.

        Delivered bytes are counted in disk usage but are NOT proven yet; the
        box must run acceptance before any object can be reused or switched.
        """
        for idx in t["needed_chunks"]:
            if str(idx) in t["chunks_local"]:
                continue
            blob = self._mirror_blob(manifest["manifest_hash"], idx)
            if blob is not None:
                t["chunks_local"][str(idx)] = _b64e(blob)

    def _complete_transfer(self, s: dict, t: dict):
        """Transfer finished: mark delivered (待验收), release the pair."""
        man = self._manifest_ref(s, t)
        t["status"] = ACCEPT_PENDING
        t["accept_stage"] = ACCEPT_PENDING
        t["bytes_done"] = t["bytes_total"]
        t["reserved_bytes"] = 0.0
        t["updated_at"] = s["clock"]
        if man is not None:
            self._stage_delivered(s, t, man)
            box = s["boxes"][t["box_id"]]
            # delivered chunks now occupy the box; quarantine/cancel release
            # exactly these holds, so no capacity can ever leak
            self._hold_box_locked(s, box, man, t["needed_chunks"])
            box["updated_at"] = s["clock"]
        self._journal(
            s, t, "TRANSFER_DELIVERED",
            f"传输完成：{t['bytes_total']:.0f} 字节已到盒，磁盘预留与传输名额成对释放；"
            f"等待盒端逐项验收（长度/块散列/根散列），验收前不计入复用")
        self._event_locked(
            s, "TRANSFER_DELIVERED",
            f"任务 {t['task_id']}（{t['job_id']}→{t['box_id']}）传输完成，待盒端验收",
            t["task_id"])

    def _auto_prove_reused(self, s: dict, t: dict, manifest: dict):
        """All objects already proven on the box: instant zero-transfer proof."""
        t["bytes_total"] = 0.0
        t["bytes_done"] = 0.0
        t["reserved_bytes"] = 0.0
        t["needed_chunks"] = []
        run = self._append_run(
            s, t, manifest, outcome="proven",
            objects=[{**o, "result": "reused", "reason": "ACCEPT_OK",
                      "chunk_index": next(
                          (c["index"] for c in manifest["chunks"]
                           if o["object_id"] in c["objects"]), None)}
                     for o in manifest["objects"]],
            chunk_results={str(c["index"]): {"result": "reused",
                                             "reason": "ACCEPT_OK"}
                           for c in manifest["chunks"]},
            root={"declared": manifest["root_hash"],
                  "computed": manifest["root_hash"], "match": True},
            note="全部对象已在盒上且有有效证明（复用率 100%），零传输即时证明")
        self._finalize_proven(s, t, manifest, run, reused=True)

    def _finalize_proven(self, s: dict, t: dict, manifest: dict, run: dict,
                         reused: bool = False):
        box = s["boxes"][t["box_id"]]
        # any still-open repair was created against an older quarantine
        # generation: it must never overwrite the new proof — retire it now
        for r in s["tasks"].values():
            if r.get("kind") == "repair" and r["parent_task_id"] == t["task_id"] \
                    and r["status"] in ("queued", "transferring", "paused") \
                    and r["repair_generation"] < run["accept_seq"]:
                r["status"] = "superseded"
                r["reserved_bytes"] = 0.0
                r["result"] = {"outcome": "superseded",
                               "repair_generation": r["repair_generation"],
                               "current_generation": run["accept_seq"],
                               "at": s["clock"]}
                r["done_at"] = s["clock"]
                r["updated_at"] = s["clock"]
                self._journal(
                    s, r, "REPAIR_SUPERSEDED",
                    f"父任务已到第 {run['accept_seq']} 代证明：第 "
                    f"{r['repair_generation']} 代旧修复立即失效，不覆盖新证明，"
                    f"名额与预留成对释放")
                self._event_locked(s, "REPAIR_SUPERSEDED",
                                   f"修复 {r['task_id']} 旧代次失效，不覆盖新证明",
                                   r["task_id"])
        t["status"] = "done"
        t["accept_stage"] = ACCEPT_PROVEN
        t["reserved_bytes"] = 0.0
        t["active_repair_id"] = None
        t["quarantined"] = []
        t["done_at"] = s["clock"]
        t["updated_at"] = s["clock"]
        # admit verified objects to the proven map (only reuse / switch source)
        admitted = 0
        for o in run["objects"]:
            if o["result"] not in ("ok", "reused"):
                continue
            h = o["hash"]
            if h not in box["proven"]:
                admitted += 1
            if reused:
                rec = box["proven"].get(h, {})
                box["proven"][h] = {"object_hash": h,
                                    "manifest_id": manifest["manifest_id"],
                                    "manifest_hash": manifest["manifest_hash"],
                                    "proven_at": s["clock"],
                                    "proof_id": rec.get("proof_id"),
                                    "source": "reused",
                                    "run_id": run["run_id"]}
            else:
                box["proven"][h] = {"object_hash": h,
                                    "manifest_id": manifest["manifest_id"],
                                    "manifest_hash": manifest["manifest_hash"],
                                    "proven_at": s["clock"],
                                    "proof_id": o.get("proof_id"),
                                    "source": "acceptance",
                                    "run_id": run["run_id"]}
        self._refresh_inventory(box)
        box["updated_at"] = s["clock"]
        self._journal(
            s, t, "ACCEPT_PROVEN",
            ("全部内容已有有效证明（复用率 100%），零传输完成" if reused else
             f"盒端验收通过（第 {run['accept_seq']} 号验收证明 "
             f"{short(run['proof_digest'])}）："
             f"长度/对象散列/块散列/根散列全部一致，{admitted} 个新对象进入"
             f"可切换集合并可参与后续复用"))
        self._event_locked(
            s, "ACCEPT_PROVEN",
            f"任务 {t['task_id']}（{t['box_id']}）第 {run['accept_seq']} 次验收通过"
            + ("（全复用零传输）" if reused else
               f"，{admitted} 个对象证明有效，进入可切换集合"),
            t["task_id"])

    # ================================================================ tick ==
    def tick(self, seconds: float) -> dict:
        """Advance the simulated clock; transfers progress at granted rates.

        Debt (欠账) accrues for every eligible-but-underserved task, so a
        continuously progressing job can never starve; jobs short of their
        minimum online guarantee accrue double.
        """
        try:
            secs = float(seconds)
        except (TypeError, ValueError):
            raise DomainError("BAD_REQUEST", "tick 秒数必须是数字", 400)
        if secs <= 0 or secs > 86400:
            raise DomainError("BAD_REQUEST", "tick 秒数须在 (0, 86400] 内", 400)

        def m(s):
            plan = self._plan(s)
            s["clock"] = _iso(self._clock(s) + timedelta(seconds=secs))
            # debt accrual per site (fair share = min(want, capacity / contenders))
            by_site: dict[str, list] = {}
            for t in self._open_tasks(s):
                by_site.setdefault(t["site_id"], []).append(t)
            for site_id, tasks in by_site.items():
                site = s["sites"].get(site_id)
                if site is None:
                    continue
                n = len(tasks)
                for t in tasks:
                    job = s["jobs"][t["job_id"]]
                    fair = min(self._want(s, t), site["capacity_kbps"] / n)
                    grant = plan.get(t["task_id"], {}).get("grant_kbps", 0.0)
                    done_n = sum(1 for tid in job["tasks"]
                                 if s["tasks"][tid]["status"] == "done")
                    boost = 2.0 if done_n < job["min_online"] else 1.0
                    t["debt_kb"] = _round6(
                        t["debt_kb"] + (fair - grant) * secs * boost)
            # advance transfers at the granted rate
            progressed, completed = [], []
            for t in self._open_tasks(s):
                p = plan.get(t["task_id"])
                if not p or p["grant_kbps"] <= EPS or t["status"] != "transferring":
                    continue
                add = p["grant_kbps"] * KB * secs
                t["bytes_done"] = min(t["bytes_total"], t["bytes_done"] + add)
                t["updated_at"] = s["clock"]
                progressed.append({"task_id": t["task_id"],
                                   "bytes_done": round(t["bytes_done"], 1),
                                   "bytes_total": round(t["bytes_total"], 1)})
                if self._remaining(t) <= EPS:
                    if self._is_repair(t):
                        self._complete_repair(s, t)
                        completed.append(t["task_id"])
                    else:
                        self._complete_transfer(s, t)
                        completed.append(t["task_id"])
            return {"clock": s["clock"], "advanced": progressed,
                    "completed": completed}

        return self.commit(m)

    # ==================================== box-end acceptance & quarantine ==
    @staticmethod
    def _chunk_size(manifest: dict, index: int) -> float:
        c = next(c for c in manifest["chunks"] if c["index"] == index)
        return float(c["size"])

    def _hold_box_locked(self, s: dict, box: dict, manifest: dict,
                         indices) -> float:
        """Charge box disk for chunks not already held (dedup across tasks)."""
        holds = box.setdefault("chunk_holds", {})
        charged = 0.0
        for idx in indices:
            key = f"{manifest['manifest_hash']}:{idx}"
            if key in holds:
                continue
            size = self._chunk_size(manifest, idx)
            holds[key] = {"size": size, "task_id": None, "manifest_id":
                          manifest["manifest_id"], "index": idx}
            box["disk_used_bytes"] += size
            charged += size
        if charged > EPS:
            box["updated_at"] = s["clock"]
        return charged

    def _release_holds_locked(self, s: dict, box: dict, manifest: dict,
                              indices) -> float:
        """Release chunk holds; returns released bytes (never double-frees)."""
        holds = box.get("chunk_holds", {})
        released = 0.0
        for idx in indices:
            key = f"{manifest['manifest_hash']}:{idx}"
            rec = holds.pop(key, None)
            if rec is not None:
                box["disk_used_bytes"] -= rec["size"]
                released += rec["size"]
        box["disk_used_bytes"] = max(0.0, box["disk_used_bytes"])
        if released > EPS:
            box["updated_at"] = s["clock"]
        return released

    def _release_unproven_locked(self, s: dict, t: dict) -> float:
        """Free delivered chunks that carry no proven object (cancel path)."""
        man = self._manifest_locked(s, t)
        box = s["boxes"][t["box_id"]]
        proven = self._proven_hashes(box)
        drop, keep = [], []
        for c in man["chunks"]:
            if str(c["index"]) not in t.get("chunks_local", {}) and \
                    c["index"] not in t["needed_chunks"]:
                continue
            objs = [o for o in man["objects"] if o["object_id"] in c["objects"]]
            # a chunk that still carries a proven object stays on disk
            (keep if any(o["hash"] in proven for o in objs) else drop) \
                .append(c["index"])
        released = self._release_holds_locked(s, box, man, drop)
        # local unproven copies are wiped; proven-object chunks remain on disk
        for idx in drop:
            t.get("chunks_local", {}).pop(str(idx), None)
        return released

    def _append_run(self, s: dict, t: dict, manifest: dict, *, outcome: str,
                    objects: list, chunk_results: dict, root: dict,
                    note: str = "", by: str = "box-agent") -> dict:
        """Append an IMMUTABLE acceptance run + chained per-object proofs.

        Carries manifest generations, job id and a per-box acceptance seq.
        Written runs are never modified later; only new runs are appended, so
        an older repair generation can never rewrite a newer-generation proof.
        """
        box = s["boxes"][t["box_id"]]
        t["accept_seq"] += 1
        seq = t["accept_seq"]
        run_id = self._next(s, "run", "AR")
        prior_ids = s["acceptance_by_box"].setdefault(t["box_id"], [])
        parent_digest = s["acceptances"][prior_ids[-1]]["proof_digest"] \
            if prior_ids else "sha256:0"
        n_ok = sum(1 for o in objects if o["result"] in ("ok", "reused"))
        n_bad = len(objects) - n_ok
        good_idx = sorted(int(k) for k, v in chunk_results.items()
                          if v["result"] in ("ok", "reused"))
        run = {
            "run_id": run_id, "accept_seq": seq,
            "task_id": t["task_id"], "job_id": t["job_id"],
            "box_id": t["box_id"], "site_id": t["site_id"],
            "manifest_id": manifest["manifest_id"],
            "manifest_hash": manifest["manifest_hash"],
            "manifest_generations": dict(manifest["generations"]),
            "version": manifest["source"]["version"],
            "outcome": outcome,                  # proven | quarantined
            "by": by, "note": note, "at": s["clock"],
            "objects": copy.deepcopy(objects),
            "chunks": copy.deepcopy(chunk_results),
            "root": copy.deepcopy(root),
            "counts": {"objects_total": len(objects), "objects_ok": n_ok,
                       "objects_bad": n_bad,
                       "chunks_total": len(manifest["chunks"]),
                       "chunks_ok": len(good_idx)},
            "parent_digest": parent_digest}
        body = {"run_id": run_id, "accept_seq": seq, "task_id": t["task_id"],
                "job_id": t["job_id"], "box_id": t["box_id"],
                "manifest_hash": manifest["manifest_hash"],
                "manifest_generations": run["manifest_generations"],
                "outcome": outcome, "at": run["at"],
                "objects": [{"object_id": o["object_id"], "hash": o["hash"],
                             "length": o.get("length"), "result": o["result"],
                             "reason": o["reason"]} for o in objects],
                "chunks": chunk_results, "root": root,
                "parent_digest": parent_digest}
        run["proof_digest"] = sha256(body)
        # one immutable proof per object; proven objects chain to the run proof
        for o in objects:
            if o["result"] not in ("ok", "reused"):
                continue
            pid = self._next(s, "proof", "AP")
            olen = o.get("length")
            if olen is None:   # zero-transfer reuse: length from the manifest
                olen = self._authoritative_objects(manifest).get(
                    o["object_id"], {}).get("length")
            proof = {"proof_id": pid, "run_id": run_id,
                     "accept_seq": seq, "task_id": t["task_id"],
                     "job_id": t["job_id"], "box_id": t["box_id"],
                     "manifest_id": manifest["manifest_id"],
                     "manifest_hash": manifest["manifest_hash"],
                     "manifest_generations": dict(manifest["generations"]),
                     "object_id": o["object_id"], "object_hash": o["hash"],
                     "length": olen, "chunk_index": o["chunk_index"],
                     "chunk_hash": o.get("chunk_hash"),
                     "root_hash": root.get("declared"),
                     "result": o["result"], "at": run["at"],
                     "parent_digest": run["proof_digest"]}
            proof["proof_digest"] = sha256(proof)
            s["proofs"][pid] = proof
            o["proof_id"] = pid
        # persist only after per-object proof ids are backfilled (immutable)
        s["acceptances"][run_id] = run
        prior_ids.append(run_id)
        t["run_ids"].append(run_id)
        t["last_run_id"] = run_id
        return copy.deepcopy(run)

    def _evaluate_objects(self, s: dict, t: dict, manifest: dict):
        """Item-by-item box-side verification.

        Verifies, for every manifest object: presence on the box, compressed
        chunk hash (块散列), object content hash and canonical LENGTH, and
        finally the whole-package ROOT hash over the chunks that verify.
        Returns (objects, chunk_results, root, bad_indices, bad_objs).
        """
        truth = self._authoritative_objects(manifest)
        proven = self._proven_hashes(s["boxes"][t["box_id"]])
        chunk_bad: dict[int, str] = {}        # index -> reason code
        chunk_objs: dict[int, list] = {}
        local = t.get("chunks_local", {})
        for c in manifest["chunks"]:
            idx = c["index"]
            objs = [o for o in manifest["objects"]
                    if o["object_id"] in c["objects"]]
            chunk_objs[idx] = objs
            b64 = local.get(str(idx))
            if b64 is None:
                # never delivered (missing block) — unless all objs reused
                if any(o["hash"] not in proven for o in objs):
                    chunk_bad[idx] = "ACCEPT_CHUNK_MISSING"
                continue
            blob = _b64d(b64)
            if sha256_bytes(blob) != c["hash"]:
                chunk_bad[idx] = "ACCEPT_CHUNK_HASH"
        objects = []
        for o in manifest["objects"]:
            idx = next((c["index"] for c in manifest["chunks"]
                        if o["object_id"] in c["objects"]), None)
            rec = {"object_id": o["object_id"], "kind": o["kind"],
                   "hash": o["hash"], "length": None,
                   "chunk_index": idx, "chunk_hash": None,
                   "result": "bad", "reason": "ACCEPT_OBJECT_HASH"}
            if o["hash"] in proven:
                rec.update({"result": "reused", "reason": "ACCEPT_OK",
                            "length": truth.get(o["object_id"], {}).get("length")})
                objects.append(rec)
                continue
            b64 = local.get(str(idx))
            payload = self._decode_blob(_b64d(b64)) if b64 is not None else None
            local_obj = None
            if payload:
                local_obj = next((x for x in payload.get("objects", [])
                                  if x["object_id"] == o["object_id"]), None)
            chunk_decl = next(c["hash"] for c in manifest["chunks"]
                              if c["index"] == idx)
            rec["chunk_hash"] = chunk_decl
            # item-level checks still run on decodable blobs, so a flipped
            # object reports OBJECT_HASH/LENGTH rather than just CHUNK_HASH;
            # undecodable / missing bytes fall back to the chunk-level reason.
            if local_obj is not None:
                length = len(canonical(local_obj.get("content")))
                rec["length"] = length
                want_len = truth.get(o["object_id"], {}).get("length")
                if want_len is not None and length != want_len:
                    rec["reason"] = "ACCEPT_OBJECT_LENGTH"
                elif sha256(local_obj.get("content")) != o["hash"]:
                    rec["reason"] = "ACCEPT_OBJECT_HASH"
                else:
                    rec["result"] = "ok"
                    rec["reason"] = "ACCEPT_OK"
            elif idx in chunk_bad:
                rec["reason"] = chunk_bad[idx]
            else:
                rec["reason"] = "ACCEPT_OBJECT_HASH"
            objects.append(rec)
        bad_indices = sorted({o["chunk_index"] for o in objects
                              if o["result"] == "bad"
                              and o["chunk_index"] is not None})
        bad_objs = [o for o in objects if o["result"] == "bad"]
        # chunk-level results for the run record
        chunk_results = {}
        for c in manifest["chunks"]:
            idx = c["index"]
            if all(o["hash"] in proven for o in chunk_objs[idx]):
                chunk_results[str(idx)] = {"result": "reused",
                                           "reason": "ACCEPT_OK",
                                           "hash": c["hash"]}
            elif idx in chunk_bad:
                code = chunk_bad[idx]
                present = str(idx) in local
                chunk_results[str(idx)] = {
                    "result": "bad", "reason": code,
                    "declared_hash": c["hash"],
                    "computed_hash": (sha256_bytes(_b64d(local[str(idx)]))
                                      if present and code != "ACCEPT_CHUNK_MISSING"
                                      else None)}
            else:
                chunk_results[str(idx)] = {"result": "ok",
                                           "reason": "ACCEPT_OK",
                                           "hash": c["hash"]}
        # root hash over chunks in their declared order; bad chunks excluded
        good_hashes = [c["hash"] for c in manifest["chunks"]
                       if chunk_results[str(c["index"])]["result"]
                       in ("ok", "reused")]
        computed_root = sha256({"chunks": good_hashes})
        root = {"declared": manifest["root_hash"],
                "computed": computed_root,
                "match": (computed_root == manifest["root_hash"]
                          and not bad_indices)}
        return objects, chunk_results, root, bad_indices, bad_objs

    def _quarantine_locked(self, s: dict, t: dict, manifest: dict,
                           objects: list, chunk_results: dict, root: dict,
                           bad_indices: list, bad_objs: list) -> dict:
        """Isolate ONLY affected objects/chunks; release their space once.

        A bad chunk still carrying a reused (already proven) object keeps its
        disk hold — that object remains live on the box; only chunks whose
        objects are ALL unproven can be freed (and they are the repair scope).
        """
        box = s["boxes"][t["box_id"]]
        proven = self._proven_hashes(box)
        releasable, kept_with_reused = [], []
        for idx in bad_indices:
            c = next(c for c in manifest["chunks"] if c["index"] == idx)
            objs = [o for o in manifest["objects"] if o["object_id"] in c["objects"]]
            (kept_with_reused if any(o["hash"] in proven for o in objs)
             else releasable).append(idx)
        released = self._release_holds_locked(s, box, manifest, releasable)
        for idx in releasable:
            t.get("chunks_local", {}).pop(str(idx), None)
        records = []
        reasons = sorted({o["reason"] for o in bad_objs})
        for o in bad_objs:
            records.append({
                "object_id": o["object_id"], "object_hash": o["hash"],
                "length": o["length"], "chunk_index": o["chunk_index"],
                "reason": o["reason"], "reason_message": ACCEPT_REASONS[o["reason"]],
                "at": s["clock"]})
        gen = t["accept_seq"]
        q = {"generation": gen, "run_id": t["last_run_id"],
             "objects": records, "chunk_indices": bad_indices,
             "released_chunk_indices": releasable,
             "retained_chunk_indices": kept_with_reused,
             "reasons": reasons,
             "released_bytes": round(released, 1),
             "opened_at": s["clock"], "resolved_at": None,
             "repair_id": None}
        t["quarantined"] = records
        t.setdefault("quarantine_history", []).append(q)
        t["accept_stage"] = ACCEPT_QUARANTINED
        t["status"] = ACCEPT_QUARANTINED
        t["active_repair_id"] = None
        self._journal(
            s, t, "OBJECTS_QUARANTINED",
            f"验收发现 {len(records)} 个对象损坏/缺失（块 {bad_indices}，"
            f"原因 {'/'.join(reasons)}）：仅隔离受影响对象并释放其空间 "
            f"{released:.0f} 字节；其他 {sum(1 for o in objects if o['result'] in ('ok', 'reused'))} "
            f"个对象证明保留，不重传")
        self._event_locked(
            s, "OBJECTS_QUARANTINED",
            f"任务 {t['task_id']}（{t['box_id']}）隔离 {len(records)} 个对象："
            f"块 {bad_indices}，释放 {released:.0f} 字节，根散列一致="
            f"{root['match']}；已证明对象保留", t["task_id"])
        return q

    def accept_task(self, task_id: str, by: str = "box-agent") -> dict:
        """Run one box-end acceptance pass (idempotency only at terminal proof).

        Delivered/quarantined content is verified object by object; a fresh
        run is appended every time. Clean runs produce the immutable proof and
        admit objects to the switchable set; bad runs quarantine and create
        exactly one repair over the remaining chunks.
        """
        with self.lock:
            t0 = self.state["tasks"].get(task_id)
            if not t0 or t0.get("kind", "transfer") != "transfer":
                raise DomainError("PREHEAT_TASK_UNKNOWN",
                                  f"传输任务 {task_id} 不存在", 404)
            if t0["status"] == "done":
                return {"run": self._run_view(self.state, t0["last_run_id"]),
                        "task": self._task_view(self.state, t0),
                        "idempotent": True}
            if t0["status"] in ("queued", "transferring", "paused"):
                raise DomainError(
                    "ACCEPT_NOT_DELIVERED",
                    f"任务 {task_id} 尚未传输完成（{t0['status']}），"
                    f"盒端验收只能在作业传输完成后进行", 409)
            if t0["status"] == "canceled":
                raise DomainError("PREHEAT_TASK_CLOSED",
                                  f"任务 {task_id} 已取消，不能验收", 409)
            self._manifest(t0["manifest_id"])  # fail early if manifest vanished

        def mutate(s):
            t = s["tasks"][task_id]
            manifest = self._manifest_locked(s, t)
            objects, chunk_results, root, bad_indices, bad_objs = \
                self._evaluate_objects(s, t, manifest)
            outcome = "proven" if not bad_objs and root["match"] else "quarantined"
            reason_codes = {o["reason"] for o in bad_objs}
            if not root["match"]:
                reason_codes.add("ACCEPT_ROOT_HASH")
            run = self._append_run(
                s, t, manifest, outcome=outcome, objects=objects,
                chunk_results=chunk_results, root=root, by=by,
                note=ACCEPT_REASONS["ACCEPT_OK"] if outcome == "proven"
                else "；".join(ACCEPT_REASONS[r]
                               for r in sorted(reason_codes)))
            repair = None
            if outcome == "proven":
                self._finalize_proven(s, t, manifest, run)
            else:
                self._quarantine_locked(s, t, manifest, objects, chunk_results,
                                        root, bad_indices, bad_objs)
                repair = self._ensure_repair(s, t, manifest)
            return {"run": copy.deepcopy(run),
                    "repair": copy.deepcopy(repair) if repair else None}

        out = self.commit(mutate)
        with self.lock:
            t = self.state["tasks"][task_id]
            return {"run": self._run_view(self.state, out["run"]["run_id"]),
                    "task": self._task_view(self.state, t),
                    "repair": out["repair"], "idempotent": False}

    def _manifest_locked(self, s: dict, t: dict) -> dict:
        if self.mirror is None:
            raise DomainError("PREHEAT_MIRROR_UNAVAILABLE",
                              "镜像仓库不可用，无法读取内容清单", 409)
        with self.mirror.lock:
            man = self.mirror.state.get("manifests", {}).get(t["manifest_id"])
            if man is None:
                raise DomainError("PREHEAT_MANIFEST_UNKNOWN",
                                  f"清单 {t['manifest_id']} 已不在镜像仓库", 409)
            return copy.deepcopy(man)

    # ------------------------------------------------------------- repairs
    @staticmethod
    def _repair_chunks(manifest: dict, quarantined: list) -> list:
        """Chunks a repair must re-pull: chunks of the remaining bad objects.

        Proven objects are never retransferred — only chunks still containing
        quarantined objects enter the repair, and the whole chunk is pulled
        once (chunks are the compression/hash unit).
        """
        return sorted({c["index"] for c in manifest["chunks"]
                       for q in quarantined
                       if q["object_id"] in c["objects"]})

    def _ensure_repair(self, s: dict, t: dict, manifest: dict) -> dict:
        """Create the single repair for (task, quarantine generation).

        Same key submitted concurrently returns the FIRST repair task. The key
        binds the quarantine generation and the exact remaining chunk set.
        """
        qrec = t["quarantine_history"][-1]
        chunks = self._repair_chunks(manifest, t["quarantined"])
        qrec["chunk_indices"] = chunks
        key = f"{t['task_id']}|gen{t['accept_seq']}|chunks{','.join(map(str, chunks))}"
        existing = s["repair_by_key"].get(key)
        if existing:
            r = s["tasks"][existing]
            if r["status"] in ("queued", "transferring", "paused"):
                t["active_repair_id"] = existing
                qrec["repair_id"] = existing
                return self._repair_view(s, r, idempotent=True)
        rid = self._next(s, "repair", "PR")
        s["seq"]["task"] += 1   # repairs share the global task ordering counter
        size = sum(self._chunk_size(manifest, i) for i in chunks)
        r = {"task_id": rid, "seq": s["seq"]["task"], "kind": "repair",
             "job_id": t["job_id"], "box_id": t["box_id"], "site_id": t["site_id"],
             "parent_task_id": t["task_id"], "repair_key": key,
             "repair_generation": t["accept_seq"],
             "attempt": 1,
             "manifest_id": manifest["manifest_id"],
             "manifest_hash": manifest["manifest_hash"],
             "chunk_indices": chunks,
             "status": "queued", "bytes_total": size, "bytes_done": 0.0,
             "reserved_bytes": 0.0, "debt_kb": 0.0,
             "result": None, "journal": [],
             "created_at": s["clock"], "updated_at": s["clock"],
             "done_at": None}
        # if a same-key repair existed but was canceled, this is a fresh attempt
        if existing:
            old = s["tasks"][existing]
            r["attempt"] = old.get("attempt", 1) + 1
        s["tasks"][rid] = r
        s["repair_by_key"][key] = rid
        t["active_repair_id"] = rid
        qrec["repair_id"] = rid
        self._journal(s, r, "REPAIR_CREATED",
                      f"按剩余块创建一次修复：重拉块 {chunks}（{size:.0f} 字节）；"
                      f"已证明对象不重传。修复键 {short(sha256({'k': key}))}")
        self._event_locked(
            s, "REPAIR_CREATED",
            f"任务 {t['task_id']}（{t['box_id']}）创建修复 {rid}："
            f"仅重拉 {len(chunks)} 个块 {chunks}，同键并发返回首次任务", rid)
        return self._repair_view(s, r, idempotent=False)

    def create_repair(self, task_id: str) -> dict:
        """Public (re-)submit: create/return the one repair for a quarantine."""
        with self.lock:
            t0 = self.state["tasks"].get(task_id)
            if not t0 or t0.get("kind", "transfer") != "transfer":
                raise DomainError("PREHEAT_TASK_UNKNOWN",
                                  f"传输任务 {task_id} 不存在", 404)
            if t0["status"] == "done":
                raise DomainError("REPAIR_NOT_NEEDED",
                                  f"任务 {task_id} 已证明，无需修复", 409)
            if t0["status"] == "canceled":
                raise DomainError("PREHEAT_TASK_CLOSED",
                                  f"任务 {task_id} 已取消", 409)
            if t0["accept_stage"] != ACCEPT_QUARANTINED:
                raise DomainError("REPAIR_NOT_QUARANTINED",
                                  f"任务 {task_id} 当前未隔离，先执行盒端验收", 409)
            rid0 = t0.get("active_repair_id")
            if rid0 and self.state["tasks"][rid0]["status"] in \
                    ("queued", "transferring", "paused"):
                return self._repair_view(self.state, self.state["tasks"][rid0],
                                         idempotent=True)

        def mutate(s):
            t = s["tasks"][task_id]
            manifest = self._manifest_locked(s, t)
            view = self._ensure_repair(s, t, manifest)
            return copy.deepcopy(view)

        view = self.commit(mutate)
        return view

    def cancel_repair(self, repair_id: str) -> dict:
        """Cancel a repair: pairs released, quarantine converges, no leak."""
        with self.lock:
            r0 = self.state["tasks"].get(repair_id)
            if not r0 or r0.get("kind") != "repair":
                raise DomainError("REPAIR_UNKNOWN", f"修复 {repair_id} 不存在", 404)
            if r0["status"] in ("canceled", "superseded", "done"):
                return {"repair": self._repair_view(self.state, r0),
                        "idempotent": True}

        def mutate(s):
            r = s["tasks"][repair_id]
            released_pair = r["reserved_bytes"] > EPS
            r["status"] = "canceled"
            r["reserved_bytes"] = 0.0
            r["result"] = {"outcome": "canceled", "at": s["clock"]}
            r["updated_at"] = s["clock"]
            parent = s["tasks"][r["parent_task_id"]]
            if parent.get("active_repair_id") == repair_id:
                parent["active_repair_id"] = None
            self._journal(s, r, "REPAIR_CANCELED",
                          "修复取消：预留与名额成对释放，隔离对象保持隔离，"
                          "可按同键重新提交修复（确定性新建一次尝试）")
            self._event_locked(
                s, "REPAIR_CANCELED",
                f"修复 {repair_id} 已取消（父任务 {r['parent_task_id']}），"
                f"无容量残留；隔离状态保留待重新修复", repair_id)
            return {"released_pair": released_pair}

        self.commit(mutate)
        with self.lock:
            return {"repair": self._repair_view(self.state,
                                                 self.state["tasks"][repair_id]),
                    "idempotent": False}

    def _complete_repair(self, s: dict, r: dict):
        """Repair bytes delivered: re-fetch chunks and converge by generation.

        Old-generation repairs never overwrite newer-generation proofs: if the
        parent moved on, the repair is marked 'superseded' and touches nothing.
        """
        parent = s["tasks"][r["parent_task_id"]]
        manifest = self._manifest_locked(s, r)
        stale = (parent["accept_stage"] != ACCEPT_QUARANTINED
                 or r["repair_generation"] != parent["accept_seq"])
        r["bytes_done"] = r["bytes_total"]
        r["reserved_bytes"] = 0.0
        r["updated_at"] = s["clock"]
        if stale:
            r["status"] = "superseded"
            r["result"] = {"outcome": "superseded",
                           "repair_generation": r["repair_generation"],
                           "current_generation": parent["accept_seq"],
                           "at": s["clock"]}
            r["done_at"] = s["clock"]
            if parent.get("active_repair_id") == r["task_id"]:
                parent["active_repair_id"] = None
            self._journal(
                s, r, "REPAIR_SUPERSEDED",
                f"修复针对第 {r['repair_generation']} 代隔离，父任务已到第 "
                f"{parent['accept_seq']} 代（{parent['accept_stage']}）："
                f"旧代次修复不覆盖新代次证明，结果确定失效且不占容量")
            self._event_locked(s, "REPAIR_SUPERSEDED",
                               f"修复 {r['task_id']} 旧代次失效，不覆盖新证明",
                               r["task_id"])
            return
        box = s["boxes"][r["box_id"]]
        applied, blocked = [], []
        for idx in r["chunk_indices"]:
            # never overwrite a chunk a NEWER proof already verified
            already_ok = str(idx) in parent.get("chunks_local", {})
            blob = self._mirror_blob(manifest["manifest_hash"], idx)
            cmeta = next(c for c in manifest["chunks"] if c["index"] == idx)
            if blob is None or sha256_bytes(blob) != cmeta["hash"]:
                blocked.append(idx)
                continue
            if not already_ok:
                parent["chunks_local"][str(idx)] = _b64e(blob)
                self._hold_box_locked(s, box, manifest, [idx])
            applied.append(idx)
        r["done_at"] = s["clock"]
        if blocked:
            # repository cannot serve a good chunk yet: repair converges to a
            # recorded blocked result; quarantine stays; no reservation leaks
            r["status"] = "done"
            r["result"] = {"outcome": "blocked_upstream",
                           "blocked_chunks": blocked,
                           "applied_chunks": applied, "at": s["clock"]}
            parent["active_repair_id"] = None
            self._journal(
                s, r, "REPAIR_BLOCKED",
                f"修复完成但上游块 {blocked} 暂不可用/损坏；已应用块 {applied}，"
                f"隔离保持，预留全部释放，待仓库恢复后重新提交修复")
            self._event_locked(s, "REPAIR_BLOCKED",
                               f"修复 {r['task_id']} 受上游阻塞：{blocked}", r["task_id"])
            return
        r["status"] = "done"
        r["result"] = {"outcome": "delivered", "applied_chunks": applied,
                       "at": s["clock"]}
        self._journal(
            s, r, "REPAIR_DELIVERED",
            f"修复块 {applied} 已到盒（{r['bytes_total']:.0f} 字节），"
            f"成对释放；自动执行第 {parent['accept_seq'] + 1} 次验收")
        self._event_locked(
            s, "REPAIR_DELIVERED",
            f"修复 {r['task_id']} 交付完成，触发父任务 {r['parent_task_id']} 重新验收",
            r["task_id"])
        # automatic re-acceptance over the remaining quarantined objects
        objects, chunk_results, root, bad_indices, bad_objs = \
            self._evaluate_objects(s, parent, manifest)
        outcome = "proven" if not bad_objs and root["match"] else "quarantined"
        run = self._append_run(
            s, parent, manifest, outcome=outcome, objects=objects,
            chunk_results=chunk_results, root=root, by="repair-agent",
            note=ACCEPT_REASONS["ACCEPT_OK"] if outcome == "proven"
            else "修复后仍有损坏对象，保持隔离")
        r["result"]["run_id"] = run["run_id"]
        if outcome == "proven":
            qh = parent["quarantine_history"][-1]
            qh["resolved_at"] = s["clock"]
            qh["resolution"] = "repaired"
            qh["resolution_run_id"] = run["run_id"]
            self._finalize_proven(s, parent, manifest, run)
        else:
            self._quarantine_locked(s, parent, manifest, objects, chunk_results,
                                    root, bad_indices, bad_objs)
            # do NOT auto-create another repair in the same commit; operator (or
            # next tick-driven policy) submits one — convergence is bounded.

    # ------------------------------------------------- corruption simulator
    def simulate_corruption(self, task_id: str, chunk: Optional[int] = None,
                            object_id: Optional[str] = None,
                            mode: str = "chunk") -> dict:
        """Demonstrate box-side defects on delivered content (simulation only).

        mode=chunk: flip compressed chunk bytes (块散列不符) at one chunk;
        mode=object: flip one object's content (对象散列/长度不符);
        mode=length: truncate one object's content (长度不符);
        mode=missing: drop a delivered chunk from the box entirely (缺块).
        The mirror repository, the manifest and the effective map are never
        touched — only this box-local staging copy changes.
        """
        with self.lock:
            t = self.state["tasks"].get(task_id)
            if not t or t.get("kind", "transfer") != "transfer":
                raise DomainError("PREHEAT_TASK_UNKNOWN",
                                  f"传输任务 {task_id} 不存在", 404)
            if t["status"] not in ACCEPT_STAGES:
                raise DomainError("ACCEPT_NOT_DELIVERED",
                                  f"任务 {task_id} 尚无已到盒内容可注入缺陷（{t['status']}）",
                                  409)
            manifest = self._manifest(t["manifest_id"])

        def mutate(s):
            t = s["tasks"][task_id]
            local = t["chunks_local"]
            if mode == "missing":
                idx = chunk if chunk is not None else \
                    int(sorted(local, key=int)[-1])
                if str(idx) not in local:
                    raise DomainError("ACCEPT_CHUNK_MISSING",
                                      f"盒上没有块 {idx}", 409)
                local.pop(str(idx))
                what = f"移除盒上块 {idx}（模拟缺块）"
            elif mode == "chunk":
                idx = chunk if chunk is not None else \
                    int(sorted(local, key=int)[-1])
                if str(idx) not in local:
                    raise DomainError("ACCEPT_CHUNK_MISSING",
                                      f"盒上没有块 {idx}", 409)
                blob = _b64d(local[str(idx)])
                local[str(idx)] = _b64e(blob[:-1] + bytes([blob[-1] ^ 0xA5]))
                what = f"翻转块 {idx} 压缩字节（模拟块损坏）"
            else:
                oid = object_id
                if oid is None and chunk is None:
                    raise DomainError("BAD_REQUEST",
                                      "object/chunk 模式需指定 object_id 或 chunk", 400)
                idx = chunk
                if idx is None:
                    c = next((c for c in manifest["chunks"]
                              if oid in c["objects"]), None)
                    if c is None:
                        raise DomainError("BAD_REQUEST",
                                          f"对象 {oid} 不在清单中", 400)
                    idx = c["index"]
                b64 = local.get(str(idx))
                if b64 is None:
                    raise DomainError("ACCEPT_CHUNK_MISSING",
                                      f"盒上没有块 {idx}（可能已隔离释放）", 409)
                payload = self._decode_blob(_b64d(b64))
                if payload is None:
                    raise DomainError("ACCEPT_CHUNK_HASH",
                                      f"块 {idx} 已损坏，无法定位对象", 409)
                target = next((o for o in payload["objects"]
                               if oid is None or o["object_id"] == oid), None)
                if target is None:
                    raise DomainError("BAD_REQUEST", f"块 {idx} 中无对象 {oid}", 400)
                content = canonical(target["content"])
                if mode == "length":
                    # JSON-safe shrink: the canonical LENGTH changes, so the
                    # box reports ACCEPT_OBJECT_LENGTH
                    target["content"] = {"__corrupt_length__":
                                         content.decode("utf-8", "replace")[:40]}
                else:
                    # JSON-safe, length-preserving corruption: flip one hex
                    # character inside an existing long string field (e.g. a
                    # tile hash in the descriptor) — same length, different
                    # object content hash => ACCEPT_OBJECT_HASH, not LENGTH
                    inner = target["content"]
                    flipped = False
                    if isinstance(inner, dict):
                        def flip(d):
                            for k, v in d.items():
                                if isinstance(v, str) and len(v) >= 8 \
                                        and any(ch in "0123456789abcdef" for ch in v):
                                    i = next(i for i, ch in enumerate(v)
                                             if ch in "0123456789abcdef")
                                    d[k] = v[:i] + \
                                        ("f" if v[i] != "f" else "0") + v[i + 1:]
                                    return True
                                if isinstance(v, dict) and flip(v):
                                    return True
                            return False
                        flipped = flip(inner)
                    if not flipped:
                        raise DomainError("BAD_REQUEST",
                                          f"对象 {target['object_id']} 无可翻转字段",
                                          400)
                blob = zlib.compress(canonical(payload), 9)
                local[str(idx)] = _b64e(blob)
                what = (f"截断对象 {target['object_id']}（模拟长度不符）"
                        if mode == "length"
                        else f"翻转对象 {target['object_id']} 内容（模拟对象损坏）")
            t["updated_at"] = s["clock"]
            self._journal(s, t, "DEFECT_INJECTED", what + "；镜像清单与已生效地图不变")
            self._event_locked(s, "DEFECT_INJECTED",
                               f"任务 {task_id}（{t['box_id']}）{what}", task_id)
            return {"task_id": task_id, "mode": mode, "what": what,
                    "chunk": idx if "idx" in locals() else None}

        return self.commit(mutate)

    # ================================================================ views ==
    def _predict(self, s: dict) -> dict:
        """Deterministic ETA simulation: {task_id: seconds_from_now | None}.

        Replays the pure plan forward (debts frozen, current eligibility),
        finishing tasks one by one until nothing can progress.
        """
        remaining = {t["task_id"]: self._remaining(t)
                     for t in self._open_tasks(s) if self._remaining(t) > EPS}
        eta: dict[str, Optional[float]] = {}
        horizon = 0.0
        for _ in range(10000):
            if not remaining:
                break
            plan = self._plan(s, remaining)
            active = {tid: p["grant_kbps"] for tid, p in plan.items()
                      if tid in remaining and p["grant_kbps"] > EPS}
            if not active:
                break
            dt = min(remaining[tid] / (g * KB) for tid, g in active.items())
            horizon += dt
            for tid, g in active.items():
                remaining[tid] -= g * KB * dt
                if remaining[tid] <= EPS:
                    eta[tid] = horizon
                    del remaining[tid]
        for tid in [t["task_id"] for t in self._open_tasks(s)]:
            eta.setdefault(tid, None)
        return eta

    def _views(self, s: dict):
        """Compute plan + ETA + ranks once, consistently for all views."""
        plan = self._plan(s)
        eta = self._predict(s)
        ranks = {}
        for i, t in enumerate(sorted(self._open_tasks(s), key=self._priority(s))):
            ranks[t["task_id"]] = i + 1
        return plan, eta, ranks

    def _task_view(self, s: dict, t: dict, plan=None, eta=None, ranks=None) -> dict:
        job = s["jobs"][t["job_id"]]
        if t.get("kind") == "repair":
            return self._repair_view(s, t, plan=plan, eta=eta, ranks=ranks)
        v = {"task_id": t["task_id"], "job_id": t["job_id"], "box_id": t["box_id"],
             "site_id": t["site_id"], "kind": "transfer",
             "manifest_id": t["manifest_id"],
             "status": t["status"], "accept_stage": t.get("accept_stage"),
             "accept_seq": t.get("accept_seq", 0),
             "last_run_id": t.get("last_run_id"),
             "bytes_total": round(t["bytes_total"], 1),
             "bytes_done": round(t["bytes_done"], 1),
             "remaining_bytes": round(self._remaining(t), 1),
             "reserved_bytes": round(t["reserved_bytes"], 1),
             "debt_kb": round(t["debt_kb"], 3),
             "reuse_ratio": round(t["reuse_ratio"], 4),
             "needed_chunks": list(t.get("needed_chunks", [])),
             "local_chunks": sorted(int(k) for k in t.get("chunks_local", {})),
             "quarantine": self._quarantine_view(s, t),
             "active_repair": (self._repair_view(
                 s, s["tasks"][t["active_repair_id"]])
                 if t.get("active_repair_id")
                 and t["active_repair_id"] in s["tasks"] else None),
             "runs": list(t.get("run_ids", [])),
             "progress": (round(t["bytes_done"] / t["bytes_total"], 4)
                          if t["bytes_total"] > EPS else 1.0),
             "journal": copy.deepcopy(t["journal"]),
             "created_at": t["created_at"], "updated_at": t["updated_at"],
             "done_at": t["done_at"]}
        p = (plan or {}).get(t["task_id"])
        if t["status"] == "paused":
            v["rate_kbps"] = 0.0
            v["limit"] = {"code": "PAUSED", "message": REASONS["PAUSED"]}
        elif t["status"] in TASK_OPEN and p is not None:
            # transferring ⇔ holds the (disk, slot) pair ⇔ granted rate > 0
            v["rate_kbps"] = p["grant_kbps"] if t["status"] == "transferring" else 0.0
            v["limit"] = {"code": p["code"], "message": p["message"]}
        else:
            v["rate_kbps"] = 0.0
            v["limit"] = None
        if ranks is not None and t["task_id"] in ranks:
            v["rank"] = ranks[t["task_id"]]
        secs = (eta or {}).get(t["task_id"])
        if t["status"] == "done":
            v["eta_seconds"] = 0.0
            v["eta"] = t["done_at"]
        elif secs is None:
            v["eta_seconds"] = None
            v["eta"] = None
        else:
            v["eta_seconds"] = _round6(secs)
            v["eta"] = _iso(self._clock(s) + timedelta(seconds=secs))
        if t["status"] in TASK_OPEN + ("paused",):
            v["at_risk"] = bool(v["eta"] and v["eta"] > job["deadline"])
        else:
            v["at_risk"] = False
        return v

    def _quarantine_view(self, s: dict, t: dict) -> Optional[dict]:
        if not t.get("quarantine_history"):
            return None
        q = t["quarantine_history"][-1]
        return {"generation": q["generation"], "run_id": q["run_id"],
                "objects": copy.deepcopy(t.get("quarantined", [])),
                "object_count": len(t.get("quarantined", [])),
                "chunk_indices": list(q.get("chunk_indices", [])),
                "reasons": list(q.get("reasons", [])),
                "released_bytes": q.get("released_bytes", 0.0),
                "repair_id": q.get("repair_id"),
                "opened_at": q.get("opened_at"),
                "resolved_at": q.get("resolved_at"),
                "resolution": q.get("resolution")}

    def _repair_view(self, s: dict, r: dict, idempotent: bool = False,
                     plan=None, eta=None, ranks=None) -> dict:
        p = (plan or {}).get(r["task_id"])
        v = {"task_id": r["task_id"], "kind": "repair",
             "job_id": r["job_id"], "box_id": r["box_id"], "site_id": r["site_id"],
             "parent_task_id": r["parent_task_id"],
             "repair_key": r["repair_key"],
             "repair_generation": r["repair_generation"],
             "attempt": r.get("attempt", 1),
             "chunk_indices": list(r.get("chunk_indices", [])),
             "status": r["status"],
             "bytes_total": round(r["bytes_total"], 1),
             "bytes_done": round(r["bytes_done"], 1),
             "remaining_bytes": round(self._remaining(r), 1),
             "reserved_bytes": round(r["reserved_bytes"], 1),
             "debt_kb": round(r.get("debt_kb", 0.0), 3),
             "result": copy.deepcopy(r.get("result")),
             "progress": (round(r["bytes_done"] / r["bytes_total"], 4)
                          if r["bytes_total"] > EPS else 1.0),
             "rate_kbps": (p["grant_kbps"] if p and r["status"] == "transferring"
                           else 0.0),
             "limit": ({"code": p["code"], "message": p["message"]}
                       if p and r["status"] in TASK_OPEN else None),
             "journal": copy.deepcopy(r["journal"]),
             "created_at": r["created_at"], "updated_at": r["updated_at"],
             "done_at": r.get("done_at"), "idempotent": idempotent}
        if ranks is not None and r["task_id"] in ranks:
            v["rank"] = ranks[r["task_id"]]
        secs = (eta or {}).get(r["task_id"])
        v["eta_seconds"] = None if secs is None else _round6(secs)
        return v

    def _job_view(self, s: dict, job: dict, idempotent: bool = False) -> dict:
        plan, eta, ranks = self._views(s)
        tasks = [self._task_view(s, s["tasks"][tid], plan, eta, ranks)
                 for tid in job.get("tasks", [])]
        done = sum(1 for t in tasks if t["status"] == "done")
        pending = sum(1 for t in tasks
                      if t["status"] in ("queued", "transferring", "paused"))
        awaiting = sum(1 for t in tasks if t["status"] == ACCEPT_PENDING)
        quarantined = sum(1 for t in tasks if t["status"] == ACCEPT_QUARANTINED)
        repairs = [self._repair_view(s, r, plan, eta, ranks)
                   for r in s["tasks"].values()
                   if r.get("kind") == "repair" and r["job_id"] == job["job_id"]]
        repairs.sort(key=lambda r: r["task_id"])
        repairing = sum(1 for r in repairs
                        if r["status"] in ("queued", "transferring", "paused"))
        total_b = sum(t["bytes_total"] for t in tasks)
        done_b = sum(t["bytes_done"] for t in tasks if t["status"] == "done")
        by_deadline = done + sum(
            1 for t in tasks if t["status"] in TASK_OPEN + ("paused",)
            and t["eta"] is not None and t["eta"] <= job["deadline"])
        v = {"job_id": job["job_id"], "manifest_id": job["manifest_id"],
             "version": job["version"], "targets": list(job["targets"]),
             "deadline": job["deadline"], "link_cap_kbps": job["link_cap_kbps"],
             "min_online": job["min_online"], "note": job["note"],
             "status": job["status"], "created_by": job["created_by"],
             "created_at": job["created_at"], "updated_at": job["updated_at"],
             "progress": {"done_tasks": done, "total_tasks": len(tasks),
                          "transferring_tasks": pending,
                          "awaiting_acceptance": awaiting,
                          "quarantined_tasks": quarantined,
                          "repairing_tasks": repairing,
                          "done_bytes": round(done_b, 1),
                          "total_bytes": round(total_b, 1)},
             "guarantee": {"required": job["min_online"], "done": done,
                           "predicted_by_deadline": by_deadline,
                           "at_risk": by_deadline < job["min_online"]},
             "tasks": tasks, "repairs": repairs, "idempotent": idempotent}
        if job["cancel_result"]:
            v["cancel_result"] = copy.deepcopy(job["cancel_result"])
        return v

    def queue(self) -> dict:
        """The scheduling queue: identical order/reasons/ETAs for web and CLI."""
        with self.lock:
            s = self.state
            plan, eta, ranks = self._views(s)
            open_tasks = sorted(self._open_tasks(s), key=self._priority(s))
            resting = sorted(
                (t for t in s["tasks"].values()
                 if t["status"] in ("paused", "done", "canceled", "superseded")
                 or (t.get("kind") == "transfer"
                     and t["status"] in ACCEPT_STAGES)),
                key=lambda t: t["seq"], reverse=True)
            site_views = []
            for site in sorted(s["sites"].values(), key=lambda x: x["site_id"]):
                tids = [t["task_id"] for t in self._open_tasks(s)
                        if t["site_id"] == site["site_id"]]
                used = sum(plan.get(tid, {}).get("grant_kbps", 0.0) for tid in tids)
                site_views.append({
                    "site_id": site["site_id"],
                    "capacity_kbps": site["capacity_kbps"],
                    "granted_kbps": _round6(used),
                    "free_kbps": _round6(site["capacity_kbps"] - used),
                    "open_tasks": len(tids)})
            return {"clock": s["clock"],
                    "sites": site_views,
                    "queue": [self._task_view(s, t, plan, eta, ranks)
                              for t in open_tasks],
                    "inactive": [self._task_view(s, t, plan, eta, ranks)
                                 for t in resting]}

    def list_jobs(self) -> list:
        with self.lock:
            return [self._job_view(self.state, j)
                    for j in sorted(self.state["jobs"].values(),
                                    key=lambda j: j["seq"], reverse=True)]

    def get_job(self, job_id: str) -> dict:
        with self.lock:
            return self._job_view(self.state, self._get_job(self.state, job_id))

    def list_sites(self) -> list:
        with self.lock:
            plan = self._plan(self.state)
            out = []
            for site in sorted(self.state["sites"].values(),
                               key=lambda x: x["site_id"]):
                tids = [t["task_id"] for t in self._open_tasks(self.state)
                        if t["site_id"] == site["site_id"]]
                used = sum(plan.get(tid, {}).get("grant_kbps", 0.0) for tid in tids)
                boxes = [b for b in self.state["boxes"].values()
                         if b["site_id"] == site["site_id"]]
                out.append({"site_id": site["site_id"],
                            "capacity_kbps": site["capacity_kbps"],
                            "granted_kbps": _round6(used),
                            "boxes": len(boxes),
                            "online": sum(1 for b in boxes if b["online"]),
                            "created_at": site["created_at"]})
            return out

    def list_boxes(self) -> list:
        with self.lock:
            return [self._box_view(self.state, b)
                    for b in sorted(self.state["boxes"].values(),
                                    key=lambda b: b["box_id"])]

    def get_site(self, site_id: str) -> dict:
        with self.lock:
            site = self.state["sites"].get(site_id)
            if not site:
                raise DomainError("PREHEAT_SITE_UNKNOWN",
                                  f"站点 {site_id} 未注册", 404)
            return next(x for x in self.list_sites() if x["site_id"] == site_id)

    def get_box(self, box_id: str) -> dict:
        with self.lock:
            box = self.state["boxes"].get(box_id)
            if not box:
                raise DomainError("PREHEAT_BOX_UNKNOWN",
                                  f"盒子 {box_id} 未注册", 404)
            return self._box_view(self.state, box)

    def _box_view(self, s: dict, b: dict) -> dict:
        reserved = sum(t["reserved_bytes"] for t in s["tasks"].values()
                       if t["box_id"] == b["box_id"] and t["status"] == "transferring")
        v = copy.deepcopy(b)
        v.pop("proven", None)   # expose counts/switchable set, not the full map
        v["reserved_bytes"] = round(reserved, 1)
        v["free_bytes"] = round(b["disk_total_bytes"] - b["disk_used_bytes"]
                                - reserved, 1)
        v["disk_total_mb"] = _round6(b["disk_total_bytes"] / 1e6)
        v["disk_used_mb"] = _round6(b["disk_used_bytes"] / 1e6)
        v["reserved_mb"] = _round6(reserved / 1e6)
        v["proven_objects"] = len(b.get("proven", {}))
        v["inventory_objects"] = len(b.get("inventory", []))
        # switchable set: manifests whose every object is proven on this box
        switchable = []
        if self.mirror is not None:
            with self.mirror.lock:
                for man in self.mirror.state.get("manifests", {}).values():
                    hashes = {o["hash"] for o in man["objects"]}
                    if hashes and hashes <= self._proven_hashes(b):
                        switchable.append({
                            "manifest_id": man["manifest_id"],
                            "version": man["source"]["version"],
                            "root_hash": man["root_hash"],
                            "objects": len(hashes)})
        switchable.sort(key=lambda x: x["manifest_id"], reverse=True)
        v["switchable"] = switchable
        return v

    # ============================================ acceptance board / query ==
    def _run_view(self, s: dict, run_id: str) -> dict:
        run = s["acceptances"].get(run_id)
        if not run:
            raise DomainError("ACCEPT_RUN_UNKNOWN", f"验收记录 {run_id} 不存在", 404)
        out = copy.deepcopy(run)
        out["proofs"] = sorted(
            ({"proof_id": p["proof_id"], "object_id": p["object_id"],
              "object_hash": p["object_hash"], "length": p["length"],
              "chunk_index": p["chunk_index"], "proof_digest": p["proof_digest"]}
             for p in s["proofs"].values() if p["run_id"] == run_id),
            key=lambda x: x["proof_id"])
        return out

    def list_runs(self, box_id: Optional[str] = None,
                  task_id: Optional[str] = None, limit: int = 200) -> list:
        with self.lock:
            ids: list = []
            if task_id:
                t = self.state["tasks"].get(task_id)
                if not t:
                    raise DomainError("PREHEAT_TASK_UNKNOWN",
                                      f"任务 {task_id} 不存在", 404)
                ids = list(t.get("run_ids", []))
            elif box_id:
                if box_id not in self.state["boxes"]:
                    raise DomainError("PREHEAT_BOX_UNKNOWN",
                                      f"盒子 {box_id} 未注册", 404)
                ids = list(self.state["acceptance_by_box"].get(box_id, []))
            else:
                ids = sorted(self.state["acceptances"],
                             key=lambda r: self.state["acceptances"][r]["accept_seq"])
                ids = [r["run_id"] for r in sorted(
                    self.state["acceptances"].values(),
                    key=lambda r: (r["box_id"], r["accept_seq"]))]
            return [self._run_view(self.state, rid)
                    for rid in ids[-limit:][::-1]]

    def list_repairs(self, task_id: Optional[str] = None,
                     box_id: Optional[str] = None) -> list:
        with self.lock:
            out = []
            for r in self.state["tasks"].values():
                if r.get("kind") != "repair":
                    continue
                if task_id and r["parent_task_id"] != task_id:
                    continue
                if box_id and r["box_id"] != box_id:
                    continue
                out.append(self._repair_view(self.state, r))
            return sorted(out, key=lambda r: r["task_id"], reverse=True)

    def acceptance_board(self, box_id: Optional[str] = None) -> dict:
        """Single state behind web + CLI: pending / proven / quarantined / repairs."""
        with self.lock:
            s = self.state
            rows = []
            for t in self._transfer_tasks(s):
                if box_id and t["box_id"] != box_id:
                    continue
                tv = self._task_view(s, t)
                stage = ("proven" if t["status"] == "done"
                         else t["accept_stage"]
                         or ("transferring" if t["status"] in TASK_OPEN
                             else t["status"]))
                # external stage vocabulary for web/CLI board
                stage = {"transferred": "pending",
                         "proven": "proven",
                         "quarantined": "quarantined"}.get(stage, stage)
                rows.append({"task_id": t["task_id"], "job_id": t["job_id"],
                             "box_id": t["box_id"], "site_id": t["site_id"],
                             "manifest_id": t["manifest_id"],
                             "version": s["jobs"][t["job_id"]]["version"],
                             "stage": stage, "status": t["status"],
                             "accept_seq": t.get("accept_seq", 0),
                             "last_run_id": t.get("last_run_id"),
                             "quarantine": tv["quarantine"],
                             "active_repair": tv["active_repair"]})
            buckets = {"transferring": [], "pending": [], "proven": [],
                       "quarantined": [], "repairing": [], "canceled": []}
            for r in rows:
                if r["stage"] == "proven":
                    buckets["proven"].append(r)
                elif r["stage"] == "pending":
                    buckets["pending"].append(r)
                elif r["stage"] == "quarantined":
                    buckets["repairing" if r["active_repair"] and
                        r["active_repair"]["status"] in
                        ("queued", "transferring", "paused")
                        else "quarantined"].append(r)
                elif r["stage"] == "canceled":
                    buckets["canceled"].append(r)
                else:
                    buckets["transferring"].append(r)
            return {"clock": s["clock"], "counts": {k: len(v) for k, v in
                                                    buckets.items()},
                    **buckets}

    def acceptance_summary(self, site_id: Optional[str] = None,
                           manifest_id: Optional[str] = None) -> dict:
        """Batch rollup by site and manifest: pass rate / quarantine capacity.

        Every acceptance and repair result is traceable: runs and repairs are
        returned alongside the aggregates, grouped by site then manifest.
        """
        with self.lock:
            s = self.state
            groups: dict[tuple, dict] = {}

            def gkey(t):
                return (t["site_id"], t["manifest_id"])

            def g(t):
                k = gkey(t)
                grp = groups.setdefault(k, {
                    "site_id": t["site_id"], "manifest_id": t["manifest_id"],
                    "version": s["jobs"][t["job_id"]]["version"],
                    "tasks": 0, "proven": 0, "pending": 0, "quarantined": 0,
                    "canceled": 0,
                    "runs": 0, "objects_total": 0, "objects_ok": 0,
                    "objects_bad": 0, "quarantine_bytes": 0.0,
                    "repairs_open": 0, "repairs_done": 0,
                    "repairs_canceled": 0, "repairs_superseded": 0,
                    "failure_reasons": {}, "task_ids": []})
                return grp
            for t in self._transfer_tasks(s):
                if site_id and t["site_id"] != site_id:
                    continue
                if manifest_id and t["manifest_id"] != manifest_id:
                    continue
                grp = g(t)
                grp["tasks"] += 1
                grp["task_ids"].append(t["task_id"])
                if t["status"] == "done":
                    grp["proven"] += 1
                elif t["status"] == "canceled":
                    grp.setdefault("canceled", 0)
                    grp["canceled"] += 1
                elif t["accept_stage"] == ACCEPT_PENDING:
                    grp["pending"] += 1
                elif t["accept_stage"] == ACCEPT_QUARANTINED:
                    grp["quarantined"] += 1
                for rid in t.get("run_ids", []):
                    run = s["acceptances"][rid]
                    grp["runs"] += 1
                    grp["objects_total"] += run["counts"]["objects_total"]
                    grp["objects_ok"] += run["counts"]["objects_ok"]
                    grp["objects_bad"] += run["counts"]["objects_bad"]
                # only LIVE (unresolved) quarantine consumes isolated capacity
                for q in t.get("quarantine_history", []):
                    if q.get("resolution"):
                        continue
                    if t["status"] == "canceled":
                        continue
                    grp["quarantine_bytes"] += q.get("released_bytes", 0.0)
                    for code in q.get("reasons", []):
                        grp["failure_reasons"][code] = \
                            grp["failure_reasons"].get(code, 0) + 1
            for r in s["tasks"].values():
                if r.get("kind") != "repair":
                    continue
                parent = s["tasks"].get(r["parent_task_id"])
                if parent is None:
                    continue
                # group strictly by the PARENT transfer task's site/manifest
                if site_id and parent["site_id"] != site_id:
                    continue
                if manifest_id and r["manifest_id"] != manifest_id:
                    continue
                grp = g(parent)
                key = {"done": "repairs_done", "canceled": "repairs_canceled",
                       "superseded": "repairs_superseded"}.get(r["status"])
                if key:
                    grp[key] += 1
                elif r["status"] in ("queued", "transferring", "paused"):
                    grp["repairs_open"] += 1
            out = []
            for grp in groups.values():
                decided = grp["proven"] + grp["quarantined"]
                grp["pass_rate"] = round(grp["proven"] / decided, 4) if decided else None
                grp["object_pass_rate"] = (
                    round(grp["objects_ok"] / grp["objects_total"], 4)
                    if grp["objects_total"] else None)
                grp["quarantine_bytes"] = round(grp["quarantine_bytes"], 1)
                grp["quarantine_mb"] = _round6(grp["quarantine_bytes"] / 1e6)
                grp["failure_reasons"] = [
                    {"code": c, "message": ACCEPT_REASONS.get(c, c), "count": n}
                    for c, n in sorted(grp["failure_reasons"].items())]
                out.append(grp)
            out.sort(key=lambda x: (x["site_id"], x["manifest_id"]))
            return {"clock": s["clock"], "site_id": site_id,
                    "manifest_id": manifest_id, "groups": out}

    # ======================================================== invariants ==
    def check_invariants(self) -> list:
        """Return a list of invariant violations (empty means healthy)."""
        with self.lock:
            s = self.state
            bad = []
            plan = self._plan(s)
            # 1. site bandwidth never exceeded
            for site in s["sites"].values():
                used = sum(p["grant_kbps"] for tid, p in plan.items()
                           if s["tasks"][tid]["site_id"] == site["site_id"])
                if used > site["capacity_kbps"] + EPS:
                    bad.append(f"站点 {site['site_id']} 超带宽: "
                               f"{used} > {site['capacity_kbps']}")
            # 2. paired acquire/release: transferring ⇔ reservation held
            for t in s["tasks"].values():
                if t["status"] == "transferring":
                    if t["reserved_bytes"] <= EPS < self._remaining(t):
                        bad.append(f"任务 {t['task_id']} 传输中但无磁盘预留")
                elif t["reserved_bytes"] > EPS:
                    bad.append(f"任务 {t['task_id']} 非传输中却持有预留（幽灵占用）")
            # 3. box disk never exceeded (used + reservations ≤ total)
            for b in s["boxes"].values():
                reserved = sum(t["reserved_bytes"] for t in s["tasks"].values()
                               if t["box_id"] == b["box_id"]
                               and t["status"] == "transferring")
                if b["disk_used_bytes"] + reserved > b["disk_total_bytes"] + EPS:
                    bad.append(f"盒子 {b['box_id']} 磁盘超占: "
                               f"{b['disk_used_bytes']}+{reserved} > "
                               f"{b['disk_total_bytes']}")
                # 3b. chunk holds are exactly the delivered/proven on-disk bytes
                hold_bytes = sum(h["size"] for h in b.get("chunk_holds", {}).values())
                if hold_bytes > b["disk_used_bytes"] + EPS:
                    bad.append(f"盒子 {b['box_id']} 块持有 {hold_bytes} 超过已用 "
                               f"{b['disk_used_bytes']}（容量账不平）")
                if b["disk_used_bytes"] < -EPS:
                    bad.append(f"盒子 {b['box_id']} 已用容量为负（重复释放）")
            # 4. repairs: open repair ⇔ parent quarantined & active job
            for r in s["tasks"].values():
                if r.get("kind") != "repair":
                    continue
                parent = s["tasks"].get(r["parent_task_id"])
                if parent is None:
                    bad.append(f"修复 {r['task_id']} 没有父任务")
                    continue
                if r["status"] in ("queued", "transferring", "paused") and \
                        (parent["accept_stage"] != ACCEPT_QUARANTINED
                         or s["jobs"][r["job_id"]]["status"] != "active"):
                    bad.append(f"修复 {r['task_id']} 仍开放但父任务未隔离/作业已终结")
            # 5. immutable proofs: proven map entries reference existing proofs
            pids = set(s["proofs"])
            for b in s["boxes"].values():
                for h, rec in b.get("proven", {}).items():
                    pid = rec.get("proof_id")
                    if pid and pid not in pids:
                        bad.append(f"盒子 {b['box_id']} 对象 {short(h)} 引用不存在证明 {pid}")
            return bad
