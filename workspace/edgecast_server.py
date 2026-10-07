#!/usr/bin/env python3
"""
edgecast_server.py — 边缘地图包分批投放编排器的零依赖 HTTP 服务。

JSON API 前缀 /api/edgecast，实时事件流走 SSE（/api/edgecast/events/stream）。
回执入口 /receipts 与批次管理入口相互独立，可被设备侧直接回调。

启动：
  python3 edgecast_server.py 8090 --data data/edgecast --no-scheduler   # 手动 tick
  python3 edgecast_server.py 8090                                      # 默认带调度线程
"""

from __future__ import annotations

import json
import queue
import re
import sys
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, urlparse
from typing import Optional

from engine import DomainError
from edgecast import EdgeCastEngine

DATA_DIR = "data/edgecast"


def build_engine(data_dir: str = DATA_DIR, scheduler: bool = True,
                 interval: float = 0.1) -> EdgeCastEngine:
    eng = EdgeCastEngine(f"{data_dir}/events.log")
    if scheduler:
        eng.start_scheduler(interval)
    return eng


class Handler(BaseHTTPRequestHandler):
    engine: EdgeCastEngine = None  # 由 main 注入（类属性）

    def log_message(self, fmt, *args):
        sys.stderr.write("[edgecast] " + fmt % args + "\n")

    # ------------------------------------------------------------- helpers ==
    def _send_json(self, obj, status: int = 200):
        body = json.dumps(obj, ensure_ascii=False).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _send_text(self, text: str, status: int = 200,
                   ctype: str = "text/plain; charset=utf-8",
                   download: Optional[str] = None):
        body = text.encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", ctype)
        self.send_header("Content-Length", str(len(body)))
        if download:
            self.send_header("Content-Disposition",
                             f'attachment; filename="{download}"')
        self.end_headers()
        self.wfile.write(body)

    def _read_json(self) -> dict:
        n = int(self.headers.get("Content-Length") or 0)
        if not n:
            return {}
        raw = self.rfile.read(n)
        try:
            data = json.loads(raw.decode("utf-8"))
        except json.JSONDecodeError as e:
            raise DomainError("BAD_REQUEST", f"请求体不是合法 JSON: {e}", 400)
        if not isinstance(data, dict):
            raise DomainError("BAD_REQUEST", "请求体必须是 JSON 对象", 400)
        return data

    def _guard(self, fn):
        try:
            return fn()
        except DomainError as e:
            self._send_json(e.to_body(), e.status)
        except (ValueError, TypeError) as e:
            self._send_json(
                {"error": {"code": "BAD_REQUEST", "message": str(e)}}, 400)

    # ============================================================ routing ==
    def do_GET(self):
        self._guard(self._do_get)

    def do_POST(self):
        self._guard(self._do_post)

    def _do_get(self):
        u = urlparse(self.path)
        p, q = u.path, parse_qs(u.query)
        eng = self.engine

        if p == "/api/edgecast/snapshot":
            return self._send_json(eng.snapshot())
        if p == "/api/edgecast/batches":
            return self._send_json({"batches": eng.list_batches()})
        if p == "/api/edgecast/packages":
            return self._send_json({"packages": eng.snapshot()["packages"]})
        if p == "/api/edgecast/devices":
            return self._send_json({"devices": eng.snapshot()["devices"]})
        if p == "/api/edgecast/rejects":
            return self._send_json(
                {"rejects": eng.list_rejects(int((q.get("limit") or [100])[0]))})
        if p == "/api/edgecast/events":
            return self._send_json(
                {"events": eng.list_events(int((q.get("limit") or [200])[0]))})
        if p == "/api/edgecast/events/stream":
            return self._sse()

        m = re.match(r"^/api/edgecast/batches/([^/]+)$", p)
        if m:
            return self._send_json(eng.batch_view(m.group(1)))
        m = re.match(r"^/api/edgecast/batches/([^/]+)/timeline$", p)
        if m:
            return self._send_json(eng.timeline(
                m.group(1), (q.get("group") or [None])[0],
                (q.get("device") or [None])[0]))
        m = re.match(r"^/api/edgecast/batches/([^/]+)/summary$", p)
        if m:
            fmt = (q.get("format") or ["json"])[0]
            bid = m.group(1)
            if fmt == "text":
                return self._send_text(
                    eng.summary_text(bid), download=f"{bid}-summary.txt")
            return self._send_json(eng.summary(bid))
        if p == "/" or p.startswith("/edgecast"):
            return self._serve_ui(p)
        self._send_json({"error": {"code": "NOT_FOUND", "message": p}}, 404)

    def _do_post(self):
        u = urlparse(self.path)
        p, body = u.path, self._read_json()
        eng = self.engine

        if p == "/api/edgecast/packages":
            return self._send_json(eng.register_package(
                str(body.get("package_id", "")).strip(),
                str(body.get("version", "")).strip(),
                str(body.get("note", ""))), 200)
        if p == "/api/edgecast/devices":
            return self._send_json(eng.register_device(
                str(body.get("device_id", "")).strip(),
                body.get("group_hint")), 200)
        if p == "/api/edgecast/batches":
            return self._send_json(eng.create_batch(body), 201)
        if p == "/api/edgecast/receipts":
            # 独立回执入口：设备侧直接投递；永远返回 200 + 确定判定
            r = eng.ingest_receipt(body)
            return self._send_json(r, 200)
        if p == "/api/edgecast/tick":
            return self._send_json(eng.tick())

        m = re.match(r"^/api/edgecast/batches/([^/]+)/(pause|resume|revoke)$", p)
        if m:
            bid, action = m.group(1), m.group(2)
            reason = str(body.get("reason", ""))
            if action == "pause":
                return self._send_json(eng.pause_batch(bid, reason))
            if action == "resume":
                return self._send_json(eng.resume_batch(bid))
            return self._send_json(eng.revoke_batch(bid, reason))
        self._send_json({"error": {"code": "NOT_FOUND", "message": p}}, 404)

    # ================================================================== SSE ==
    def _sse(self):
        eng = self.engine
        q: queue.Queue = eng.subscribe()
        self.send_response(200)
        self.send_header("Content-Type", "text/event-stream; charset=utf-8")
        self.send_header("Cache-Control", "no-cache")
        self.send_header("Connection", "keep-alive")
        self.end_headers()
        try:
            self.wfile.write(b"retry: 2000\n\n")
            self.wfile.flush()
            while True:
                try:
                    ev = q.get(timeout=15)
                except queue.Empty:
                    self.wfile.write(b": ping\n\n")
                    self.wfile.flush()
                    continue
                data = json.dumps(ev, ensure_ascii=False)
                self.wfile.write(f"id: {ev['id']}\nevent: {ev['type']}\n"
                                 f"data: {data}\n\n".encode("utf-8"))
                self.wfile.flush()
        except (BrokenPipeError, ConnectionResetError):
            pass
        finally:
            eng.unsubscribe(q)

    # ================================================================== UI ==
    def _serve_ui(self, path: str):
        import os
        base = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                            "static_edgecast")
        rel = "index.html" if path in ("/", "/edgecast", "/edgecast/") \
            else path.split("/edgecast/", 1)[-1]
        fp = os.path.normpath(os.path.join(base, rel))
        if not fp.startswith(base) or not os.path.isfile(fp):
            self._send_json({"error": {"code": "NOT_FOUND", "message": path}}, 404)
            return
        ctype = {"html": "text/html; charset=utf-8",
                 "js": "application/javascript; charset=utf-8",
                 "css": "text/css; charset=utf-8"}.get(
            fp.rsplit(".", 1)[-1], "application/octet-stream")
        with open(fp, "rb") as f:
            body = f.read()
        self.send_response(200)
        self.send_header("Content-Type", ctype)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)


def main(argv=None):
    argv = argv or sys.argv[1:]
    port = int(argv[0]) if argv else 8090
    host = "0.0.0.0"
    data_dir = DATA_DIR
    scheduler = True
    interval = 0.1
    i = 1
    while i < len(argv):
        a = argv[i]
        if a == "--data":
            data_dir = argv[i + 1]; i += 2
        elif a == "--no-scheduler":
            scheduler = False; i += 1
        elif a == "--interval":
            interval = float(argv[i + 1]); i += 2
        else:
            i += 1
    Handler.engine = build_engine(data_dir, scheduler, interval)
    httpd = ThreadingHTTPServer((host, port), Handler)
    print(f"EdgeCast 编排器已启动: http://localhost:{port}/edgecast/")
    print(f"存储: {data_dir}/events.log  调度线程: {'开' if scheduler else '关（手动 /tick）'}")
    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        Handler.engine.close()
        httpd.server_close()


if __name__ == "__main__":
    main()
