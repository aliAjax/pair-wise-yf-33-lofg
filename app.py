#!/usr/bin/env python3
"""请求入口层：HTTP 路由与鉴权头解析。

业务规则在 rules.py，持久化在 persistence.py，三者分开维护。
历史调用方（及 tests/）直接 `from app import ...` 仍然可用。
"""
from __future__ import annotations

import argparse
import json
import os
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

from persistence import iso, utcnow
from rules import ROLES, ApiError, SatelliteSchedulingService, parse_time

PORT = 8204
__all__ = ["ApiError", "SatelliteSchedulingService", "iso", "utcnow", "parse_time", "ROLES", "create_server", "main"]


def send_json(handler: BaseHTTPRequestHandler, status: int, payload: Any) -> None:
    raw = json.dumps(payload, ensure_ascii=False, default=str).encode()
    handler.send_response(status)
    handler.send_header("Content-Type", "application/json; charset=utf-8")
    handler.send_header("Content-Length", str(len(raw)))
    handler.end_headers()
    handler.wfile.write(raw)


class Handler(BaseHTTPRequestHandler):
    service: SatelliteSchedulingService
    web_root: Path

    def log_message(self, fmt: str, *args: Any) -> None:
        print(f"{self.address_string()} - {fmt % args}")

    def body(self) -> dict[str, Any]:
        size = int(self.headers.get("Content-Length", "0"))
        if not size:
            return {}
        try:
            value = json.loads(self.rfile.read(size))
        except json.JSONDecodeError as exc:
            raise ApiError(400, "invalid_json", "请求体不是有效 JSON") from exc
        if not isinstance(value, dict):
            raise ApiError(400, "invalid_json", "请求体必须是对象")
        return value

    def get_api(self, path: str) -> tuple[int, Any]:
        if path == "/health":
            return 200, {"status": "ok", "service": "satellite-scheduling"}
        actor, role, tenant = self.service.identity(self.headers)
        if path == "/api/state":
            return 200, self.service.state(role, tenant)
        parts = [p for p in path.split("/") if p]
        if len(parts) == 3 and parts[:2] == ["api", "schedules"] and parts[2].isdigit():
            return 200, self.service.get_schedule(int(parts[2]))
        raise ApiError(404, "not_found", "接口不存在")

    def post_api(self, path: str) -> tuple[int, Any]:
        actor, role, tenant = self.service.identity(self.headers)
        body = self.body()
        parts = [p for p in path.split("/") if p]
        actions = {
            "/api/satellites": lambda: (201, self.service.create_satellite(actor, role, body)),
            "/api/stations": lambda: (201, self.service.create_station(actor, role, body)),
            "/api/antennas": lambda: (201, self.service.create_antenna(actor, role, body)),
            "/api/maintenance": lambda: (201, self.service.create_maintenance(actor, role, body)),
            "/api/visibility-windows": lambda: (201, self.service.create_window(actor, role, body)),
            "/api/quotas": lambda: (200, self.service.set_quota(actor, role, body)),
            "/api/requests": lambda: (201, self.service.create_request(actor, role, tenant, body)),
        }
        if path in actions:
            return actions[path]()
        if len(parts) == 4 and parts[:2] == ["api", "requests"] and parts[2].isdigit():
            rid, action = int(parts[2]), parts[3]
            if action == "schedule":
                return 201, self.service.schedule_request(rid, actor, role, body)
            if action == "reschedule":
                return 200, self.service.reschedule(rid, actor, role, body)
            if action == "resume":
                # 断点续传：重选窗口/天线/时段，按剩余量复核后接着累计
                return 201, self.service.resume_request(rid, actor, role, body)
        if len(parts) == 4 and parts[:2] == ["api", "schedules"] and parts[2].isdigit():
            sid, action = int(parts[2]), parts[3]
            if action in {"start", "complete"}:
                return 200, self.service.transition(sid, actor, role, tenant, "receiving" if action == "start" else "received", body)
            if action == "cancel":
                return 200, self.service.cancel_schedule(sid, actor, role, tenant, body)
            if action == "preempt":
                return 200, self.service.emergency_preempt(sid, actor, role, body)
            if action == "progress":
                # 接收过程登记累计数据量
                return 200, self.service.register_progress(sid, actor, role, body)
            if action == "offline":
                # 链路闪断：写明原因、释放剩余时段、进度留档
                return 200, self.service.mark_offline(sid, actor, role, body)
        if len(parts) == 4 and parts[:2] == ["api", "visibility-windows"] and parts[2].isdigit() and parts[3] == "change":
            return 200, self.service.change_window(int(parts[2]), actor, role, body)
        raise ApiError(404, "not_found", "接口不存在")

    def handle_request(self, method: str) -> None:
        parsed = urlparse(self.path)
        try:
            if method == "GET" and parsed.path == "/":
                raw = (self.web_root / "index.html").read_bytes()
                self.send_response(200)
                self.send_header("Content-Type", "text/html; charset=utf-8")
                self.send_header("Content-Length", str(len(raw)))
                self.end_headers()
                self.wfile.write(raw)
                return
            status, payload = self.get_api(parsed.path) if method == "GET" else self.post_api(parsed.path)
            send_json(self, status, payload)
        except ApiError as exc:
            payload = {"error": exc.code, "message": exc.message}
            if exc.details is not None:
                payload["details"] = exc.details
            send_json(self, exc.status, payload)
        except Exception as exc:
            print(f"unhandled error: {exc!r}")
            send_json(self, 500, {"error": "internal_error", "message": str(exc)})

    def do_GET(self) -> None:
        self.handle_request("GET")

    def do_POST(self) -> None:
        self.handle_request("POST")


def create_server(db_path: str | Path, host: str = "127.0.0.1", port: int = PORT) -> ThreadingHTTPServer:
    service = SatelliteSchedulingService(db_path)
    handler = type("SatelliteHandler", (Handler,), {"service": service, "web_root": Path(__file__).resolve().parent / "static"})
    return ThreadingHTTPServer((host, port), handler)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=PORT)
    parser.add_argument("--db", default=os.environ.get("SAT_DB", "satellite_scheduling.db"))
    args = parser.parse_args()
    server = create_server(args.db, args.host, args.port)
    print(f"satellite-scheduling listening on http://{args.host}:{args.port}")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()


if __name__ == "__main__":
    main()
