"""持久化层：SQLite 表结构、迁移、事务与审计/进度留档。

本模块只负责数据存取，不包含任何排程规则；规则见 rules.py，
HTTP 请求入口见 app.py。
"""
from __future__ import annotations

import json
import sqlite3
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


def iso(value: datetime | None = None) -> str:
    return (value or utcnow()).replace(microsecond=0).isoformat().replace("+00:00", "Z")


class Repository:
    def __init__(self, path: str | Path):
        self.conn = sqlite3.connect(str(path), check_same_thread=False, isolation_level=None)
        self.conn.row_factory = sqlite3.Row
        self.conn.execute("PRAGMA foreign_keys=ON")
        self.conn.execute("PRAGMA journal_mode=WAL")
        self.conn.executescript("""
        CREATE TABLE IF NOT EXISTS satellites(id TEXT PRIMARY KEY, name TEXT NOT NULL, data_rate_mbps REAL NOT NULL, priority INTEGER NOT NULL, storage_capacity_mb REAL NOT NULL, tenant TEXT NOT NULL, status TEXT NOT NULL DEFAULT 'active');
        CREATE TABLE IF NOT EXISTS stations(id TEXT PRIMARY KEY, name TEXT NOT NULL, status TEXT NOT NULL DEFAULT 'active', weather TEXT NOT NULL DEFAULT 'clear');
        CREATE TABLE IF NOT EXISTS antennas(id TEXT PRIMARY KEY, station_id TEXT NOT NULL REFERENCES stations(id), max_rate_mbps REAL NOT NULL, status TEXT NOT NULL DEFAULT 'active');
        CREATE TABLE IF NOT EXISTS maintenance(id INTEGER PRIMARY KEY AUTOINCREMENT, station_id TEXT NOT NULL REFERENCES stations(id), antenna_id TEXT REFERENCES antennas(id), starts_at TEXT NOT NULL, ends_at TEXT NOT NULL, reason TEXT NOT NULL);
        CREATE TABLE IF NOT EXISTS visibility_windows(id INTEGER PRIMARY KEY AUTOINCREMENT, satellite_id TEXT NOT NULL REFERENCES satellites(id), station_id TEXT NOT NULL REFERENCES stations(id), starts_at TEXT NOT NULL, ends_at TEXT NOT NULL, max_rate_mbps REAL NOT NULL, revision INTEGER NOT NULL DEFAULT 1);
        CREATE TABLE IF NOT EXISTS requests(id INTEGER PRIMARY KEY AUTOINCREMENT, satellite_id TEXT NOT NULL REFERENCES satellites(id), tenant TEXT NOT NULL, priority INTEGER NOT NULL, data_mb REAL NOT NULL, deadline TEXT NOT NULL, status TEXT NOT NULL DEFAULT 'pending', created_by TEXT NOT NULL, created_at TEXT NOT NULL);
        CREATE TABLE IF NOT EXISTS quotas(id INTEGER PRIMARY KEY AUTOINCREMENT, tenant TEXT NOT NULL, station_id TEXT NOT NULL REFERENCES stations(id), daily_seconds INTEGER NOT NULL, UNIQUE(tenant,station_id));
        CREATE TABLE IF NOT EXISTS schedules(id INTEGER PRIMARY KEY AUTOINCREMENT, request_id INTEGER NOT NULL REFERENCES requests(id), window_id INTEGER NOT NULL REFERENCES visibility_windows(id), station_id TEXT NOT NULL REFERENCES stations(id), antenna_id TEXT NOT NULL REFERENCES antennas(id), satellite_id TEXT NOT NULL REFERENCES satellites(id), starts_at TEXT NOT NULL, ends_at TEXT NOT NULL, rate_mbps REAL NOT NULL, status TEXT NOT NULL DEFAULT 'scheduled', revision INTEGER NOT NULL DEFAULT 1, disposition_reason TEXT, created_by TEXT NOT NULL, created_at TEXT NOT NULL, updated_at TEXT NOT NULL);
        CREATE TABLE IF NOT EXISTS audit_log(id INTEGER PRIMARY KEY AUTOINCREMENT, request_id INTEGER, schedule_id INTEGER, actor TEXT NOT NULL, role TEXT NOT NULL, action TEXT NOT NULL, detail_json TEXT NOT NULL, created_at TEXT NOT NULL);
        """)
        self._migrate()

    def _migrate(self) -> None:
        """老库补列/补表，已有数据保持不动。"""
        cols = {r["name"] for r in self.conn.execute("PRAGMA table_info(schedules)")}
        add_column = [
            ("received_mb", "received_mb REAL NOT NULL DEFAULT 0"),
            ("actual_ends_at", "actual_ends_at TEXT"),
            ("resume_of_schedule_id", "resume_of_schedule_id INTEGER REFERENCES schedules(id)"),
        ]
        for name, ddl in add_column:
            if name not in cols:
                self.conn.execute(f"ALTER TABLE schedules ADD COLUMN {ddl}")
        # 断点续传进度流水：只追加，不更新不删除，历史进度因此不会被覆盖。
        self.conn.execute("""
        CREATE TABLE IF NOT EXISTS receive_progress(
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            request_id INTEGER NOT NULL REFERENCES requests(id),
            schedule_id INTEGER NOT NULL REFERENCES schedules(id),
            kind TEXT NOT NULL,
            received_mb REAL NOT NULL,
            cumulative_mb REAL NOT NULL,
            detail_json TEXT NOT NULL DEFAULT '{}',
            recorded_by TEXT NOT NULL,
            created_at TEXT NOT NULL)
        """)

    @contextmanager
    def tx(self):
        self.conn.execute("BEGIN IMMEDIATE")
        try:
            yield self.conn
            self.conn.execute("COMMIT")
        except Exception:
            self.conn.execute("ROLLBACK")
            raise

    @staticmethod
    def audit(conn: sqlite3.Connection, request_id: int | None, schedule_id: int | None, actor: str, role: str, action: str, detail: dict[str, Any]) -> None:
        conn.execute("INSERT INTO audit_log(request_id,schedule_id,actor,role,action,detail_json,created_at) VALUES(?,?,?,?,?,?,?)",
                     (request_id, schedule_id, actor, role, action, json.dumps(detail, ensure_ascii=False, sort_keys=True), iso()))

    @staticmethod
    def ledger(conn: sqlite3.Connection, request_id: int, schedule_id: int, kind: str, received_mb: float, cumulative_mb: float, actor: str, detail: dict[str, Any] | None = None) -> None:
        """登记一条接收进度流水（checkpoint/offline/resumed/completed）。"""
        conn.execute("""INSERT INTO receive_progress(request_id,schedule_id,kind,received_mb,cumulative_mb,detail_json,recorded_by,created_at)
                        VALUES(?,?,?,?,?,?,?,?)""",
                     (request_id, schedule_id, kind, float(received_mb), float(cumulative_mb),
                      json.dumps(detail or {}, ensure_ascii=False, sort_keys=True), actor, iso()))
