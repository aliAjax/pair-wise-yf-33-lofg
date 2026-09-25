"""规则层：多站卫星排程与断点续传的业务规则。

只依赖 persistence.Repository，不感知 HTTP。
接收状态机：
    scheduled -> receiving -> received            正常完成
    receiving -> offline（可恢复掉线）-> 新排程 receiving -> received
掉线段会释放实际结束时刻之后的剩余时段，请求回到 receiving 可续传。
"""
from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from persistence import Repository, iso, utcnow

ROLES = {"viewer", "requester", "operator", "commander", "auditor"}
# 浮点比较容差（MB / Mbps 计算累积误差）
EPS = 1e-6
# 参与天线/卫星冲突与窗口变化处理的“仍占时段”状态
ACTIVE_STATUSES = ("scheduled", "receiving")
LEDGER_KINDS = ("checkpoint", "offline", "resumed", "completed")


class ApiError(Exception):
    def __init__(self, status: int, code: str, message: str, details: Any = None):
        super().__init__(message)
        self.status, self.code, self.message, self.details = status, code, message, details


def parse_time(value: str | None) -> datetime:
    if not value:
        raise ApiError(400, "time_required", "必须提供 ISO 8601 时间")
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise ApiError(400, "invalid_time", f"时间格式错误: {value}") from exc
    return parsed.replace(tzinfo=timezone.utc) if parsed.tzinfo is None else parsed.astimezone(timezone.utc)


def overlap(a_start: datetime, a_end: datetime, b_start: datetime, b_end: datetime) -> bool:
    return a_start < b_end and b_start < a_end


# get_schedule / state 用的进度汇总：取本段最近一条留档的累计值；
# receiving 段用 resumed 基线加本段实时 received_mb。口径只取一次，不双计。
PROGRESS_SELECT = """CASE s.status
    WHEN 'receiving' THEN COALESCE((SELECT cumulative_mb FROM receive_progress WHERE schedule_id=s.id AND kind='resumed' ORDER BY id LIMIT 1),0)+s.received_mb
    WHEN 'offline' THEN (SELECT cumulative_mb FROM receive_progress WHERE schedule_id=s.id AND kind='offline' ORDER BY id LIMIT 1)
    WHEN 'received' THEN (SELECT cumulative_mb FROM receive_progress WHERE schedule_id=s.id AND kind='completed' ORDER BY id LIMIT 1)
    ELSE COALESCE((SELECT SUM(p.received_mb) FROM receive_progress p WHERE p.request_id=s.request_id AND p.kind='offline'),0)+s.received_mb
    END AS cumulative_mb"""

SCHEDULE_SELECT = f"""SELECT s.*,r.tenant,r.data_mb,r.priority request_priority,r.deadline,
    (SELECT COALESCE(SUM(p.received_mb),0) FROM receive_progress p WHERE p.schedule_id=s.id) AS segment_mb,
    {PROGRESS_SELECT.lstrip()}
    FROM schedules s JOIN requests r ON r.id=s.request_id"""


class SatelliteSchedulingService:
    def __init__(self, path):
        self.repo = Repository(path)

    # ---- 通用 ----
    @staticmethod
    def identity(headers: Any) -> tuple[str, str, str]:
        actor, role, tenant = headers.get("X-User-Id", "").strip(), headers.get("X-Role", "").strip(), headers.get("X-Tenant", "").strip()
        if not actor or role not in ROLES:
            raise ApiError(401, "unauthorized", "需要 X-User-Id 和有效 X-Role")
        if role == "requester" and not tenant:
            raise ApiError(401, "tenant_required", "requester 必须提供 X-Tenant")
        return actor, role, tenant

    @staticmethod
    def _dict(row) -> dict[str, Any] | None:
        return dict(row) if row else None

    def _prior_received(self, conn, request_id: int, exclude_schedule: int | None = None) -> float:
        """请求在其它（历史掉线）排程段上已确认接收的数据量。"""
        sql = """SELECT COALESCE(SUM(received_mb),0) FROM receive_progress
                 WHERE request_id=? AND kind='offline'"""
        args: list[Any] = [request_id]
        if exclude_schedule is not None:
            sql += " AND schedule_id<>?"
            args.append(exclude_schedule)
        return float(conn.execute(sql, args).fetchone()[0])

    def _progress_rows(self, conn, where_sql: str, args: list[Any]) -> list[dict[str, Any]]:
        rows = conn.execute(f"""SELECT p.* FROM receive_progress p WHERE {where_sql} ORDER BY p.id DESC LIMIT 200""", args).fetchall()
        return [dict(r) for r in rows]

    # ---- 资源配置 ----
    def create_satellite(self, actor: str, role: str, body: dict[str, Any]) -> dict[str, Any]:
        if role not in {"operator", "commander"}: raise ApiError(403, "resource_forbidden", "当前角色不能维护卫星")
        sid, name, tenant = str(body.get("id", "")).strip(), str(body.get("name", "")).strip(), str(body.get("tenant", "")).strip()
        rate, priority, capacity = body.get("data_rate_mbps"), body.get("priority"), body.get("storage_capacity_mb")
        if not sid or not name or not tenant or not isinstance(rate, (int, float)) or float(rate) <= 0 or not isinstance(priority, int) or not 1 <= priority <= 10 or not isinstance(capacity, (int, float)) or float(capacity) <= 0:
            raise ApiError(400, "invalid_satellite", "卫星参数不完整")
        with self.repo.tx() as conn:
            conn.execute("INSERT OR REPLACE INTO satellites(id,name,data_rate_mbps,priority,storage_capacity_mb,tenant,status) VALUES(?,?,?,?,?,?,?)", (sid, name, float(rate), priority, float(capacity), tenant, body.get("status", "active")))
            return dict(conn.execute("SELECT * FROM satellites WHERE id=?", (sid,)).fetchone())

    def create_station(self, actor: str, role: str, body: dict[str, Any]) -> dict[str, Any]:
        if role not in {"operator", "commander"}: raise ApiError(403, "resource_forbidden", "当前角色不能维护地面站")
        sid, name = str(body.get("id", "")).strip(), str(body.get("name", "")).strip()
        weather = str(body.get("weather", "clear")).lower()
        if not sid or not name or weather not in {"clear", "rain", "storm", "closed"}: raise ApiError(400, "invalid_station", "地面站名称或天气状态无效")
        with self.repo.tx() as conn:
            conn.execute("INSERT OR REPLACE INTO stations(id,name,status,weather) VALUES(?,?,?,?)", (sid, name, body.get("status", "active"), weather))
            return dict(conn.execute("SELECT * FROM stations WHERE id=?", (sid,)).fetchone())

    def create_antenna(self, actor: str, role: str, body: dict[str, Any]) -> dict[str, Any]:
        if role not in {"operator", "commander"}: raise ApiError(403, "resource_forbidden", "当前角色不能维护天线")
        aid, station, rate = str(body.get("id", "")).strip(), str(body.get("station_id", "")).strip(), body.get("max_rate_mbps")
        if not aid or not station or not isinstance(rate, (int, float)) or float(rate) <= 0: raise ApiError(400, "invalid_antenna", "天线参数无效")
        with self.repo.tx() as conn:
            if not conn.execute("SELECT 1 FROM stations WHERE id=?", (station,)).fetchone(): raise ApiError(404, "station_not_found", "地面站不存在")
            conn.execute("INSERT OR REPLACE INTO antennas(id,station_id,max_rate_mbps,status) VALUES(?,?,?,?)", (aid, station, float(rate), body.get("status", "active")))
            return dict(conn.execute("SELECT * FROM antennas WHERE id=?", (aid,)).fetchone())

    def create_maintenance(self, actor: str, role: str, body: dict[str, Any]) -> dict[str, Any]:
        if role not in {"operator", "commander"}: raise ApiError(403, "maintenance_forbidden", "当前角色不能登记维护")
        station, start, end, reason = str(body.get("station_id", "")).strip(), parse_time(body.get("starts_at")), parse_time(body.get("ends_at")), str(body.get("reason", "")).strip()
        antenna = body.get("antenna_id")
        if not station or end <= start or not reason: raise ApiError(400, "invalid_maintenance", "维护参数无效")
        with self.repo.tx() as conn:
            if not conn.execute("SELECT 1 FROM stations WHERE id=?", (station,)).fetchone(): raise ApiError(404, "station_not_found", "地面站不存在")
            if antenna and not conn.execute("SELECT 1 FROM antennas WHERE id=? AND station_id=?", (antenna, station)).fetchone(): raise ApiError(400, "antenna_station_mismatch", "天线不属于该站")
            cur = conn.execute("INSERT INTO maintenance(station_id,antenna_id,starts_at,ends_at,reason) VALUES(?,?,?,?,?)", (station, antenna, iso(start), iso(end), reason))
            return dict(conn.execute("SELECT * FROM maintenance WHERE id=?", (cur.lastrowid,)).fetchone())

    def create_window(self, actor: str, role: str, body: dict[str, Any]) -> dict[str, Any]:
        if role not in {"operator", "commander"}: raise ApiError(403, "window_forbidden", "当前角色不能维护可见窗口")
        satellite, station, start, end, rate = str(body.get("satellite_id", "")).strip(), str(body.get("station_id", "")).strip(), parse_time(body.get("starts_at")), parse_time(body.get("ends_at")), body.get("max_rate_mbps")
        if not satellite or not station or end <= start or not isinstance(rate, (int, float)) or float(rate) <= 0: raise ApiError(400, "invalid_window", "可见窗口参数无效")
        with self.repo.tx() as conn:
            if not conn.execute("SELECT 1 FROM satellites WHERE id=?", (satellite,)).fetchone(): raise ApiError(404, "satellite_not_found", "卫星不存在")
            if not conn.execute("SELECT 1 FROM stations WHERE id=?", (station,)).fetchone(): raise ApiError(404, "station_not_found", "地面站不存在")
            cur = conn.execute("INSERT INTO visibility_windows(satellite_id,station_id,starts_at,ends_at,max_rate_mbps) VALUES(?,?,?,?,?)", (satellite, station, iso(start), iso(end), float(rate)))
            return dict(conn.execute("SELECT * FROM visibility_windows WHERE id=?", (cur.lastrowid,)).fetchone())

    def set_quota(self, actor: str, role: str, body: dict[str, Any]) -> dict[str, Any]:
        if role not in {"operator", "commander"}: raise ApiError(403, "quota_forbidden", "当前角色不能设置租户配额")
        tenant, station, seconds = str(body.get("tenant", "")).strip(), str(body.get("station_id", "")).strip(), body.get("daily_seconds")
        if not tenant or not station or not isinstance(seconds, int) or seconds <= 0: raise ApiError(400, "invalid_quota", "配额参数无效")
        with self.repo.tx() as conn:
            if not conn.execute("SELECT 1 FROM stations WHERE id=?", (station,)).fetchone(): raise ApiError(404, "station_not_found", "地面站不存在")
            conn.execute("INSERT INTO quotas(tenant,station_id,daily_seconds) VALUES(?,?,?) ON CONFLICT(tenant,station_id) DO UPDATE SET daily_seconds=excluded.daily_seconds", (tenant, station, seconds))
            return dict(conn.execute("SELECT * FROM quotas WHERE tenant=? AND station_id=?", (tenant, station)).fetchone())

    # ---- 数据请求 ----
    def create_request(self, actor: str, role: str, tenant: str, body: dict[str, Any]) -> dict[str, Any]:
        if role not in {"requester", "operator", "commander"}: raise ApiError(403, "request_forbidden", "当前角色不能创建数据请求")
        satellite = str(body.get("satellite_id", "")).strip(); data_mb = body.get("data_mb"); priority = body.get("priority"); deadline = parse_time(body.get("deadline"))
        owner = tenant if role == "requester" else str(body.get("tenant", "")).strip()
        if not satellite or not owner or not isinstance(data_mb, (int, float)) or float(data_mb) <= 0 or not isinstance(priority, int) or not 1 <= priority <= 10:
            raise ApiError(400, "invalid_request", "请求参数无效")
        with self.repo.tx() as conn:
            satellite_row = conn.execute("SELECT * FROM satellites WHERE id=?", (satellite,)).fetchone()
            if not satellite_row: raise ApiError(404, "satellite_not_found", "卫星不存在")
            if owner != satellite_row["tenant"]: raise ApiError(403, "tenant_satellite_forbidden", "租户不能申请不属于自己的卫星")
            if deadline <= utcnow(): raise ApiError(409, "deadline_expired", "请求截止时间已经过去")
            cur = conn.execute("INSERT INTO requests(satellite_id,tenant,priority,data_mb,deadline,created_by,created_at) VALUES(?,?,?,?,?,?,?)", (satellite, owner, priority, float(data_mb), iso(deadline), actor, iso()))
            request_id = cur.lastrowid
            Repository.audit(conn, request_id, None, actor, role, "request_created", {"data_mb": float(data_mb), "deadline": iso(deadline)})
            return dict(conn.execute("SELECT * FROM requests WHERE id=?", (request_id,)).fetchone())

    def _used_quota(self, conn, tenant: str, station: str, day: str, exclude_schedule: int | None = None) -> int:
        """当日配额占用：计划段按计划时长，掉线段只按实际占用时长（剩余时段已释放）。"""
        sql = """SELECT COALESCE(SUM(
                    (julianday(COALESCE(s.actual_ends_at,s.ends_at))-julianday(s.starts_at))*86400),0)
                 FROM schedules s JOIN requests r ON r.id=s.request_id
                 WHERE r.tenant=? AND s.station_id=? AND substr(s.starts_at,1,10)=?
                   AND s.status IN ('scheduled','receiving','received','offline')"""
        args: list[Any] = [tenant, station, day]
        if exclude_schedule is not None:
            sql += " AND s.id<>?"; args.append(exclude_schedule)
        return int(conn.execute(sql, args).fetchone()[0])

    def _check_placement(self, conn, request, window, antenna, start, end, rate, required_mb: float,
                         exclude_schedule: int | None = None) -> tuple[dict[str, Any], dict[str, Any], dict[str, Any], float]:
        """新排程/续传共用的窗口、天线、时段校验：容量、维护、天气、冲突、截止时刻、租户额度。"""
        if request["satellite_id"] != window["satellite_id"] or window["station_id"] != antenna["station_id"]:
            raise ApiError(409, "window_mismatch", "卫星、窗口和天线不匹配")
        station = conn.execute("SELECT * FROM stations WHERE id=?", (window["station_id"],)).fetchone()
        satellite = conn.execute("SELECT * FROM satellites WHERE id=?", (request["satellite_id"],)).fetchone()
        if satellite["status"] != "active" or station["status"] != "active" or antenna["status"] != "active":
            raise ApiError(409, "resource_inactive", "卫星、地面站或天线不可用")
        if station["weather"] != "clear":
            raise ApiError(409, "weather_blocked", "天气条件不允许接收")
        w_start, w_end = parse_time(window["starts_at"]), parse_time(window["ends_at"])
        if start < w_start or end > w_end:
            raise ApiError(409, "outside_visibility", "排程超出可见窗口")
        if end > parse_time(request["deadline"]):
            raise ApiError(409, "deadline_missed", "预计结束时间超过请求截止时间")
        max_rate = min(float(satellite["data_rate_mbps"]), float(window["max_rate_mbps"]), float(antenna["max_rate_mbps"]))
        if float(rate) > max_rate + EPS:
            raise ApiError(409, "rate_exceeded", "请求速率超过可用上限", {"max_rate_mbps": max_rate})
        capacity = (end - start).total_seconds() * float(rate) / 8
        if capacity + EPS < required_mb:
            raise ApiError(409, "insufficient_capacity", "窗口内可接收数据量不足",
                           {"capacity_mb": capacity, "required_mb": required_mb})
        maintenance = conn.execute("""SELECT * FROM maintenance WHERE station_id=? AND (antenna_id IS NULL OR antenna_id=?) AND starts_at<? AND ends_at>?""",
                                   (station["id"], antenna["id"], iso(end), iso(start))).fetchone()
        if maintenance:
            raise ApiError(409, "maintenance_conflict", "天线或地面站处于维护期", dict(maintenance))
        equipment = conn.execute("""SELECT id,status FROM schedules WHERE station_id=? AND antenna_id=? AND starts_at<? AND COALESCE(actual_ends_at,ends_at)>? AND status IN ('scheduled','receiving')""",
                                 (station["id"], antenna["id"], iso(end), iso(start))).fetchone()
        if equipment:
            raise ApiError(409, "antenna_conflict", "天线时段已被占用", {"schedule_id": equipment["id"]})
        satellite_conflict = conn.execute("""SELECT id,status FROM schedules WHERE satellite_id=? AND starts_at<? AND COALESCE(actual_ends_at,ends_at)>? AND status IN ('scheduled','receiving')""",
                                          (request["satellite_id"], iso(end), iso(start))).fetchone()
        if satellite_conflict:
            raise ApiError(409, "satellite_conflict", "同一卫星时段已被其他站接收", {"schedule_id": satellite_conflict["id"]})
        quota = conn.execute("SELECT daily_seconds FROM quotas WHERE tenant=? AND station_id=?", (request["tenant"], station["id"])).fetchone()
        used = self._used_quota(conn, request["tenant"], station["id"], start.date().isoformat(), exclude_schedule)
        duration = int((end - start).total_seconds())
        if quota and used + duration > quota["daily_seconds"]:
            raise ApiError(409, "tenant_quota_exceeded", "租户当日地面站配额不足",
                           {"used_seconds": used, "requested_seconds": duration, "limit": quota["daily_seconds"]})
        return station, satellite, capacity

    def schedule_request(self, request_id: int, actor: str, role: str, body: dict[str, Any]) -> dict[str, Any]:
        if role not in {"operator", "commander"}: raise ApiError(403, "schedule_forbidden", "只有排程员可以安排接收")
        window_id, antenna_id = body.get("window_id"), str(body.get("antenna_id", "")).strip()
        start, end, rate = parse_time(body.get("starts_at")), parse_time(body.get("ends_at")), body.get("rate_mbps")
        if not isinstance(window_id, int) or not antenna_id or end <= start or not isinstance(rate, (int, float)) or float(rate) <= 0:
            raise ApiError(400, "invalid_schedule", "排程参数无效")
        with self.repo.tx() as conn:
            request = conn.execute("SELECT * FROM requests WHERE id=?", (request_id,)).fetchone()
            window = conn.execute("SELECT * FROM visibility_windows WHERE id=?", (window_id,)).fetchone()
            antenna = conn.execute("SELECT * FROM antennas WHERE id=?", (antenna_id,)).fetchone()
            if not request or not window or not antenna: raise ApiError(404, "schedule_ref_not_found", "请求、窗口或天线不存在")
            if request["status"] == "receiving":
                raise ApiError(409, "resume_required", "请求处于掉线续传状态，请使用续传接口", {"resume_from": "receiving"})
            if request["status"] not in {"pending", "preempted"}:
                raise ApiError(409, "request_closed", "请求当前不能排程")
            station, satellite, transferred = self._check_placement(conn, request, window, antenna, start, end, rate, float(request["data_mb"]))
            cur = conn.execute("""INSERT INTO schedules(request_id,window_id,station_id,antenna_id,satellite_id,starts_at,ends_at,rate_mbps,created_by,created_at,updated_at)
                                  VALUES(?,?,?,?,?,?,?,?,?,?,?)""",
                               (request_id, window_id, station["id"], antenna_id, request["satellite_id"], iso(start), iso(end), float(rate), actor, iso(), iso()))
            schedule_id = cur.lastrowid
            conn.execute("UPDATE requests SET status='scheduled' WHERE id=?", (request_id,))
            Repository.audit(conn, request_id, schedule_id, actor, role, "schedule_created", {"window_id": window_id, "antenna_id": antenna_id, "capacity_mb": transferred})
            return self.get_schedule(schedule_id)

    def get_schedule(self, schedule_id: int) -> dict[str, Any]:
        row = self.repo.conn.execute(f"{SCHEDULE_SELECT} WHERE s.id=?", (schedule_id,)).fetchone()
        if not row: raise ApiError(404, "schedule_not_found", "排程不存在")
        return dict(row)

    # ---- 接收中：登记累计数据量 ----
    def register_progress(self, schedule_id: int, actor: str, role: str, body: dict[str, Any]) -> dict[str, Any]:
        if role not in {"operator", "commander"}: raise ApiError(403, "receive_forbidden", "当前角色不能登记接收进度")
        received = body.get("received_mb")
        if not isinstance(received, (int, float)) or isinstance(received, bool) or float(received) < 0:
            raise ApiError(400, "invalid_progress", "received_mb 必须是非负数（MB）")
        received = float(received)
        with self.repo.tx() as conn:
            row = conn.execute("""SELECT s.*,r.data_mb,r.status request_status FROM schedules s JOIN requests r ON r.id=s.request_id WHERE s.id=?""", (schedule_id,)).fetchone()
            if not row: raise ApiError(404, "schedule_not_found", "排程不存在")
            if row["status"] != "receiving": raise ApiError(409, "invalid_transition", "只有接收中任务可以登记进度")
            if received + EPS < float(row["received_mb"]):
                raise ApiError(409, "progress_regressed", "累计接收量不能回退", {"registered_mb": float(row["received_mb"])})
            capacity = (parse_time(row["ends_at"]) - parse_time(row["starts_at"])).total_seconds() * float(row["rate_mbps"]) / 8
            if received > capacity + EPS:
                raise ApiError(409, "progress_over_segment", "登记量超过本段计划可接收量", {"segment_capacity_mb": capacity})
            prior = self._prior_received(conn, row["request_id"], schedule_id)
            cumulative = prior + received
            conn.execute("UPDATE schedules SET received_mb=?,revision=revision+1,updated_at=? WHERE id=?", (received, iso(), schedule_id))
            Repository.ledger(conn, row["request_id"], schedule_id, "checkpoint", received, cumulative, actor)
            Repository.audit(conn, row["request_id"], schedule_id, actor, role, "receive_progress",
                             {"received_mb": received, "cumulative_mb": cumulative})
            return self.get_schedule(schedule_id)

    # ---- 掉线：写明原因、释放剩余时段、进度留档 ----
    def mark_offline(self, schedule_id: int, actor: str, role: str, body: dict[str, Any]) -> dict[str, Any]:
        if role not in {"operator", "commander"}: raise ApiError(403, "receive_forbidden", "当前角色不能登记掉线")
        reason = str(body.get("reason", "")).strip()
        if not reason: raise ApiError(400, "reason_required", "掉线原因必填")
        ended = parse_time(body["ends_at"]) if body.get("ends_at") else utcnow()
        received = body.get("received_mb", None)
        with self.repo.tx() as conn:
            row = conn.execute("""SELECT s.*,r.data_mb,r.status request_status FROM schedules s JOIN requests r ON r.id=s.request_id WHERE s.id=?""", (schedule_id,)).fetchone()
            if not row: raise ApiError(404, "schedule_not_found", "排程不存在")
            if row["status"] != "receiving": raise ApiError(409, "invalid_transition", "只有接收中任务可以登记掉线")
            start, planned_end = parse_time(row["starts_at"]), parse_time(row["ends_at"])
            if ended < start: raise ApiError(409, "invalid_offline_time", "掉线时刻不能早于排程开始时间")
            clamped = False
            if ended > planned_end:
                ended, clamped = planned_end, True
            if received is None:
                received = float(row["received_mb"])
            if not isinstance(received, (int, float)) or isinstance(received, bool) or float(received) < 0:
                raise ApiError(400, "invalid_progress", "received_mb 必须是非负数（MB）")
            received = float(received)
            if received + EPS < float(row["received_mb"]):
                raise ApiError(409, "progress_regressed", "累计接收量不能回退", {"registered_mb": float(row["received_mb"])})
            actual_capacity = (ended - start).total_seconds() * float(row["rate_mbps"]) / 8
            if received > actual_capacity + EPS:
                raise ApiError(409, "progress_over_segment", "登记量超过掉线前实际可接收量", {"actual_capacity_mb": actual_capacity})
            prior = self._prior_received(conn, row["request_id"], schedule_id)
            cumulative = prior + received
            conn.execute("""UPDATE schedules SET status='offline',received_mb=?,actual_ends_at=?,disposition_reason=?,revision=revision+1,updated_at=? WHERE id=?""",
                         (received, iso(ended), reason, iso(), schedule_id))
            # 普通待接收请求不动；正在接收的请求保持 receiving，即“可恢复掉线”状态
            conn.execute("UPDATE requests SET status='receiving' WHERE id=? AND status='scheduled'", (row["request_id"],))
            detail = {"reason": reason, "ended_at": iso(ended), "planned_ends_at": iso(planned_end),
                      "released_seconds": max(0, int((planned_end - ended).total_seconds())),
                      "clamped_to_end": clamped}
            Repository.ledger(conn, row["request_id"], schedule_id, "offline", received, cumulative, actor, detail)
            Repository.audit(conn, row["request_id"], schedule_id, actor, role, "receive_offline",
                             {"reason": reason, "received_mb": received, "cumulative_mb": cumulative,
                              "released_seconds": detail["released_seconds"]})
            schedule = self.get_schedule(schedule_id)
            schedule["released_window"] = {"after": iso(ended), "until": iso(planned_end), "seconds": detail["released_seconds"]}
            schedule["resume"] = {"remaining_mb": float(row["data_mb"]) - cumulative, "cumulative_mb": cumulative, "reason": reason}
            return schedule

    # ---- 续传：重选窗口、天线和时段，按剩余量复核全部约束 ----
    def resume_request(self, request_id: int, actor: str, role: str, body: dict[str, Any]) -> dict[str, Any]:
        if role not in {"operator", "commander"}: raise ApiError(403, "schedule_forbidden", "只有排程员可以安排续传")
        window_id, antenna_id = body.get("window_id"), str(body.get("antenna_id", "")).strip()
        start, end, rate = parse_time(body.get("starts_at")), parse_time(body.get("ends_at")), body.get("rate_mbps")
        if not isinstance(window_id, int) or not antenna_id or end <= start or not isinstance(rate, (int, float)) or float(rate) <= 0:
            raise ApiError(400, "invalid_schedule", "续传参数无效")
        with self.repo.tx() as conn:
            request = conn.execute("SELECT * FROM requests WHERE id=?", (request_id,)).fetchone()
            window = conn.execute("SELECT * FROM visibility_windows WHERE id=?", (window_id,)).fetchone()
            antenna = conn.execute("SELECT * FROM antennas WHERE id=?", (antenna_id,)).fetchone()
            if not request: raise ApiError(404, "request_not_found", "请求不存在")
            if not window or not antenna: raise ApiError(404, "schedule_ref_not_found", "窗口或天线不存在")
            if request["status"] != "receiving":
                raise ApiError(409, "resume_not_needed", "只有处于可恢复掉线状态的请求可以续传")
            previous = conn.execute("""SELECT * FROM receive_progress WHERE request_id=? AND kind='offline' ORDER BY id DESC LIMIT 1""", (request_id,)).fetchone()
            if not previous: raise ApiError(409, "resume_not_needed", "没有掉线进度可续传")
            prior = float(previous["cumulative_mb"])
            remaining = float(request["data_mb"]) - prior
            if remaining <= EPS:
                raise ApiError(409, "nothing_to_resume", "累计数据量已达目标，无需续传", {"cumulative_mb": prior})
            # 容量等约束全部按剩余量复核
            station, satellite, capacity = self._check_placement(conn, request, window, antenna, start, end, rate, remaining)
            cur = conn.execute("""INSERT INTO schedules(request_id,window_id,station_id,antenna_id,satellite_id,starts_at,ends_at,rate_mbps,
                                      status,resume_of_schedule_id,created_by,created_at,updated_at)
                                  VALUES(?,?,?,?,?,?,?,?, 'receiving', ?,?,?,?)""",
                               (request_id, window_id, station["id"], antenna_id, request["satellite_id"],
                                iso(start), iso(end), float(rate), previous["schedule_id"], actor, iso(), iso()))
            schedule_id = cur.lastrowid
            Repository.ledger(conn, request_id, schedule_id, "resumed", 0.0, prior, actor,
                              {"resume_of_schedule_id": previous["schedule_id"], "remaining_mb": remaining,
                               "segment_capacity_mb": capacity})
            Repository.audit(conn, request_id, schedule_id, actor, role, "receive_resumed",
                             {"window_id": window_id, "antenna_id": antenna_id, "prior_mb": prior,
                              "remaining_mb": remaining, "resume_of_schedule_id": previous["schedule_id"]})
            result = self.get_schedule(schedule_id)
            result["resume_result"] = {"continued_from_mb": prior, "remaining_mb": remaining, "segment_capacity_mb": capacity}
            return result

    def transition(self, schedule_id: int, actor: str, role: str, tenant: str, target: str, body: dict[str, Any]) -> dict[str, Any]:
        with self.repo.tx() as conn:
            row = conn.execute("""SELECT s.*,r.tenant,r.status request_status,r.data_mb FROM schedules s JOIN requests r ON r.id=s.request_id WHERE s.id=?""", (schedule_id,)).fetchone()
            if not row: raise ApiError(404, "schedule_not_found", "排程不存在")
            if target == "receiving":
                if role not in {"operator", "commander"}: raise ApiError(403, "receive_forbidden", "当前角色不能开始接收")
                if row["status"] != "scheduled": raise ApiError(409, "invalid_transition", "只有已排程任务可以开始接收")
                conn.execute("UPDATE schedules SET status='receiving',revision=revision+1,updated_at=? WHERE id=?", (iso(), schedule_id))
            elif target == "received":
                if role not in {"operator", "commander"}: raise ApiError(403, "receive_forbidden", "当前角色不能完成接收")
                if row["status"] != "receiving": raise ApiError(409, "invalid_transition", "只有接收中任务可以完成")
                # 未到目标就结束：拒绝。优先以登记累计量为准，未登记过则按本段计划容量（兼容旧流程）。
                prior = self._prior_received(conn, row["request_id"], schedule_id)
                current = float(row["received_mb"])
                if current <= EPS:
                    current = (parse_time(row["ends_at"]) - parse_time(row["starts_at"])).total_seconds() * float(row["rate_mbps"]) / 8
                cumulative = prior + current
                if cumulative + EPS < float(row["data_mb"]):
                    raise ApiError(409, "incomplete_transfer", "累计接收量未到目标，不能完成；请登记掉线并安排续传",
                                   {"cumulative_mb": cumulative, "target_mb": float(row["data_mb"]),
                                    "missing_mb": float(row["data_mb"]) - cumulative})
                conn.execute("UPDATE schedules SET status='received',received_mb=?,revision=revision+1,updated_at=? WHERE id=?", (current, iso(), schedule_id))
                conn.execute("UPDATE requests SET status='received' WHERE id=?", (row["request_id"],))
                Repository.ledger(conn, row["request_id"], schedule_id, "completed", current, cumulative, actor)
            else:
                raise ApiError(400, "invalid_transition", "未知状态")
            Repository.audit(conn, row["request_id"], schedule_id, actor, role, f"receive_{target}", {})
            return self.get_schedule(schedule_id)

    def cancel_schedule(self, schedule_id: int, actor: str, role: str, tenant: str, body: dict[str, Any]) -> dict[str, Any]:
        reason = str(body.get("reason", "")).strip()
        if not reason: raise ApiError(400, "reason_required", "取消原因必填")
        with self.repo.tx() as conn:
            row = conn.execute("""SELECT s.*,r.tenant FROM schedules s JOIN requests r ON r.id=s.request_id WHERE s.id=?""", (schedule_id,)).fetchone()
            if not row: raise ApiError(404, "schedule_not_found", "排程不存在")
            if role == "requester":
                if row["tenant"] != tenant: raise ApiError(403, "tenant_forbidden", "不能取消其他租户排程")
                if row["status"] != "scheduled": raise ApiError(409, "cancel_not_allowed", "接收开始后租户不能取消")
            elif role not in {"operator", "commander"}: raise ApiError(403, "cancel_forbidden", "当前角色不能取消排程")
            if row["status"] == "received": raise ApiError(409, "received_data_protected", "已接收数据不能取消或删除")
            if row["status"] == "canceled": return self.get_schedule(schedule_id)
            conn.execute("UPDATE schedules SET status='canceled',disposition_reason=?,revision=revision+1,updated_at=? WHERE id=?", (reason, iso(), schedule_id))
            conn.execute("UPDATE requests SET status='pending' WHERE id=?", (row["request_id"],))
            Repository.audit(conn, row["request_id"], schedule_id, actor, role, "schedule_canceled", {"reason": reason})
            return self.get_schedule(schedule_id)

    def emergency_preempt(self, schedule_id: int, actor: str, role: str, body: dict[str, Any]) -> dict[str, Any]:
        if role != "commander": raise ApiError(403, "commander_required", "只有任务指挥官可以执行紧急抢占")
        order_id, reason = str(body.get("order_id", "")).strip(), str(body.get("reason", "")).strip()
        if not order_id or not reason: raise ApiError(400, "emergency_details_required", "order_id 和 reason 必填")
        with self.repo.tx() as conn:
            row = conn.execute("""SELECT s.*,r.priority,r.tenant FROM schedules s JOIN requests r ON r.id=s.request_id WHERE s.id=?""", (schedule_id,)).fetchone()
            if not row: raise ApiError(404, "schedule_not_found", "排程不存在")
            if row["status"] == "received": raise ApiError(409, "received_data_protected", "已接收数据的排程不能被抢占")
            if row["status"] not in {"scheduled", "receiving"}: raise ApiError(409, "invalid_transition", "当前排程不可抢占")
            conn.execute("UPDATE schedules SET status='preempted',disposition_reason=?,revision=revision+1,updated_at=? WHERE id=?", (f"{order_id}: {reason}", iso(), schedule_id))
            conn.execute("UPDATE requests SET status='preempted' WHERE id=?", (row["request_id"],))
            Repository.audit(conn, row["request_id"], schedule_id, actor, role, "emergency_preemption", {"order_id": order_id, "reason": reason, "displaced_priority": row["priority"]})
            return {"schedule": self.get_schedule(schedule_id), "reschedule_required": True, "order_id": order_id}

    def change_window(self, window_id: int, actor: str, role: str, body: dict[str, Any]) -> dict[str, Any]:
        if role not in {"operator", "commander"}: raise ApiError(403, "window_forbidden", "当前角色不能变更可见窗口")
        start, end = parse_time(body.get("starts_at")), parse_time(body.get("ends_at"))
        if end <= start: raise ApiError(400, "invalid_window", "窗口结束时间必须晚于开始时间")
        with self.repo.tx() as conn:
            window = conn.execute("SELECT * FROM visibility_windows WHERE id=?", (window_id,)).fetchone()
            if not window: raise ApiError(404, "window_not_found", "可见窗口不存在")
            rows = conn.execute("SELECT * FROM schedules WHERE window_id=? AND status IN ('scheduled','receiving','received','offline')", (window_id,)).fetchall()
            impacts = []
            for row in rows:
                sched_start, sched_end = parse_time(row["starts_at"]), parse_time(row["ends_at"])
                invalid = sched_start < start or sched_end > end
                if row["status"] == "received":
                    impacts.append({"schedule_id": row["id"], "action": "preserve_received_data", "reason": "已接收数据不可回滚", "invalid": invalid})
                    continue
                if row["status"] == "offline":
                    # 已掉线段已释放时段，不再受窗口缩短影响
                    impacts.append({"schedule_id": row["id"], "action": "offline_preserved", "reason": "掉线段已释放剩余时段，等待续传", "invalid": invalid})
                    continue
                if invalid:
                    if row["status"] == "receiving":
                        # 窗口缩短影响接收任务：转成可恢复的掉线状态，而不是简单抢占。
                        # 截断时刻由系统给出；已登记的接收量照实留档（不按截断时刻容量复核）。
                        cut = utcnow()
                        if cut < sched_start: cut = sched_start
                        if cut > end: cut = end
                        received = float(row["received_mb"])
                        prior = self._prior_received(conn, row["request_id"], row["id"])
                        cumulative = prior + received
                        reason = "visibility_window_shortened"
                        released = max(0, int((sched_end - cut).total_seconds()))
                        conn.execute("""UPDATE schedules SET status='offline',actual_ends_at=?,disposition_reason=?,revision=revision+1,updated_at=? WHERE id=?""",
                                     (iso(cut), reason, iso(), row["id"]))
                        conn.execute("UPDATE requests SET status='receiving' WHERE id=?", (row["request_id"],))
                        Repository.ledger(conn, row["request_id"], row["id"], "offline", received, cumulative, actor,
                                          {"reason": reason, "ended_at": iso(cut), "planned_ends_at": row["ends_at"],
                                           "released_seconds": released, "clamped_to_end": cut == end})
                        impacts.append({"schedule_id": row["id"], "request_id": row["request_id"], "action": "offline_recoverable",
                                        "reason": "窗口缩短中断接收，已转为可恢复掉线", "old_start": row["starts_at"], "old_end": row["ends_at"],
                                        "actual_end": iso(cut), "cumulative_mb": cumulative, "released_seconds": released})
                    else:
                        # 普通待接收任务的现有处理照旧：抢占
                        conn.execute("UPDATE schedules SET status='preempted',disposition_reason=?,revision=revision+1,updated_at=? WHERE id=?", ("visibility_window_changed", iso(), row["id"]))
                        conn.execute("UPDATE requests SET status='preempted' WHERE id=?", (row["request_id"],))
                        impacts.append({"schedule_id": row["id"], "request_id": row["request_id"], "action": "preempted", "reason": "新窗口无法覆盖原排程", "old_start": row["starts_at"], "old_end": row["ends_at"]})
                else:
                    impacts.append({"schedule_id": row["id"], "action": "unchanged", "reason": "新窗口仍覆盖排程"})
            conn.execute("UPDATE visibility_windows SET starts_at=?,ends_at=?,revision=revision+1 WHERE id=?", (iso(start), iso(end), window_id))
            Repository.audit(conn, None, None, actor, role, "visibility_window_changed", {"window_id": window_id, "impacts": impacts})
            return {"window": dict(conn.execute("SELECT * FROM visibility_windows WHERE id=?", (window_id,)).fetchone()), "impacts": impacts}

    def reschedule(self, request_id: int, actor: str, role: str, body: dict[str, Any]) -> dict[str, Any]:
        with self.repo.tx() as conn:
            request = conn.execute("SELECT * FROM requests WHERE id=?", (request_id,)).fetchone()
            if not request: raise ApiError(404, "request_not_found", "请求不存在")
            if request["status"] != "preempted": raise ApiError(409, "reschedule_not_needed", "只有被抢占请求需要重排")
            conn.execute("UPDATE requests SET status='pending' WHERE id=?", (request_id,))
            Repository.audit(conn, request_id, None, actor, role, "reschedule_requested", {"reason": body.get("reason", "")})
            return dict(conn.execute("SELECT * FROM requests WHERE id=?", (request_id,)).fetchone())

    # ---- 状态查询 ----
    def state(self, role: str, tenant: str) -> dict[str, Any]:
        conn = self.repo.conn
        if role == "requester":
            requests = [dict(r) for r in conn.execute("SELECT * FROM requests WHERE tenant=? ORDER BY id DESC", (tenant,))]
            schedules = [dict(r) for r in conn.execute(f"""SELECT s.*,r.tenant,r.data_mb,r.priority request_priority,r.deadline,
                (SELECT COALESCE(SUM(p.received_mb),0) FROM receive_progress p WHERE p.schedule_id=s.id) AS segment_mb,
                {PROGRESS_SELECT.lstrip()}
                FROM schedules s JOIN requests r ON r.id=s.request_id WHERE r.tenant=? ORDER BY s.id DESC""", (tenant,))]
            stations = []
            progress = self._progress_rows(conn, "p.request_id IN (SELECT id FROM requests WHERE tenant=?)", [tenant])
        elif role == "viewer":
            requests = []
            schedules = [dict(r) for r in conn.execute(f"""SELECT s.id,s.status,s.station_id,s.antenna_id,s.starts_at,s.actual_ends_at,s.ends_at,s.disposition_reason,s.received_mb,
                {PROGRESS_SELECT.lstrip()}
                FROM schedules s ORDER BY s.id DESC""")]
            stations = []
            progress = [dict(r) for r in conn.execute("SELECT * FROM receive_progress ORDER BY id DESC LIMIT 200")]
        else:
            requests = [dict(r) for r in conn.execute("SELECT * FROM requests ORDER BY id DESC")]
            schedules = [dict(r) for r in conn.execute(f"{SCHEDULE_SELECT} ORDER BY s.id DESC")]
            stations = [dict(r) for r in conn.execute("SELECT * FROM stations ORDER BY id")]
            progress = [dict(r) for r in conn.execute("SELECT * FROM receive_progress ORDER BY id DESC LIMIT 200")]
        return {"requests": requests, "schedules": schedules, "stations": stations, "receive_progress": progress, "server_time": iso()}
