import sys, tempfile, unittest
from datetime import timedelta
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from app import ApiError, SatelliteSchedulingService, iso, utcnow


class SatelliteFlowTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(); self.svc = SatelliteSchedulingService(Path(self.tmp.name) / "test.db"); self.now = utcnow() + timedelta(hours=1)
        self.svc.create_satellite("op", "operator", {"id": "SAT1", "name": "遥感一号", "data_rate_mbps": 100, "priority": 8, "storage_capacity_mb": 100000, "tenant": "T1"})
        self.svc.create_station("op", "operator", {"id": "GS1", "name": "北京站", "weather": "clear"})
        self.svc.create_antenna("op", "operator", {"id": "ANT1", "station_id": "GS1", "max_rate_mbps": 80})
        self.window = self.svc.create_window("op", "operator", {"satellite_id": "SAT1", "station_id": "GS1", "starts_at": iso(self.now), "ends_at": iso(self.now + timedelta(hours=2)), "max_rate_mbps": 70})
        self.svc.set_quota("op", "operator", {"tenant": "T1", "station_id": "GS1", "daily_seconds": 7200})

    def tearDown(self): self.tmp.cleanup()

    def request(self, mb=35000):
        return self.svc.create_request("requester-t1", "requester", "T1", {"satellite_id": "SAT1", "data_mb": mb, "priority": 7, "deadline": iso(self.now + timedelta(days=1))})

    def schedule(self, request_id, start, end, rate=60):
        return self.svc.schedule_request(request_id, "op", "operator", {"window_id": self.window["id"], "antenna_id": "ANT1", "starts_at": iso(start), "ends_at": iso(end), "rate_mbps": rate})

    def test_complete_receive_and_window_change_impact(self):
        req = self.request(); schedule = self.schedule(req["id"], self.now, self.now + timedelta(hours=1, minutes=30))
        self.assertEqual(schedule["status"], "scheduled")
        self.svc.transition(schedule["id"], "op", "operator", "", "receiving", {})
        self.svc.register_progress(schedule["id"], "op", "operator", {"received_mb": 35000})
        self.svc.transition(schedule["id"], "op", "operator", "", "received", {})
        req2 = self.request(10000)
        schedule2 = self.schedule(req2["id"], self.now + timedelta(hours=1), self.now + timedelta(hours=1, minutes=30), 50)
        changed = self.svc.change_window(self.window["id"], "op", "operator", {"starts_at": iso(self.now), "ends_at": iso(self.now + timedelta(hours=1, minutes=10))})
        impacts = {x["schedule_id"]: x for x in changed["impacts"]}
        self.assertEqual(impacts[schedule["id"]]["action"], "preserve_received_data")
        self.assertEqual(impacts[schedule2["id"]]["action"], "preempted")
        self.assertEqual(self.svc.get_schedule(schedule2["id"])["status"], "preempted")

    def test_conflicts_permissions_and_data_protection(self):
        req = self.request(10000); schedule = self.schedule(req["id"], self.now, self.now + timedelta(minutes=30), 50)
        req2 = self.request(10000)
        with self.assertRaises(ApiError) as ctx:
            self.svc.schedule_request(req2["id"], "requester-t1", "requester", {"window_id": self.window["id"], "antenna_id": "ANT1", "starts_at": iso(self.now), "ends_at": iso(self.now + timedelta(minutes=20)), "rate_mbps": 50})
        self.assertEqual(ctx.exception.status, 403)
        with self.assertRaises(ApiError) as ctx:
            self.svc.schedule_request(req2["id"], "op", "operator", {"window_id": self.window["id"], "antenna_id": "ANT1", "starts_at": iso(self.now + timedelta(minutes=10)), "ends_at": iso(self.now + timedelta(minutes=40)), "rate_mbps": 50})
        self.assertEqual(ctx.exception.code, "antenna_conflict")
        self.svc.transition(schedule["id"], "op", "operator", "", "receiving", {})
        self.svc.register_progress(schedule["id"], "op", "operator", {"received_mb": 10000})
        self.svc.transition(schedule["id"], "op", "operator", "", "received", {})
        with self.assertRaises(ApiError) as ctx:
            self.svc.cancel_schedule(schedule["id"], "op", "operator", "", {"reason": "测试"})
        self.assertEqual(ctx.exception.code, "received_data_protected")


class ResumableReceiveTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(); self.svc = SatelliteSchedulingService(Path(self.tmp.name) / "test.db"); self.now = utcnow() + timedelta(hours=1)
        self.svc.create_satellite("op", "operator", {"id": "SAT1", "name": "遥感一号", "data_rate_mbps": 100, "priority": 8, "storage_capacity_mb": 100000, "tenant": "T1"})
        self.svc.create_station("op", "operator", {"id": "GS1", "name": "北京站", "weather": "clear"})
        self.svc.create_antenna("op", "operator", {"id": "ANT1", "station_id": "GS1", "max_rate_mbps": 80})
        self.window = self.svc.create_window("op", "operator", {"satellite_id": "SAT1", "station_id": "GS1", "starts_at": iso(self.now), "ends_at": iso(self.now + timedelta(hours=2)), "max_rate_mbps": 70})
        self.svc.set_quota("op", "operator", {"tenant": "T1", "station_id": "GS1", "daily_seconds": 7200})

    def tearDown(self): self.tmp.cleanup()

    def request(self, mb=35000):
        return self.svc.create_request("requester-t1", "requester", "T1", {"satellite_id": "SAT1", "data_mb": mb, "priority": 7, "deadline": iso(self.now + timedelta(days=1))})

    def schedule(self, request_id, start, end, rate=60):
        return self.svc.schedule_request(request_id, "op", "operator", {"window_id": self.window["id"], "antenna_id": "ANT1", "starts_at": iso(start), "ends_at": iso(end), "rate_mbps": rate})

    def resume(self, request_id, start, end, rate=60):
        return self.svc.resume_request(request_id, "op", "operator", {"window_id": self.window["id"], "antenna_id": "ANT1", "starts_at": iso(start), "ends_at": iso(end), "rate_mbps": rate})

    def receiving(self, mb=35000):
        req = self.request(mb); schedule = self.schedule(req["id"], self.now, self.now + timedelta(hours=1, minutes=30))
        self.svc.transition(schedule["id"], "op", "operator", "", "receiving", {})
        return req, schedule

    def test_complete_requires_target_data(self):
        req, schedule = self.receiving()
        self.svc.register_progress(schedule["id"], "op", "operator", {"received_mb": 20000})
        with self.assertRaises(ApiError) as ctx:
            self.svc.transition(schedule["id"], "op", "operator", "", "received", {})
        self.assertEqual(ctx.exception.code, "incomplete_reception")
        self.assertEqual(ctx.exception.details["received_mb"], 20000)
        self.svc.register_progress(schedule["id"], "op", "operator", {"received_mb": 35000})
        self.assertEqual(self.svc.transition(schedule["id"], "op", "operator", "", "received", {})["status"], "received")

    def test_progress_is_cumulative_and_monotonic(self):
        req, schedule = self.receiving()
        with self.assertRaises(ApiError) as ctx:
            self.svc.register_progress(schedule["id"], "op", "operator", {"received_mb": -5})
        self.assertEqual(ctx.exception.code, "invalid_progress")
        prog = self.svc.register_progress(schedule["id"], "op", "operator", {"received_mb": 20000})
        self.assertEqual(prog["request_received_mb"], 20000)
        self.assertEqual(prog["remaining_mb"], 15000)
        with self.assertRaises(ApiError) as ctx:
            self.svc.register_progress(schedule["id"], "op", "operator", {"received_mb": 15000})
        self.assertEqual(ctx.exception.code, "progress_regression")

    def test_dropout_releases_slot_and_resume_continues(self):
        req, schedule = self.receiving()
        self.svc.register_progress(schedule["id"], "op", "operator", {"received_mb": 20000})
        with self.assertRaises(ApiError) as ctx:
            self.svc.drop_schedule(schedule["id"], "op", "operator", {})
        self.assertEqual(ctx.exception.code, "reason_required")
        dropped = self.svc.drop_schedule(schedule["id"], "op", "operator", {"reason": "链路闪断"})
        self.assertEqual(dropped["schedule"]["status"], "dropped")
        self.assertEqual(dropped["remaining_mb"], 15000)
        self.assertEqual(dropped["released_slot"]["ends_at"], schedule["ends_at"])
        # 掉线请求不能走普通排程入口
        with self.assertRaises(ApiError) as ctx:
            self.schedule(req["id"], self.now, self.now + timedelta(minutes=30))
        self.assertEqual(ctx.exception.code, "request_closed")
        # 续传容量按剩余量复核：10 分钟 * 60Mbps = 4500MB < 15000MB
        with self.assertRaises(ApiError) as ctx:
            self.resume(req["id"], self.now, self.now + timedelta(minutes=10))
        self.assertEqual(ctx.exception.code, "insufficient_capacity")
        self.assertEqual(ctx.exception.details["required_mb"], 15000)
        # 剩余时段已释放：续传可与原掉线排程重叠
        resumed = self.resume(req["id"], self.now + timedelta(minutes=30), self.now + timedelta(hours=1, minutes=30), 50)
        self.assertEqual(resumed["previously_received_mb"], 20000)
        self.assertEqual(resumed["remaining_mb"], 15000)
        resumed_id = resumed["schedule"]["id"]
        self.assertNotEqual(resumed_id, schedule["id"])
        self.svc.transition(resumed_id, "op", "operator", "", "receiving", {})
        self.svc.register_progress(resumed_id, "op", "operator", {"received_mb": 15000})
        self.assertEqual(self.svc.transition(resumed_id, "op", "operator", "", "received", {})["status"], "received")
        # 历史进度不被覆盖
        old = self.svc.get_schedule(schedule["id"])
        self.assertEqual(old["status"], "dropped")
        self.assertEqual(old["received_mb"], 20000)
        self.assertEqual(old["disposition_reason"], "链路闪断")
        ledger = self.svc.request_progress(req["id"], "operator", "")
        self.assertEqual(ledger["received_mb"], 35000)
        self.assertEqual(ledger["remaining_mb"], 0)
        self.assertEqual([e["event"] for e in ledger["entries"]], ["progress", "dropout", "resume", "progress", "completed"])

    def test_window_shrink_drops_receiving_but_preempts_scheduled(self):
        req1, schedule1 = self.receiving()
        self.svc.register_progress(schedule1["id"], "op", "operator", {"received_mb": 20000})
        req2 = self.request(10000)
        schedule2 = self.schedule(req2["id"], self.now + timedelta(hours=1, minutes=30), self.now + timedelta(hours=2), 50)
        changed = self.svc.change_window(self.window["id"], "op", "operator", {"starts_at": iso(self.now), "ends_at": iso(self.now + timedelta(hours=1))})
        impacts = {x["schedule_id"]: x for x in changed["impacts"]}
        self.assertEqual(impacts[schedule1["id"]]["action"], "dropped_resumable")
        self.assertEqual(impacts[schedule1["id"]]["received_mb"], 20000)
        self.assertEqual(impacts[schedule1["id"]]["remaining_mb"], 15000)
        self.assertEqual(impacts[schedule2["id"]]["action"], "preempted")
        self.assertEqual(self.svc.get_schedule(schedule1["id"])["status"], "dropped")
        self.assertEqual(self.svc.get_schedule(schedule1["id"])["disposition_reason"], "visibility_window_changed")
        # 掉线排程不可取消，请求可续传
        with self.assertRaises(ApiError) as ctx:
            self.svc.cancel_schedule(schedule1["id"], "op", "operator", "", {"reason": "测试"})
        self.assertEqual(ctx.exception.code, "dropout_archived")
        resumed = self.resume(req1["id"], self.now, self.now + timedelta(hours=1))
        self.assertEqual(resumed["remaining_mb"], 15000)
        # 普通待接收请求照旧走重排
        self.assertEqual(self.svc.reschedule(req2["id"], "op", "operator", {})["status"], "pending")

    def test_resume_permissions_and_guards(self):
        req, schedule = self.receiving(10000)
        with self.assertRaises(ApiError) as ctx:
            self.svc.register_progress(schedule["id"], "requester-t1", "requester", {"received_mb": 100})
        self.assertEqual(ctx.exception.status, 403)
        with self.assertRaises(ApiError) as ctx:
            self.svc.drop_schedule(schedule["id"], "requester-t1", "requester", {"reason": "链路闪断"})
        self.assertEqual(ctx.exception.status, 403)
        self.svc.drop_schedule(schedule["id"], "op", "operator", {"reason": "链路闪断"})
        with self.assertRaises(ApiError) as ctx:
            self.svc.resume_request(req["id"], "requester-t1", "requester", {"window_id": self.window["id"], "antenna_id": "ANT1", "starts_at": iso(self.now), "ends_at": iso(self.now + timedelta(minutes=30)), "rate_mbps": 50})
        self.assertEqual(ctx.exception.status, 403)
        req2 = self.request(10000)
        with self.assertRaises(ApiError) as ctx:
            self.resume(req2["id"], self.now, self.now + timedelta(minutes=30), 50)
        self.assertEqual(ctx.exception.code, "resume_not_needed")
        with self.assertRaises(ApiError) as ctx:
            self.svc.request_progress(req["id"], "requester", "OTHER")
        self.assertEqual(ctx.exception.code, "tenant_forbidden")


if __name__ == "__main__": unittest.main()
