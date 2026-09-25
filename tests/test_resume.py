import sys, tempfile, unittest
from datetime import timedelta
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from app import ApiError, SatelliteSchedulingService, iso, utcnow


class ResumeFlowTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.svc = SatelliteSchedulingService(Path(self.tmp.name) / "test.db")
        self.now = utcnow() + timedelta(hours=1)
        self.svc.create_satellite("op", "operator", {"id": "SAT1", "name": "遥感一号", "data_rate_mbps": 100, "priority": 8, "storage_capacity_mb": 100000, "tenant": "T1"})
        self.svc.create_station("op", "operator", {"id": "GS1", "name": "北京站", "weather": "clear"})
        self.svc.create_antenna("op", "operator", {"id": "ANT1", "station_id": "GS1", "max_rate_mbps": 80})
        self.window1 = self.svc.create_window("op", "operator", {"satellite_id": "SAT1", "station_id": "GS1", "starts_at": iso(self.now), "ends_at": iso(self.now + timedelta(hours=2)), "max_rate_mbps": 70})
        self.window2 = self.svc.create_window("op", "operator", {"satellite_id": "SAT1", "station_id": "GS1", "starts_at": iso(self.now + timedelta(hours=3)), "ends_at": iso(self.now + timedelta(hours=6)), "max_rate_mbps": 70})
        self.svc.set_quota("op", "operator", {"tenant": "T1", "station_id": "GS1", "daily_seconds": 30000})

    def tearDown(self):
        self.tmp.cleanup()

    def request(self, mb=30000):
        return self.svc.create_request("requester-t1", "requester", "T1", {"satellite_id": "SAT1", "data_mb": mb, "priority": 7, "deadline": iso(self.now + timedelta(days=1))})

    def schedule_first(self, req, minutes=70, rate=60):
        return self.svc.schedule_request(req["id"], "op", "operator", {
            "window_id": self.window1["id"], "antenna_id": "ANT1",
            "starts_at": iso(self.now), "ends_at": iso(self.now + timedelta(minutes=minutes)),
            "rate_mbps": rate})

    def test_offline_releases_window_and_resume_continues_cumulative(self):
        req = self.request(30000)  # 需 4000s @60Mbps
        seg = self.schedule_first(req)
        self.svc.transition(seg["id"], "op", "operator", "", "receiving", {})
        # 接收过程登记累计数据量
        self.svc.register_progress(seg["id"], "op", "operator", {"received_mb": 9000})
        with self.assertRaises(ApiError) as ctx:
            self.svc.register_progress(seg["id"], "op", "operator", {"received_mb": 8000})
        self.assertEqual(ctx.exception.code, "progress_regressed")
        # 30 分钟处闪断：已收 13500 MB，释放后 40 分钟
        offline = self.svc.mark_offline(seg["id"], "op", "operator",
                                        {"reason": "链路闪断-雨衰", "ends_at": iso(self.now + timedelta(minutes=30)), "received_mb": 13500})
        self.assertEqual(offline["status"], "offline")
        self.assertEqual(offline["disposition_reason"], "链路闪断-雨衰")
        self.assertEqual(offline["released_window"]["seconds"], 2400)
        req_state = next(r for r in self.svc.state("operator", "")["requests"] if r["id"] == req["id"])
        self.assertEqual(req_state["status"], "receiving")
        # 未到目标直接 complete 被拒绝
        with self.assertRaises(ApiError) as ctx:
            self.svc.transition(seg["id"], "op", "operator", "", "received", {})
        self.assertEqual(ctx.exception.code, "invalid_transition")
        # 掉线请求必须走续传入口
        with self.assertRaises(ApiError) as ctx:
            self.schedule_first(req)
        self.assertEqual(ctx.exception.code, "resume_required")
        # 释放出的尾段可被新任务占用（旧段 actual_end 已截断）
        other = self.request(10000)
        tail = self.svc.schedule_request(other["id"], "op", "operator", {
            "window_id": self.window1["id"], "antenna_id": "ANT1",
            "starts_at": iso(self.now + timedelta(minutes=40)), "ends_at": iso(self.now + timedelta(hours=1, minutes=10)), "rate_mbps": 50})
        self.assertEqual(tail["status"], "scheduled")
        # 续传：新窗口新时段，按剩余 16500 MB 复核（>=2200s）
        resumed = self.svc.resume_request(req["id"], "op", "operator", {
            "window_id": self.window2["id"], "antenna_id": "ANT1",
            "starts_at": iso(self.now + timedelta(hours=3)), "ends_at": iso(self.now + timedelta(hours=3, minutes=40)),
            "rate_mbps": 60})
        self.assertEqual(resumed["status"], "receiving")
        self.assertEqual(resumed["resume_of_schedule_id"], seg["id"])
        self.assertAlmostEqual(resumed["resume_result"]["continued_from_mb"], 13500, places=3)
        self.assertAlmostEqual(resumed["resume_result"]["remaining_mb"], 16500, places=3)
        self.assertEqual(resumed["cumulative_mb"], 13500.0)
        # 接着累计：在新段登记，历史进度不被覆盖
        self.svc.register_progress(resumed["id"], "op", "operator", {"received_mb": 16500})
        done = self.svc.transition(resumed["id"], "op", "operator", "", "received", {})
        self.assertEqual(done["status"], "received")
        state = self.svc.state("operator", "")
        kinds = [p["kind"] for p in state["receive_progress"] if p["request_id"] == req["id"]]
        self.assertEqual(kinds.count("offline"), 1)
        self.assertIn("resumed", kinds)
        self.assertIn("completed", kinds)
        # 历史掉线段留档保持 13500，未被覆盖（该段时点累计即 13500）
        old = self.svc.get_schedule(seg["id"])
        self.assertEqual(old["status"], "offline")
        self.assertEqual(old["received_mb"], 13500.0)
        self.assertEqual(old["cumulative_mb"], 13500.0)

    def test_complete_rejected_when_target_not_reached(self):
        req = self.request(30000)
        seg = self.schedule_first(req)
        self.svc.transition(seg["id"], "op", "operator", "", "receiving", {})
        self.svc.register_progress(seg["id"], "op", "operator", {"received_mb": 1000})
        with self.assertRaises(ApiError) as ctx:
            self.svc.transition(seg["id"], "op", "operator", "", "received", {})
        self.assertEqual(ctx.exception.code, "incomplete_transfer")
        self.assertAlmostEqual(ctx.exception.details["missing_mb"], 29000, places=3)
        with self.assertRaises(ApiError) as ctx:
            self.svc.mark_offline(seg["id"], "op", "operator", {"reason": ""})
        self.assertEqual(ctx.exception.code, "reason_required")

    def test_resume_checks_remaining_capacity_weather_maintenance_quota_deadline(self):
        req = self.request(30000)
        seg = self.schedule_first(req)
        self.svc.transition(seg["id"], "op", "operator", "", "receiving", {})
        self.svc.mark_offline(seg["id"], "op", "operator",
                              {"reason": "闪断", "ends_at": iso(self.now + timedelta(minutes=30)), "received_mb": 13500})
        payload = {"window_id": self.window2["id"], "antenna_id": "ANT1",
                   "starts_at": iso(self.now + timedelta(hours=3)), "ends_at": iso(self.now + timedelta(hours=3, minutes=20)),
                   "rate_mbps": 60}
        # 20 分钟容量 9000 < 剩余 16500
        with self.assertRaises(ApiError) as ctx:
            self.svc.resume_request(req["id"], "op", "operator", payload)
        self.assertEqual(ctx.exception.code, "insufficient_capacity")
        # 天气
        self.svc.create_station("op", "operator", {"id": "GS1", "name": "北京站", "weather": "rain"})
        with self.assertRaises(ApiError) as ctx:
            self.svc.resume_request(req["id"], "op", "operator", {**payload, "ends_at": iso(self.now + timedelta(hours=3, minutes=40))})
        self.assertEqual(ctx.exception.code, "weather_blocked")
        self.svc.create_station("op", "operator", {"id": "GS1", "name": "北京站", "weather": "clear"})
        # 维护
        self.svc.create_maintenance("op", "operator", {"station_id": "GS1", "antenna_id": "ANT1",
                                                       "starts_at": iso(self.now + timedelta(hours=3, minutes=10)),
                                                       "ends_at": iso(self.now + timedelta(hours=3, minutes=20)), "reason": "巡检"})
        with self.assertRaises(ApiError) as ctx:
            self.svc.resume_request(req["id"], "op", "operator", {**payload, "ends_at": iso(self.now + timedelta(hours=3, minutes=40))})
        self.assertEqual(ctx.exception.code, "maintenance_conflict")
        # 截止时刻：先排好第二个请求的段（避开第一个掉线段），再制造它的掉线
        short = self.svc.create_request("requester-t1", "requester", "T1", {"satellite_id": "SAT1", "data_mb": 30000, "priority": 7, "deadline": iso(self.now + timedelta(hours=3, minutes=30))})
        seg2 = self.svc.schedule_request(short["id"], "op", "operator", {
            "window_id": self.window1["id"], "antenna_id": "ANT1",
            "starts_at": iso(self.now + timedelta(minutes=35)), "ends_at": iso(self.now + timedelta(hours=1, minutes=45)), "rate_mbps": 60})
        self.svc.transition(seg2["id"], "op", "operator", "", "receiving", {})
        self.svc.mark_offline(seg2["id"], "op", "operator", {"reason": "闪断", "ends_at": iso(self.now + timedelta(hours=1)), "received_mb": 11250})
        with self.assertRaises(ApiError) as ctx:
            self.svc.resume_request(short["id"], "op", "operator", {**payload, "ends_at": iso(self.now + timedelta(hours=3, minutes=40))})
        self.assertEqual(ctx.exception.code, "deadline_missed")
        # 非掉线请求不能续传
        fresh = self.request(5000)
        with self.assertRaises(ApiError) as ctx:
            self.svc.resume_request(fresh["id"], "op", "operator", {**payload, "ends_at": iso(self.now + timedelta(hours=3, minutes=40))})
        self.assertEqual(ctx.exception.code, "resume_not_needed")

    def test_window_shortened_turns_receiving_into_recoverable_offline(self):
        req = self.request(30000)
        seg = self.schedule_first(req)
        self.svc.transition(seg["id"], "op", "operator", "", "receiving", {})
        self.svc.register_progress(seg["id"], "op", "operator", {"received_mb": 4500})
        # 普通待接收任务照旧抢占
        other = self.request(10000)
        pending = self.svc.schedule_request(other["id"], "op", "operator", {
            "window_id": self.window1["id"], "antenna_id": "ANT1",
            "starts_at": iso(self.now + timedelta(hours=1, minutes=30)), "ends_at": iso(self.now + timedelta(hours=2)), "rate_mbps": 50})
        changed = self.svc.change_window(self.window1["id"], "op", "operator",
                                         {"starts_at": iso(self.now), "ends_at": iso(self.now + timedelta(minutes=40))})
        actions = {x["schedule_id"]: x for x in changed["impacts"]}
        self.assertEqual(actions[seg["id"]]["action"], "offline_recoverable")
        self.assertEqual(actions[pending["id"]]["action"], "preempted")
        seg = self.svc.get_schedule(seg["id"])
        self.assertEqual(seg["status"], "offline")
        self.assertEqual(seg["disposition_reason"], "visibility_window_shortened")
        self.assertEqual(seg["cumulative_mb"], 4500.0)
        # 仍可用 window2 续传完成
        resumed = self.svc.resume_request(req["id"], "op", "operator", {
            "window_id": self.window2["id"], "antenna_id": "ANT1",
            "starts_at": iso(self.now + timedelta(hours=3)), "ends_at": iso(self.now + timedelta(hours=4)), "rate_mbps": 60})
        self.svc.register_progress(resumed["id"], "op", "operator", {"received_mb": 25500})
        done = self.svc.transition(resumed["id"], "op", "operator", "", "received", {})
        self.assertEqual(done["status"], "received")

    def test_progress_ledger_is_append_only(self):
        req = self.request(30000)
        seg = self.schedule_first(req)
        self.svc.transition(seg["id"], "op", "operator", "", "receiving", {})
        self.svc.register_progress(seg["id"], "op", "operator", {"received_mb": 1000})
        self.svc.register_progress(seg["id"], "op", "operator", {"received_mb": 5000})
        self.svc.mark_offline(seg["id"], "op", "operator",
                              {"reason": "闪断", "ends_at": iso(self.now + timedelta(minutes=30)), "received_mb": 13500})
        progress = [p for p in self.svc.state("operator", "")["receive_progress"] if p["request_id"] == req["id"]]
        # 倒序返回：offline + 两条 checkpoint，历史条目原样保留
        self.assertEqual([p["kind"] for p in progress[:3]], ["offline", "checkpoint", "checkpoint"])
        self.assertEqual([p["received_mb"] for p in progress if p["kind"] == "checkpoint"], [5000.0, 1000.0])


if __name__ == "__main__":
    unittest.main()
