"""调用耗时标准化的回归测试（标准库 unittest，无需额外依赖）。

运行：cd backend && python -m unittest discover -s tests -v
"""
from __future__ import annotations

import json
import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from app.seed import SEED_ROWS  # noqa: E402
from app.services import call_duration as cd  # noqa: E402
from app.services.api_monitor import ApiMonitorService  # noqa: E402
from app.services.call_log import CallLogService  # noqa: E402
from app.store import store  # noqa: E402


class DurationStandardizationTest(unittest.TestCase):
    def setUp(self) -> None:
        store._tables["call_log"] = [dict(row) for row in SEED_ROWS["call_log"]]
        store._tables["api_monitor"] = [dict(row) for row in SEED_ROWS["api_monitor"]]
        store._tables["retry_queue"] = [dict(row) for row in SEED_ROWS["retry_queue"]]
        cd.reset_runtime_state()

    def record(self, number: str) -> dict:
        return next(row for row in store.rows("call_log") if row["记录编号"] == number)

    # ---- 单位与换算 ----

    def test_manual_unit_is_converted_to_same_milliseconds(self) -> None:
        self.assertEqual(cd.parse_duration("0.2 s")["ms"], 200)
        self.assertEqual(cd.parse_duration("0.2s")["ms"], 200)
        self.assertEqual(cd.parse_duration("200毫秒")["ms"], 200)
        self.assertEqual(cd.parse_duration("1 s")["basis"], "1 s = 1000 ms")

    def test_auto_written_milliseconds_keep_value(self) -> None:
        self.assertEqual(cd.parse_duration(120, default_unit="ms")["ms"], 120)
        self.assertEqual(
            cd.parse_duration(self.record("CALL-0002")["响应耗时(ms)"], default_unit="ms")["ms"],
            120,
        )

    def test_bare_number_is_not_zero_or_default_unit(self) -> None:
        parsed = cd.parse_duration(32)
        self.assertFalse(parsed["resolved"])
        self.assertIsNone(parsed["ms"])
        self.assertIn("缺少单位", parsed["reason"])
        effective = cd.resolve_effective(self.record("CALL-0003"))
        self.assertFalse(effective["resolved"])
        self.assertIsNone(effective["ms"])

    def test_parse_rejects_negative_and_garbage(self) -> None:
        self.assertFalse(cd.parse_duration(-3, default_unit="ms")["resolved"])
        self.assertFalse(cd.parse_duration("abc")["resolved"])
        self.assertFalse(cd.parse_duration("")["resolved"])

    # ---- 回填分批、续跑、影子预览 ----

    def test_dry_run_does_not_touch_records(self) -> None:
        before = json.dumps(store.rows("call_log"), sort_keys=True, default=str)
        result = cd.run_backfill(dry_run=True)
        after = json.dumps(store.rows("call_log"), sort_keys=True, default=str)
        self.assertEqual(before, after)
        self.assertEqual(result["would_convert"], 5)
        self.assertEqual(
            [item["记录编号"] for item in result["unconverted_records"]], ["CALL-0003"]
        )

    def test_staged_backfill_resumes_same_batch(self) -> None:
        first = cd.run_backfill(limit=2)
        self.assertEqual(first["status"], "中断")
        self.assertEqual(first["remaining"], 4)
        second = cd.run_backfill(limit=2)
        self.assertEqual(second["batch_id"], first["batch_id"])
        third = cd.run_backfill(limit=2)
        self.assertEqual(third["status"], "已完成")
        self.assertEqual(third["remaining"], 0)
        self.assertEqual(third["converted"], 5)
        self.assertEqual(third["verified"], 6)
        self.assertEqual(third["corrected"], 0)

    def test_completed_batch_is_idempotent(self) -> None:
        cd.run_backfill()
        revisions = [row.get("耗时修订") for row in store.rows("call_log")]
        again = cd.run_backfill()
        self.assertEqual(again["converted"], 0)
        self.assertEqual([row.get("耗时修订") for row in store.rows("call_log")], revisions)

    # ---- 监控计算与分母 ----

    def test_monitor_computes_mixed_records_during_interruption(self) -> None:
        cd.run_backfill(limit=2)  # CALL-0001、CALL-0002 已换算，CALL-0003 未迁移
        monitor = store.find("api_monitor", 1)
        ApiMonitorService()._collect_call_log_stats(monitor)
        stats = monitor["stats"]
        self.assertEqual(stats["latency_ms"], 160)  # (200 + 120) / 2
        self.assertEqual(stats["latency_sample_used"], 2)
        self.assertEqual(stats["latency_unconverted"], 1)
        self.assertIsNone(cd.resolve_effective(self.record("CALL-0003"))["ms"])

    def test_monitor_result_is_written_back(self) -> None:
        cd.run_backfill()
        monitor = store.find("api_monitor", 1)
        ApiMonitorService()._collect_call_log_stats(monitor)
        self.assertEqual(monitor["规范耗时(ms)"], 160)
        self.assertEqual(monitor["耗时单位"], "ms")
        self.assertIn("样本2/3", monitor["耗时换算依据"])

    # ---- 幂等登记 ----

    def test_duplicate_submission_keeps_single_record(self) -> None:
        before = len(store.rows("call_log"))
        for _ in range(2):
            CallLogService().create_entry({
                "记录编号": "CALL-0001",
                "接口端点": "ENDP-0001",
                "请求参数": "dup",
                "响应耗时": "0.2 s",
            })
        self.assertEqual(len(store.rows("call_log")), before)
        self.assertEqual(self.record("CALL-0001")["规范耗时(ms)"], 200)

    def test_new_record_is_standardized_on_ingest(self) -> None:
        entry, missing = CallLogService().create_entry({
            "记录编号": "CALL-NEW-1",
            "接口端点": "ENDP-0001",
            "请求参数": "{}",
            "响应耗时": "0.2 s",
        })
        self.assertEqual(missing, [])
        self.assertEqual(entry["规范耗时(ms)"], 200)
        self.assertEqual(entry["耗时换算依据"], "1 s = 1000 ms")

    # ---- 回滚与重算 ----

    def test_rollback_clears_standard_but_keeps_legacy(self) -> None:
        cd.run_backfill()
        result = cd.rollback()
        self.assertEqual(result["affected"], 6)
        self.assertEqual(result["legacy_readable"]["readable"], 6)
        for row in store.rows("call_log"):
            self.assertNotIn("规范耗时(ms)", row)
            self.assertTrue("响应耗时" in row or "响应耗时(ms)" in row)
        self.assertEqual(self.record("CALL-0001")["响应耗时"], "0.2 s")
        self.assertEqual(self.record("CALL-0002")["响应耗时(ms)"], 120)

    def test_rollback_plus_recompute_with_corrected_unit(self) -> None:
        cd.run_backfill()
        cd.rollback()
        result = cd.recompute(unit="ms")
        self.assertTrue(result["ok"])
        self.assertEqual(result["unconverted"], 0)
        self.assertEqual(self.record("CALL-0003")["规范耗时(ms)"], 32)
        self.assertTrue(
            self.record("CALL-0003")["耗时换算依据"].startswith("人工校对单位")
        )
        self.assertEqual(self.record("CALL-0003")["响应耗时"], 32)

    def test_recompute_rejects_unknown_unit(self) -> None:
        result = cd.recompute(unit="furlong")
        self.assertFalse(result["ok"])

    # ---- 报告 ----

    def test_report_contains_adopted_values_and_denominator(self) -> None:
        cd.run_backfill()
        report = cd.migration_report()
        self.assertEqual(report["totals"]["records"], 6)
        self.assertEqual(report["totals"]["adopted"], 5)
        self.assertEqual(report["sample"]["denominator"], 5)
        self.assertEqual(report["sample"]["total_records"], 6)
        adopted = {item["记录编号"]: item for item in report["adopted"]}
        self.assertEqual(adopted["CALL-0001"]["canonical_ms"], 200)
        self.assertEqual(adopted["CALL-0001"]["basis"], "1 s = 1000 ms")
        self.assertEqual(adopted["CALL-0002"]["canonical_ms"], 120)
        self.assertEqual(
            [item["记录编号"] for item in report["unconverted"]], ["CALL-0003"]
        )
        self.assertEqual(report["legacy_readable"]["readable"], 6)


if __name__ == "__main__":
    unittest.main()
