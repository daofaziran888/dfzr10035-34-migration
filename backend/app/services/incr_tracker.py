"""增量追踪领域服务：管理数据增量游标追踪，与 request_batch、call_log、retry_queue、data_version 联动。"""
from __future__ import annotations

from app.services import call_duration
from app.services.base import BaseModuleService
from app.store import store


class IncrTrackerService(BaseModuleService):
    MODULE = "incr_tracker"
    ENTITY = "增量标记"
    OPERATION_TERM = "采集"
    LIST_FIELDS = ["标记编号", "目标端点", "增量字段", "起始游标", "当前游标", "追踪间隔", "数据来源", "最后同步"]
    REQUIRED_FIELDS = ["标记编号", "目标端点", "增量字段"]
    STATUS_ORDER = ["待初始化", "追踪中", "已追上", "追踪异常"]
    ACTION_RULES = {"初始化游标": "追踪中", "启动追踪": "已追上", "标记异常": "追踪异常"}
    NEGATIVE_ACTIONS = []

    def run_action(self, entry_id: int, action: str):
        """初始化游标时写调用日志；追踪中时联动重放队列挂钩；异常时触发治理报告。"""
        entry, message = super().run_action(entry_id, action)
        if entry is None:
            return entry, message
        if action == "初始化游标":
            self._log_tracker_event(entry, "init")
            message += "，已记录调用日志"
        elif action == "启动追踪":
            self._check_call_logs_for_cursor(entry)
            self._sync_retry_queue_hook(entry)
            message += "，已从调用日志回填游标并同步重放队列"
        elif action == "标记异常":
            self._trigger_govern_and_retry(entry)
            message += "，已触发治理报告和重放任务"
        return entry, message

    def _log_tracker_event(self, tracker: dict, event: str):
        log_rows = store.rows("call_log")
        tid = str(tracker["id"]).zfill(4)
        log_rows.append({
            "id": max((int(r.get("id", 0)) for r in log_rows), default=0) + 1,
            "记录编号": f"CALL-INCR-{tid}",
            "接口端点": tracker.get("目标端点", ""),
            "请求参数": f"tracker_event:{event}",
            "响应状态": "200",
            "响应耗时(ms)": 20,
            "返回行数": 0,
            "错误信息": "",
            "调用时间": "2026-11-17",
            "status": "成功", "pending": False, "abnormal": False,
        })
        call_duration.standardize_record(log_rows[-1], batch_id="PIPELINE")

    def _check_call_logs_for_cursor(self, tracker: dict):
        """从调用日志回填游标位置。"""
        endpoint = tracker.get("目标端点", "")
        logs = [l for l in store.rows("call_log") if l.get("接口端点") == endpoint and l.get("status") == "成功"]
        if logs:
            last = logs[-1]
            tracker["当前游标"] = str(last.get("id", "0"))
            tracker["最后同步"] = "2026-11-17"

    def _sync_retry_queue_hook(self, tracker: dict):
        """回溯重放队列中关联此端点的任务。"""
        endpoint = tracker.get("目标端点", "")
        for retry in store.rows("retry_queue"):
            if retry.get("失败端点") == endpoint and retry.get("status") == "待重试":
                retry["队列状态"] = f"tracker_hook:{tracker.get('增量字段', '')}"

    def _trigger_govern_and_retry(self, tracker: dict):
        """追踪异常触发治理报告和重放任务。"""
        tid = str(tracker["id"]).zfill(4)
        endpoint = tracker.get("目标端点", "")
        # 治理报告
        gov_rows = store.rows("govern_report")
        gov_rows.append({
            "id": max((int(r.get("id", 0)) for r in gov_rows), default=0) + 1,
            "报告编号": f"GOVE-INCR-{tid}",
            "评估范围": f"增量追踪异常:{endpoint}",
            "合规指标": "增量采集完整性",
            "当前得分": 50.0,
            "改进事项": f"端点{endpoint}增量追踪异常，字段{tracker.get('增量字段', '')}",
            "评估周期": "2026-Q4",
            "负责人": "数据采集组",
            "生成时间": "2026-11-17",
            "status": "待生成", "pending": True, "abnormal": True,
        })
        # 重放任务
        retry_rows = store.rows("retry_queue")
        retry_rows.append({
            "id": max((int(r.get("id", 0)) for r in retry_rows), default=0) + 1,
            "任务编号": f"RETR-INCR-{tid}",
            "失败端点": endpoint,
            "失败原因": f"增量追踪异常:游标={tracker.get('当前游标', '')}",
            "重试次数": 0,
            "最大重试": 5,
            "退避策略": "exponential:5s/baseline",
            "最后重试": "",
            "队列状态": "incr_tracker_error",
            "status": "待重试", "pending": True, "abnormal": True,
        })

    def check_incremental_health(self, endpoint: str) -> dict:
        """跨模块增量健康度检查。"""
        trackers = [t for t in store.rows("incr_tracker") if t.get("目标端点") == endpoint]
        call_logs = [c for c in store.rows("call_log") if c.get("接口端点") == endpoint]
        retries = [r for r in store.rows("retry_queue") if r.get("失败端点") == endpoint]
        return {
            "endpoint": endpoint,
            "trackers": len(trackers),
            "active_trackers": sum(1 for t in trackers if t.get("status") == "追踪中"),
            "call_records": len(call_logs),
            "pending_retries": sum(1 for r in retries if r.get("status") == "待重试"),
        }