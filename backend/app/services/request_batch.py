"""请求批次领域服务：管理批量API请求，与端点定义、调用日志、重放队列、结果缓存联动。"""
from __future__ import annotations

from app.services import call_duration
from app.services.base import BaseModuleService
from app.store import store


class RequestBatchService(BaseModuleService):
    MODULE = "request_batch"
    ENTITY = "调用批次"
    OPERATION_TERM = "关联"
    LIST_FIELDS = ["批次编号", "关联端点", "参数模板", "请求数量", "并发控制", "超时上限", "完成数量", "失败数量"]
    REQUIRED_FIELDS = ["批次编号", "关联端点", "参数模板"]
    STATUS_ORDER = ["待执行", "执行中", "已完成", "部分失败"]
    ACTION_RULES = {"启动执行": "执行中", "取消批次": "已完成", "标记完成": "部分失败"}
    NEGATIVE_ACTIONS = []

    def run_action(self, entry_id: int, action: str):
        """启动执行时联动创建调用日志；标记完成/部分失败时联动创建重放任务和缓存刷新。"""
        entry, message = super().run_action(entry_id, action)
        if entry is None:
            return entry, message
        if action == "启动执行":
            self._log_batch_start(entry)
            message += "，已记录调用日志"
        elif action in ("取消批次", "标记完成"):
            self._on_batch_complete(entry, action)
            if int(entry.get("失败数量", 0)) > 0:
                self._enqueue_retry(entry)
                message += "，失败请求已加入重放队列"
        return entry, message

    def _log_batch_start(self, batch: dict):
        """启动时写调用日志，供 api_monitor 和 call_log 模块消费。"""
        log_rows = store.rows("call_log")
        bid_num = str(batch["id"]).zfill(4)
        log_rows.append({
            "id": max((int(r.get("id", 0)) for r in log_rows), default=0) + 1,
            "记录编号": f"CALL-BATCH-{bid_num}",
            "接口端点": batch.get("关联端点", ""),
            "请求参数": batch.get("参数模板", ""),
            "响应状态": "executing",
            "响应耗时(ms)": 0,
            "返回行数": batch.get("请求数量", 0),
            "错误信息": "",
            "调用时间": "2026-11-17",
            "status": "超时", "pending": True, "abnormal": False,
        })
        call_duration.standardize_record(log_rows[-1], batch_id="PIPELINE")

    def _on_batch_complete(self, batch: dict, action: str):
        """批次完成时联动：更新调用日志最终状态，同步数据版本。"""
        batch_id = f"CALL-BATCH-{str(batch['id']).zfill(4)}"
        for log in store.rows("call_log"):
            if log.get("记录编号") == batch_id:
                log["status"] = "成功" if action == "取消批次" else "限流拒绝"
                log["响应耗时(ms)"] = batch.get("超时上限", 5000)
                log["返回行数"] = batch.get("完成数量", 0)
                log["错误信息"] = "" if action == "取消批次" else f"失败{batch.get('失败数量', 0)}项"
                call_duration.standardize_record(log, batch_id="PIPELINE")
                break

    def _enqueue_retry(self, batch: dict):
        """失败请求自动创建重放任务。"""
        retry_rows = store.rows("retry_queue")
        bid = str(batch["id"]).zfill(4)
        retry_rows.append({
            "id": max((int(r.get("id", 0)) for r in retry_rows), default=0) + 1,
            "任务编号": f"RETR-BATCH-{bid}",
            "失败端点": batch.get("关联端点", ""),
            "失败原因": f"批次{bid}含{batch.get('失败数量', 0)}条失败",
            "重试次数": 0,
            "最大重试": 3,
            "退避策略": "exponential:2s/baseline",
            "最后重试": "",
            "队列状态": "等待",
            "status": "待重试", "pending": True, "abnormal": False,
        })

    def check_batch_throughput(self, endpoint_id: str) -> dict:
        """跨模块吞吐量分析：汇总调用日志+重放队列+缓存的健康度。"""
        logs = [l for l in store.rows("call_log") if l.get("接口端点") == endpoint_id]
        retries = [r for r in store.rows("retry_queue") if r.get("失败端点") == endpoint_id]
        caches = [c for c in store.rows("data_cache") if c.get("目标端点") == endpoint_id]
        return {
            "endpoint": endpoint_id,
            "total_calls": len(logs),
            "failed_calls": sum(1 for l in logs if l.get("status") != "成功"),
            "pending_retries": len(retries),
            "cache_hit": any(c.get("status") == "生效中" for c in caches),
        }