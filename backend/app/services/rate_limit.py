"""限流熔断领域服务：管理请求速率限制，与调用日志、重放队列联动。"""
from __future__ import annotations

from app.services import call_duration
from app.services.base import BaseModuleService
from app.store import store


class RateLimitService(BaseModuleService):
    MODULE = "rate_limit"
    ENTITY = "限流策略"
    OPERATION_TERM = "校验"
    LIST_FIELDS = ["策略编号", "目标端点", "QPS上限", "滑动窗口", "熔断阈值", "恢复时长", "排队队列", "当前状态"]
    REQUIRED_FIELDS = ["策略编号", "目标端点", "QPS上限"]
    STATUS_ORDER = ["草稿", "生效中", "已熔断", "已恢复"]
    ACTION_RULES = {"启效策略": "生效中", "手动熔断": "已熔断", "确认恢复": "已恢复"}
    NEGATIVE_ACTIONS = []

    def run_action(self, entry_id: int, action: str):
        """启效策略时检查关联端点监控状态；已熔断时联动创建重放任务和写入调用日志。"""
        entry, message = super().run_action(entry_id, action)
        if entry is None:
            return entry, message
        if action == "启效策略":
            self._check_monitor_sync(entry)
            message += "，已同步关联监控配置"
        elif action == "手动熔断":
            self._on_circuit_break(entry)
            message += "，已创建重放任务并记录熔断日志"
        elif action == "确认恢复":
            self._on_circuit_restore(entry)
            message += "，已清理重放队列并更新缓存"
        return entry, message

    def _check_monitor_sync(self, limit: dict):
        """启效时检查关联监控是否存在，不存在则补建。"""
        endpoint = limit.get("目标端点", "")
        exist = any(
            m.get("目标端点") == endpoint
            for m in store.rows("api_monitor")
        )
        if not exist:
            mon_rows = store.rows("api_monitor")
            mid = str(limit["id"]).zfill(4)
            mon_rows.append({
                "id": max((int(r.get("id", 0)) for r in mon_rows), default=0) + 1,
                "指标编号": f"API_-RATE-{mid}",
                "目标端点": endpoint,
                "监控类型": "限流熔断监测",
                "采集间隔": "10s",
                "告警阈值": f"熔断阈值{limit.get('熔断阈值(fail%)', 30)}%",
                "告警渠道": "webhook+mail",
                "静默时段": "2026-11-17",
                "最近采样": "2026-11-17",
                "status": "监控中", "pending": True, "abnormal": False,
            })

    def _on_circuit_break(self, limit: dict):
        """熔断时创建重放任务并写调用日志。"""
        endpoint = limit.get("目标端点", "")
        lid = str(limit["id"]).zfill(4)
        # 创建重放任务
        retry_rows = store.rows("retry_queue")
        retry_rows.append({
            "id": max((int(r.get("id", 0)) for r in retry_rows), default=0) + 1,
            "任务编号": f"RETR-CB-{lid}",
            "失败端点": endpoint,
            "失败原因": f"熔断触发:限流策略{lid}",
            "重试次数": 0,
            "最大重试": 5,
            "退避策略": "exponential:5s/baseline",
            "最后重试": "",
            "队列状态": "熔断等待",
            "status": "待重试", "pending": True, "abnormal": True,
        })
        # 写调用日志
        log_rows = store.rows("call_log")
        log_rows.append({
            "id": max((int(r.get("id", 0)) for r in log_rows), default=0) + 1,
            "记录编号": f"CALL-CB-{lid}",
            "接口端点": endpoint,
            "请求参数": f"circuit_break:{lid}",
            "响应状态": "503",
            "响应耗时(ms)": 0,
            "返回行数": 0,
            "错误信息": f"限流熔断触发，策略{lid}",
            "调用时间": "2026-11-17",
            "status": "限流拒绝", "pending": False, "abnormal": True,
        })
        call_duration.standardize_record(log_rows[-1], batch_id="PIPELINE")

    def _on_circuit_restore(self, limit: dict):
        """恢复后清理关联重放任务并刷新缓存状态。"""
        endpoint = limit.get("目标端点", "")
        for retry in store.rows("retry_queue"):
            if retry.get("失败端点") == endpoint and retry.get("status") == "待重试":
                retry["status"] = "已成功"
                retry["pending"] = False
                retry["队列状态"] = "已恢复"
        for cache in store.rows("data_cache"):
            if cache.get("目标端点") == endpoint:
                cache["最后刷新"] = "2026-11-17"

    def check_rate_health(self, endpoint: str) -> dict:
        """跨模块限流健康检查：综合重放队列+调用日志+缓存。"""
        retries = [r for r in store.rows("retry_queue") if r.get("失败端点") == endpoint]
        logs = [l for l in store.rows("call_log") if l.get("接口端点") == endpoint]
        caches = [c for c in store.rows("data_cache") if c.get("目标端点") == endpoint]
        rate_limited = sum(1 for l in logs if l.get("status") == "限流拒绝")
        return {
            "endpoint": endpoint,
            "rate_limit_hits": rate_limited,
            "pending_retries": sum(1 for r in retries if r.get("status") == "待重试"),
            "cache_active": any(c.get("status") == "生效中" for c in caches),
            "need_v2_upgrade": rate_limited > 3,
        }