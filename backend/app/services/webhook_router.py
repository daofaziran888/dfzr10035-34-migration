"""Webhook路由领域服务：管理回调路由规则，与 subscriber、call_log、data_delivery、pipeline_audit 联动。"""
from __future__ import annotations

from app.services import call_duration
from app.services.base import BaseModuleService
from app.store import store


class WebhookRouterService(BaseModuleService):
    MODULE = "webhook_router"
    ENTITY = "回调规则"
    OPERATION_TERM = "校验"
    LIST_FIELDS = ["规则编号", "触发事件", "目标URL", "签名方式", "重试上限", "超时秒数", "投递状态", "最后触发"]
    REQUIRED_FIELDS = ["规则编号", "触发事件", "目标URL"]
    STATUS_ORDER = ["草稿", "已启用", "投递失败", "已停用"]
    ACTION_RULES = {"启用规则": "已启用", "手动重发": "投递失败", "停用规则": "已停用"}
    NEGATIVE_ACTIONS = ["停用规则"]

    def run_action(self, entry_id: int, action: str):
        """启用时通知订阅方并写审计日志；投递失败时创建重放任务；停用时更新订阅状态。"""
        entry, message = super().run_action(entry_id, action)
        if entry is None:
            return entry, message
        if action == "启用规则":
            self._notify_subscribers(entry)
            self._audit_webhook(entry, "enabled")
            message += "，已通知订阅方并记录审计日志"
        elif action == "手动重发":
            self._enqueue_retry(entry)
            self._log_webhook(entry, "redeliver")
            message += "，失败事件已加入重放队列"
        elif action == "停用规则":
            self._update_subscriber_status(entry)
            self._audit_webhook(entry, "disabled")
            message += "，已更新关联订阅方状态"
        return entry, message

    def _notify_subscribers(self, wh: dict):
        event = wh.get("触发事件", "")
        for sub in store.rows("subscriber"):
            if sub.get("数据主题") == event:
                sub["最后推送"] = "2026-11-17"
                sub["消费状态"] = "待消费"

    def _audit_webhook(self, wh: dict, action: str):
        audit_rows = store.rows("pipeline_audit")
        wid = str(wh["id"]).zfill(4)
        audit_rows.append({
            "id": max((int(r.get("id", 0)) for r in audit_rows), default=0) + 1,
            "记录编号": f"PIPE-WH-{wid}",
            "管道名称": f"webhook_router:{wid}",
            "操作人": "system",
            "操作类型": action,
            "变更摘要": f"Webhook规则{wid}{action}",
            "操作时间": "2026-11-17",
            "来源节点": "webhook_router",
            "审计结论": "通过" if action == "enabled" else "已记录",
            "status": "已记录", "pending": True, "abnormal": False,
        })

    def _enqueue_retry(self, wh: dict):
        retry_rows = store.rows("retry_queue")
        wid = str(wh["id"]).zfill(4)
        retry_rows.append({
            "id": max((int(r.get("id", 0)) for r in retry_rows), default=0) + 1,
            "任务编号": f"RETR-WH-{wid}",
            "失败端点": wh.get("目标URL", ""),
            "失败原因": f"Webhook{wid}投递失败",
            "重试次数": 0,
            "最大重试": 3,
            "退避策略": "linear:2s",
            "最后重试": "",
            "队列状态": "webhook_retry",
            "status": "待重试", "pending": True, "abnormal": True,
        })

    def _log_webhook(self, wh: dict, event_type: str):
        log_rows = store.rows("call_log")
        wid = str(wh["id"]).zfill(4)
        log_rows.append({
            "id": max((int(r.get("id", 0)) for r in log_rows), default=0) + 1,
            "记录编号": f"CALL-WH-{wid}",
            "接口端点": wh.get("目标URL", ""),
            "请求参数": wh.get("触发事件", ""),
            "响应状态": "200" if event_type == "enabled" else "500",
            "响应耗时(ms)": 100,
            "返回行数": 0,
            "错误信息": "" if event_type == "enabled" else f"Webhook{wid}失败",
            "调用时间": "2026-11-17",
            "status": "成功" if event_type == "enabled" else "超时",
            "pending": event_type != "enabled",
            "abnormal": event_type != "enabled",
        })
        call_duration.standardize_record(log_rows[-1], batch_id="PIPELINE")

    def _update_subscriber_status(self, wh: dict):
        event = wh.get("触发事件", "")
        for sub in store.rows("subscriber"):
            if sub.get("数据主题") == event:
                sub["消费状态"] = "已停用"

    def check_webhook_chain(self, event_type: str) -> dict:
        """跨模块Webhook链路检查：Webhook→订阅→投递管道→审计。"""
        whs = [w for w in store.rows("webhook_router") if w.get("触发事件") == event_type]
        subs = [s for s in store.rows("subscriber") if s.get("数据主题") == event_type]
        audits = [a for a in store.rows("pipeline_audit") if a.get("来源节点") == "webhook_router"]
        logs = [l for l in store.rows("call_log") if "WH" in str(l.get("记录编号", ""))]
        return {
            "event_type": event_type,
            "webhook_rules": len(whs),
            "linked_subscribers": len(subs),
            "audit_records": len(audits),
            "call_logs": len(logs),
            "active_count": sum(1 for w in whs if w.get("status") == "已启用"),
        }