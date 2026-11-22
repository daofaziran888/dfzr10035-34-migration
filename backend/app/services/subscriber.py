"""订阅管理领域服务：管理数据订阅，与 webhook_router、data_version、call_log、data_cache 联动。"""
from __future__ import annotations

from app.services import call_duration
from app.services.base import BaseModuleService
from app.store import store


class SubscriberService(BaseModuleService):
    MODULE = "subscriber"
    ENTITY = "订阅方"
    OPERATION_TERM = "校验"
    LIST_FIELDS = ["订阅编号", "订阅方", "数据主题", "推送地址", "推送频率", "消费状态", "通知方式", "最后推送"]
    REQUIRED_FIELDS = ["订阅编号", "订阅方", "数据主题"]
    STATUS_ORDER = ["待激活", "推送中", "消费确认", "已停订"]
    ACTION_RULES = {"激活订阅": "推送中", "确认消费": "消费确认", "停订服务": "已停订"}
    NEGATIVE_ACTIONS = []

    def run_action(self, entry_id: int, action: str):
        """激活订阅时创建webhook规则和数据版本；确认消费时更新调用日志；停订时清理缓存。"""
        entry, message = super().run_action(entry_id, action)
        if entry is None:
            return entry, message
        if action == "激活订阅":
            self._create_webhook_rule(entry)
            self._create_data_version(entry)
            message += "，已创建Webhook路由规则和数据版本"
        elif action == "确认消费":
            self._log_consume_event(entry)
            message += "，已记录消费日志"
        elif action == "停订服务":
            self._cleanup_cache_refs(entry)
            message += "，已清理关联缓存引用"
        return entry, message

    def _create_webhook_rule(self, sub: dict):
        """订阅激活时创建对应Webhook路由规则。"""
        wh_rows = store.rows("webhook_router")
        sid = str(sub["id"]).zfill(4)
        wh_rows.append({
            "id": max((int(r.get("id", 0)) for r in wh_rows), default=0) + 1,
            "规则编号": f"WEBH-SUB-{sid}",
            "触发事件": sub.get("数据主题", ""),
            "目标URL": sub.get("推送地址", ""),
            "签名方式": "HMAC-SHA256",
            "重试上限": 3,
            "超时秒数": 30,
            "投递状态": "就绪",
            "最后触发": "",
            "status": "草稿", "pending": True, "abnormal": False,
        })

    def _create_data_version(self, sub: dict):
        """订阅激活时创建数据版本追溯。"""
        ver_rows = store.rows("data_version")
        sid = str(sub["id"]).zfill(4)
        ver_rows.append({
            "id": max((int(r.get("id", 0)) for r in ver_rows), default=0) + 1,
            "版本编号": f"DATA-SUB-{sid}",
            "数据来源": sub.get("数据主题", ""),
            "行数统计": 0,
            "版本标签": "sub_init",
            "Schema摘要": sub.get("推送频率", ""),
            "创建原因": f"订阅{sid}激活",
            "依赖版本": "v0",
            "创建时间": "2026-11-17",
            "status": "创建中", "pending": True, "abnormal": False,
        })

    def _log_consume_event(self, sub: dict):
        """消费确认时写调用日志。"""
        log_rows = store.rows("call_log")
        sid = str(sub["id"]).zfill(4)
        log_rows.append({
            "id": max((int(r.get("id", 0)) for r in log_rows), default=0) + 1,
            "记录编号": f"CALL-SUB-{sid}",
            "接口端点": sub.get("数据主题", ""),
            "请求参数": sub.get("订阅方", ""),
            "响应状态": "200",
            "响应耗时(ms)": 50,
            "返回行数": 1,
            "错误信息": "",
            "调用时间": "2026-11-17",
            "status": "成功", "pending": False, "abnormal": False,
        })
        call_duration.standardize_record(log_rows[-1], batch_id="PIPELINE")

    def _cleanup_cache_refs(self, sub: dict):
        """停订时清理关联缓存策略引用标记。"""
        topic = sub.get("数据主题", "")
        for cache in store.rows("data_cache"):
            if cache.get("目标端点") == topic:
                cache["命中次数"] = str(max(0, int(cache.get("命中次数", 0)) - 1))

    def check_subscription_chain(self, topic: str) -> dict:
        """跨模块订阅链路检查：订阅方→Webhook路由→数据版本→缓存→投递管道。"""
        subs = [s for s in store.rows("subscriber") if s.get("数据主题") == topic]
        whs = [w for w in store.rows("webhook_router") if w.get("触发事件") == topic]
        vers = [v for v in store.rows("data_version") if v.get("数据来源") == topic]
        caches = [c for c in store.rows("data_cache") if c.get("目标端点") == topic]
        deliveries = [d for d in store.rows("data_delivery") if d.get("数据来源") == topic]
        return {
            "topic": topic,
            "subscribers": len(subs),
            "webhook_rules": len(whs),
            "data_versions": len(vers),
            "cache_policies": len(caches),
            "delivery_pipelines": len(deliveries),
            "chain_complete": len(whs) > 0 and len(vers) > 0,
        }