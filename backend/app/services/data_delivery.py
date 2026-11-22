"""数据投递领域服务：管理数据管道投递，与 subscriber、call_log、govern_report、data_version 联动。"""
from __future__ import annotations

from app.services import call_duration
from app.services.base import BaseModuleService
from app.store import store


class DataDeliveryService(BaseModuleService):
    MODULE = "data_delivery"
    ENTITY = "投递管道"
    OPERATION_TERM = "采集"
    LIST_FIELDS = ["管道编号", "数据来源", "目标地址", "投递格式", "分区策略", "投递频率", "压缩方式", "最后投递"]
    REQUIRED_FIELDS = ["管道编号", "数据来源", "目标地址"]
    STATUS_ORDER = ["待配置", "投递中", "已完成", "投递异常"]
    ACTION_RULES = {"启动投递": "投递中", "暂停投递": "已完成", "标记异常": "投递异常"}
    NEGATIVE_ACTIONS = []

    def run_action(self, entry_id: int, action: str):
        """投递中时通知订阅方并记日志；完成时创建数据版本；异常时触发治理报告。"""
        entry, message = super().run_action(entry_id, action)
        if entry is None:
            return entry, message
        if action == "启动投递":
            self._notify_subscribers(entry)
            self._log_delivery(entry, "started")
            message += "，已通知订阅方并记录调用日志"
        elif action == "暂停投递":
            self._create_data_version(entry)
            self._log_delivery(entry, "completed")
            message += "，已创建数据版本"
        elif action == "标记异常":
            self._trigger_govern_report(entry)
            self._log_delivery(entry, "error")
            message += "，已触发治理报告"
        return entry, message

    def _notify_subscribers(self, delivery: dict):
        """启动投递通知所有关联订阅方。"""
        source = delivery.get("数据来源", "")
        did = str(delivery["id"]).zfill(4)
        for sub in store.rows("subscriber"):
            if sub.get("数据主题") == source or sub.get("订阅方") and source in str(sub.get("订阅方", "")):
                sub["最后推送"] = "2026-11-17"
                sub["消费状态"] = "待消费"
        # 触发webhook路由
        for wh in store.rows("webhook_router"):
            if wh.get("触发事件") == "data_delivery":
                wh["最后触发"] = "2026-11-17"
                wh["投递状态"] = f"delivery_{did}"

    def _log_delivery(self, delivery: dict, event: str):
        log_rows = store.rows("call_log")
        did = str(delivery["id"]).zfill(4)
        log_rows.append({
            "id": max((int(r.get("id", 0)) for r in log_rows), default=0) + 1,
            "记录编号": f"CALL-DELIV-{did}",
            "接口端点": delivery.get("数据来源", ""),
            "请求参数": delivery.get("目标地址", ""),
            "响应状态": "200" if event != "error" else "500",
            "响应耗时(ms)": 300 if event == "started" else 500,
            "返回行数": 0,
            "错误信息": "" if event != "error" else f"管道{did}投递异常",
            "调用时间": "2026-11-17",
            "status": "成功" if event != "error" else "超时",
            "pending": event != "error",
            "abnormal": event == "error",
        })
        call_duration.standardize_record(log_rows[-1], batch_id="PIPELINE")

    def _create_data_version(self, delivery: dict):
        """投递完成创建数据版本快照。"""
        ver_rows = store.rows("data_version")
        did = str(delivery["id"]).zfill(4)
        ver_rows.append({
            "id": max((int(r.get("id", 0)) for r in ver_rows), default=0) + 1,
            "版本编号": f"DATA-DELIV-{did}",
            "数据来源": delivery.get("数据来源", ""),
            "行数统计": 1000,
            "版本标签": "delivery_complete",
            "Schema摘要": delivery.get("投递格式", "JSON"),
            "创建原因": f"投递管道{did}完成",
            "依赖版本": delivery.get("分区策略", ""),
            "创建时间": "2026-11-17",
            "status": "可用", "pending": True, "abnormal": False,
        })

    def _trigger_govern_report(self, delivery: dict):
        """投递异常触发治理报告。"""
        gov_rows = store.rows("govern_report")
        did = str(delivery["id"]).zfill(4)
        gov_rows.append({
            "id": max((int(r.get("id", 0)) for r in gov_rows), default=0) + 1,
            "报告编号": f"GOVE-DELIV-{did}",
            "评估范围": f"投递管道异常:{delivery.get('数据来源', '')}",
            "合规指标": "数据投递成功率",
            "当前得分": 50.0,
            "改进事项": f"管道{did}投递异常，需重启或排查",
            "评估周期": "2026-Q4",
            "负责人": "数据运维",
            "生成时间": "2026-11-17",
            "status": "待生成", "pending": True, "abnormal": True,
        })

    def calc_pipeline_health(self) -> dict:
        """跨模块管道健康度：汇总投递管道+订阅方+数据版本+调用日志。"""
        deliveries = store.rows("data_delivery")
        subscribers = store.rows("subscriber")
        versions = store.rows("data_version")
        logs = [l for l in store.rows("call_log") if str(l.get("记录编号", "")).startswith("CALL-DELIV-")]
        active = sum(1 for d in deliveries if d.get("status") == "投递中")
        error = sum(1 for d in deliveries if d.get("status") == "投递异常")
        return {
            "total_pipelines": len(deliveries),
            "active": active,
            "error": error,
            "subscribers_waiting": sum(1 for s in subscribers if s.get("消费状态") == "待消费"),
            "version_snapshots": len(versions),
            "logged_events": len(logs),
        }