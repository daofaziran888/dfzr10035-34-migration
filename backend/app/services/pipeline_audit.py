"""管道审计领域服务：管理操作审计记录，与 govern_report、call_log、data_version 联动。"""
from __future__ import annotations

from app.services import call_duration
from app.services.base import BaseModuleService
from app.store import store


class PipelineAuditService(BaseModuleService):
    MODULE = "pipeline_audit"
    ENTITY = "审计记录"
    OPERATION_TERM = "校验"
    LIST_FIELDS = ["记录编号", "管道名称", "操作人", "操作类型", "变更摘要", "操作时间", "来源节点", "审计结论"]
    REQUIRED_FIELDS = ["记录编号", "管道名称", "操作人"]
    STATUS_ORDER = ["已记录", "待复核", "已通过", "已驳回"]
    ACTION_RULES = {"提交复核": "待复核", "确认通过": "已通过", "驳回记录": "已驳回"}
    NEGATIVE_ACTIONS = ["驳回记录"]

    def run_action(self, entry_id: int, action: str):
        """待复核时联动治理报告和调用日志；确认通过时创建数据版本；驳回时记录治理项。"""
        entry, message = super().run_action(entry_id, action)
        if entry is None:
            return entry, message
        if action == "提交复核":
            self._create_govern_item(entry)
            self._log_audit_action(entry, "review")
            message += "，已创建治理项并记录调用日志"
        elif action == "确认通过":
            self._create_data_version(entry)
            self._log_audit_action(entry, "approved")
            message += "，已创建数据版本"
        elif action == "驳回记录":
            self._create_retry_and_govern(entry)
            message += "，已创建重放任务和治理项"
        return entry, message

    def _create_govern_item(self, audit: dict):
        gov_rows = store.rows("govern_report")
        aid = str(audit["id"]).zfill(4)
        gov_rows.append({
            "id": max((int(r.get("id", 0)) for r in gov_rows), default=0) + 1,
            "报告编号": f"GOVE-AUDIT-{aid}",
            "评估范围": f"审计记录复核:{audit.get('管道名称', '')}",
            "合规指标": "操作合规性",
            "当前得分": 80.0,
            "改进事项": f"审计记录{aid}待复核: {audit.get('变更摘要', '')}",
            "评估周期": "2026-Q4",
            "负责人": audit.get("操作人", "审计组"),
            "生成时间": "2026-11-17",
            "status": "待生成", "pending": True, "abnormal": False,
        })

    def _log_audit_action(self, audit: dict, action: str):
        log_rows = store.rows("call_log")
        aid = str(audit["id"]).zfill(4)
        log_rows.append({
            "id": max((int(r.get("id", 0)) for r in log_rows), default=0) + 1,
            "记录编号": f"CALL-AUDIT-{aid}",
            "接口端点": audit.get("管道名称", ""),
            "请求参数": audit.get("来源节点", ""),
            "响应状态": "200" if action == "approved" else "pending",
            "响应耗时(ms)": 50,
            "返回行数": 1,
            "错误信息": "",
            "调用时间": "2026-11-17",
            "status": "成功" if action == "approved" else "超时",
            "pending": action != "approved",
            "abnormal": action != "approved",
        })
        call_duration.standardize_record(log_rows[-1], batch_id="PIPELINE")

    def _create_data_version(self, audit: dict):
        ver_rows = store.rows("data_version")
        aid = str(audit["id"]).zfill(4)
        ver_rows.append({
            "id": max((int(r.get("id", 0)) for r in ver_rows), default=0) + 1,
            "版本编号": f"DATA-AUDIT-{aid}",
            "数据来源": audit.get("管道名称", ""),
            "行数统计": 1,
            "版本标签": "audit_approved",
            "Schema摘要": audit.get("变更摘要", ""),
            "创建原因": f"审计记录{aid}通过",
            "依赖版本": "current",
            "创建时间": "2026-11-17",
            "status": "可用", "pending": False, "abnormal": False,
        })

    def _create_retry_and_govern(self, audit: dict):
        """驳回时创建重放任务触发重采。"""
        rid = str(audit["id"]).zfill(4)
        retry_rows = store.rows("retry_queue")
        retry_rows.append({
            "id": max((int(r.get("id", 0)) for r in retry_rows), default=0) + 1,
            "任务编号": f"RETR-AUDIT-{rid}",
            "失败端点": audit.get("来源节点", ""),
            "失败原因": f"审计记录{rid}被驳回",
            "重试次数": 0,
            "最大重试": 1,
            "退避策略": "fixed:5s",
            "最后重试": "",
            "队列状态": "audit_reject",
            "status": "待重试", "pending": True, "abnormal": True,
        })

    def calc_audit_trail(self) -> dict:
        """跨模块审计追踪统计。"""
        audits = store.rows("pipeline_audit")
        gov_items = [g for g in store.rows("govern_report") if "AUDIT" in str(g.get("报告编号", ""))]
        logs = [l for l in store.rows("call_log") if "AUDIT" in str(l.get("记录编号", ""))]
        return {
            "total_audits": len(audits),
            "pending_review": sum(1 for a in audits if a.get("status") == "待复核"),
            "rejected": sum(1 for a in audits if a.get("status") == "已驳回"),
            "govern_items": len(gov_items),
            "logged_events": len(logs),
        }