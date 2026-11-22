"""治理报告领域服务：管理数据质量与合规报告，与 pipeline_audit、data_version、call_log、api_monitor 联动。"""
from __future__ import annotations

from app.services import call_duration
from app.services.base import BaseModuleService
from app.store import store


class GovernReportService(BaseModuleService):
    MODULE = "govern_report"
    ENTITY = "治理报告"
    OPERATION_TERM = "采集"
    LIST_FIELDS = ["报告编号", "评估范围", "合规指标", "当前得分", "改进事项", "评估周期", "负责人", "生成时间"]
    REQUIRED_FIELDS = ["报告编号", "评估范围", "合规指标"]
    STATUS_ORDER = ["待生成", "已生成", "已审核", "已归档"]
    ACTION_RULES = {"生成报告": "已生成", "提交审核": "已审核", "归档报告": "已归档"}
    NEGATIVE_ACTIONS = []

    def run_action(self, entry_id: int, action: str):
        """生成报告时收集跨模块数据；审核时联动管道审计和调用日志。"""
        entry, message = super().run_action(entry_id, action)
        if entry is None:
            return entry, message
        if action == "生成报告":
            self._collect_cross_module_data(entry)
            message += "，已采集跨模块全景数据"
        elif action == "提交审核":
            self._audit_report(entry)
            message += "，已记录审计并写入调用日志"
        elif action == "归档报告":
            self._snapshot_data_version(entry)
            message += "，已创建数据版本快照"
        return entry, message

    def _collect_cross_module_data(self, report: dict):
        """生成报告时采集跨模块统计。"""
        total_calls = len(store.rows("call_log"))
        pending_retries = sum(1 for r in store.rows("retry_queue") if r.get("status") == "待重试")
        alerts = sum(1 for m in store.rows("api_monitor") if m.get("status") == "已告警")
        broken = sum(1 for rl in store.rows("rate_limit") if rl.get("status") == "已熔断")
        pipeline_errors = sum(1 for d in store.rows("data_delivery") if d.get("status") == "投递异常")
        # 基于统计计算综合得分
        health = 100.0
        health -= 5 * pending_retries
        health -= 10 * alerts
        health -= 15 * broken
        health -= 10 * pipeline_errors
        report["当前得分"] = max(0.0, health)
        report["改进事项"] = (
            f"待重试{pending_retries}项/告警{alerts}条/"
            f"熔断{broken}路/投递异常{pipeline_errors}路"
        )
        report["stats"] = {
            "total_calls": total_calls,
            "pending_retries": pending_retries,
            "active_alerts": alerts,
            "circuit_breaks": broken,
            "pipeline_errors": pipeline_errors,
        }

    def _audit_report(self, report: dict):
        """审核报告时写审计日志和调用日志。"""
        rid = str(report["id"]).zfill(4)
        # 管道审计
        audit_rows = store.rows("pipeline_audit")
        audit_rows.append({
            "id": max((int(r.get("id", 0)) for r in audit_rows), default=0) + 1,
            "记录编号": f"PIPE-GOV-{rid}",
            "管道名称": f"govern_report:{rid}",
            "操作人": report.get("负责人", "system"),
            "操作类型": "审核",
            "变更摘要": f"治理报告{rid}提交审核",
            "操作时间": "2026-11-17",
            "来源节点": "govern_report",
            "审计结论": "待复核",
            "status": "待复核", "pending": True, "abnormal": False,
        })
        # 调用日志
        log_rows = store.rows("call_log")
        log_rows.append({
            "id": max((int(r.get("id", 0)) for r in log_rows), default=0) + 1,
            "记录编号": f"CALL-GOV-{rid}",
            "接口端点": "govern_report",
            "请求参数": report.get("评估范围", ""),
            "响应状态": "200",
            "响应耗时(ms)": 200,
            "返回行数": 1,
            "错误信息": "",
            "调用时间": "2026-11-17",
            "status": "成功", "pending": False, "abnormal": False,
        })
        call_duration.standardize_record(log_rows[-1], batch_id="PIPELINE")

    def _snapshot_data_version(self, report: dict):
        """归档时创建数据版本快照。"""
        ver_rows = store.rows("data_version")
        rid = str(report["id"]).zfill(4)
        ver_rows.append({
            "id": max((int(r.get("id", 0)) for r in ver_rows), default=0) + 1,
            "版本编号": f"DATA-GOV-{rid}",
            "数据来源": "govern_report",
            "行数统计": 1,
            "版本标签": "report_archived",
            "Schema摘要": report.get("合规指标", ""),
            "创建原因": f"治理报告{rid}已归档",
            "依赖版本": report.get("评估周期", ""),
            "创建时间": "2026-11-17",
            "status": "可用", "pending": False, "abnormal": False,
        })

    def calc_govern_score(self) -> dict:
        """跨模块治理全景评分。"""
        return {
            "reports_total": len(store.rows("govern_report")),
            "reports_pending": sum(1 for r in store.rows("govern_report") if r.get("status") == "待生成"),
            "audits_open": sum(1 for a in store.rows("pipeline_audit") if a.get("status") == "待复核"),
            "data_versions": len(store.rows("data_version")),
            "call_anomalies": sum(1 for c in store.rows("call_log") if c.get("abnormal")),
        }