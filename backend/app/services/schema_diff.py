"""Schema差异检测领域服务：管理API Schema变更差异，与 endpoint_def、govern_report、data_version 联动。"""
from __future__ import annotations

from app.services import call_duration
from app.services.base import BaseModuleService
from app.store import store


class SchemaDiffService(BaseModuleService):
    MODULE = "schema_diff"
    ENTITY = "差异记录"
    OPERATION_TERM = "关联"
    LIST_FIELDS = ["记录编号", "关联端点", "变更字段", "旧定义", "新定义", "差异等级", "影响评估", "检测时间"]
    REQUIRED_FIELDS = ["记录编号", "关联端点", "变更字段"]
    STATUS_ORDER = ["已检测", "评估中", "已确认", "已忽略"]
    ACTION_RULES = {"发起评估": "评估中", "确认变更": "已确认", "忽略差异": "已忽略"}
    NEGATIVE_ACTIONS = ["忽略差异"]

    def run_action(self, entry_id: int, action: str):
        """评估中时更新端点状态和治理报告；确认变更时创建数据版本；忽略差异时记录治理。"""
        entry, message = super().run_action(entry_id, action)
        if entry is None:
            return entry, message
        if action == "发起评估":
            self._update_endpoint_status(entry, "Schema变更")
            self._create_govern_watch(entry)
            message += "，已更新端点状态并创建治理观察"
        elif action == "确认变更":
            self._create_data_version(entry)
            self._update_endpoint_status(entry, "已投产")
            message += "，已创建数据版本并恢复端点状态"
        elif action == "忽略差异":
            self._log_ignored_diff(entry)
        return entry, message

    def _update_endpoint_status(self, diff: dict, new_status: str):
        endpoint_id = diff.get("关联端点", "")
        for ep in store.rows("endpoint_def"):
            if ep.get("端点编号") == endpoint_id:
                ep["status"] = new_status
                ep["pending"] = new_status == "Schema变更"
                break

    def _create_govern_watch(self, diff: dict):
        gov_rows = store.rows("govern_report")
        did = str(diff["id"]).zfill(4)
        gov_rows.append({
            "id": max((int(r.get("id", 0)) for r in gov_rows), default=0) + 1,
            "报告编号": f"GOVE-SCHEMA-{did}",
            "评估范围": f"Schema差异评估:{diff.get('关联端点', '')}",
            "合规指标": "Schema变更合规性",
            "当前得分": 70.0,
            "改进事项": f"端点{diff.get('关联端点', '')}发生{diff.get('差异等级', '')}级变更",
            "评估周期": "2026-Q4",
            "负责人": "数据架构组",
            "生成时间": "2026-11-17",
            "status": "待生成", "pending": True, "abnormal": True,
        })

    def _create_data_version(self, diff: dict):
        ver_rows = store.rows("data_version")
        did = str(diff["id"]).zfill(4)
        ver_rows.append({
            "id": max((int(r.get("id", 0)) for r in ver_rows), default=0) + 1,
            "版本编号": f"DATA-SCHEMA-{did}",
            "数据来源": diff.get("关联端点", ""),
            "行数统计": 0,
            "版本标签": "schema_upgrade_v2",
            "Schema摘要": diff.get("新定义", ""),
            "创建原因": f"Schema差异{did}确认变更",
            "依赖版本": diff.get("旧定义", ""),
            "创建时间": "2026-11-17",
            "status": "可用", "pending": False, "abnormal": False,
        })

    def _log_ignored_diff(self, diff: dict):
        log_rows = store.rows("call_log")
        did = str(diff["id"]).zfill(4)
        log_rows.append({
            "id": max((int(r.get("id", 0)) for r in log_rows), default=0) + 1,
            "记录编号": f"CALL-SCHEMA-{did}",
            "接口端点": diff.get("关联端点", ""),
            "请求参数": f"ignore_diff:{did}",
            "响应状态": "ignored",
            "响应耗时(ms)": 0,
            "返回行数": 0,
            "错误信息": f"Schema差异{did}已忽略",
            "调用时间": "2026-11-17",
            "status": "成功", "pending": False, "abnormal": False,
        })
        call_duration.standardize_record(log_rows[-1], batch_id="PIPELINE")

    def calc_schema_health(self) -> dict:
        """跨模块Schema健康度。"""
        diffs = store.rows("schema_diff")
        endpoints = store.rows("endpoint_def")
        govern_reports = [g for g in store.rows("govern_report") if "SCHEMA" in str(g.get("报告编号", ""))]
        return {
            "total_diffs": len(diffs),
            "pending_assessment": sum(1 for d in diffs if d.get("status") == "已检测"),
            "confirmed_changes": sum(1 for d in diffs if d.get("status") == "已确认"),
            "endpoints_affected": sum(1 for ep in endpoints if ep.get("status") == "Schema变更"),
            "govern_items": len(govern_reports),
        }