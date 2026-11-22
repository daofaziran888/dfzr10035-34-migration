"""任务调度领域服务：管理定时调度计划，与 request_batch、task_schedule、call_log、retry_queue、incr_tracker 联动。"""
from __future__ import annotations

from app.services import call_duration
from app.services.base import BaseModuleService
from app.store import store


class TaskScheduleService(BaseModuleService):
    MODULE = "task_schedule"
    ENTITY = "调度计划"
    OPERATION_TERM = "关联"
    LIST_FIELDS = ["计划编号", "关联端点", "Cron表达式", "时区设置", "失败通知", "依赖检查", "并发上限", "下次执行"]
    REQUIRED_FIELDS = ["计划编号", "关联端点", "Cron表达式"]
    STATUS_ORDER = ["待激活", "执行中", "已完成", "已停用"]
    ACTION_RULES = {"激活计划": "执行中", "立即执行": "已完成", "停用计划": "已停用"}
    NEGATIVE_ACTIONS = ["停用计划"]

    def run_action(self, entry_id: int, action: str):
        """激活计划时创建调用批次和增量追踪挂钩；立即执行时写日志和更新版本；停用时审计。"""
        entry, message = super().run_action(entry_id, action)
        if entry is None:
            return entry, message
        if action == "激活计划":
            self._create_request_batch(entry)
            self._link_incr_tracker(entry)
            message += "，已创建调用批次并链接增量追踪"
        elif action == "立即执行":
            self._log_schedule_run(entry)
            self._update_data_version(entry)
            message += "，已记录调度日志并更新数据版本"
        elif action == "停用计划":
            self._audit_disable(entry)
            self._cleanup_batch_refs(entry)
            message += "，已审计并清理关联批次引用"
        return entry, message

    def _create_request_batch(self, schedule: dict):
        """激活时自动创建关联的调用批次。"""
        batch_rows = store.rows("request_batch")
        sid = str(schedule["id"]).zfill(4)
        batch_rows.append({
            "id": max((int(r.get("id", 0)) for r in batch_rows), default=0) + 1,
            "批次编号": f"REQU-SCHED-{sid}",
            "关联端点": schedule.get("关联端点", ""),
            "参数模板": f"cron={schedule.get('Cron表达式', '')}",
            "请求数量": 50,
            "并发控制": schedule.get("并发上限", "10"),
            "超时上限": 30000,
            "完成数量": 0,
            "失败数量": 0,
            "status": "待执行", "pending": True, "abnormal": False,
        })

    def _link_incr_tracker(self, schedule: dict):
        """激活时确保增量追踪存在。"""
        endpoint = schedule.get("关联端点", "")
        exist = any(t.get("目标端点") == endpoint for t in store.rows("incr_tracker"))
        if not exist:
            tracker_rows = store.rows("incr_tracker")
            sid = str(schedule["id"]).zfill(4)
            tracker_rows.append({
                "id": max((int(r.get("id", 0)) for r in tracker_rows), default=0) + 1,
                "标记编号": f"INCR-SCHED-{sid}",
                "目标端点": endpoint,
                "增量字段": "updated_at",
                "起始游标": "0",
                "当前游标": "0",
                "追踪间隔": schedule.get("Cron表达式", ""),
                "数据来源": f"schedule:{sid}",
                "最后同步": "",
                "status": "待初始化", "pending": True, "abnormal": False,
            })

    def _log_schedule_run(self, schedule: dict):
        log_rows = store.rows("call_log")
        sid = str(schedule["id"]).zfill(4)
        log_rows.append({
            "id": max((int(r.get("id", 0)) for r in log_rows), default=0) + 1,
            "记录编号": f"CALL-SCHED-{sid}",
            "接口端点": schedule.get("关联端点", ""),
            "请求参数": schedule.get("Cron表达式", ""),
            "响应状态": "200",
            "响应耗时(ms)": 500,
            "返回行数": 100,
            "错误信息": "",
            "调用时间": "2026-11-17",
            "status": "成功", "pending": False, "abnormal": False,
        })
        call_duration.standardize_record(log_rows[-1], batch_id="PIPELINE")

    def _update_data_version(self, schedule: dict):
        """回调执行完成后创建数据版本。"""
        ver_rows = store.rows("data_version")
        sid = str(schedule["id"]).zfill(4)
        ver_rows.append({
            "id": max((int(r.get("id", 0)) for r in ver_rows), default=0) + 1,
            "版本编号": f"DATA-SCHED-{sid}",
            "数据来源": schedule.get("关联端点", ""),
            "行数统计": 100,
            "版本标签": "schedule_run",
            "Schema摘要": f"cron={schedule.get('Cron表达式', '')}",
            "创建原因": f"调度{sid}执行完成",
            "依赖版本": "previous",
            "创建时间": "2026-11-17",
            "status": "可用", "pending": False, "abnormal": False,
        })

    def _audit_disable(self, schedule: dict):
        audit_rows = store.rows("pipeline_audit")
        sid = str(schedule["id"]).zfill(4)
        audit_rows.append({
            "id": max((int(r.get("id", 0)) for r in audit_rows), default=0) + 1,
            "记录编号": f"PIPE-SCHED-{sid}",
            "管道名称": f"task_schedule:{sid}",
            "操作人": "system",
            "操作类型": "停用",
            "变更摘要": f"调度计划{sid}({schedule.get('Cron表达式', '')})已停用",
            "操作时间": "2026-11-17",
            "来源节点": "task_schedule",
            "审计结论": "已记录",
            "status": "待复核", "pending": True, "abnormal": True,
        })

    def _cleanup_batch_refs(self, schedule: dict):
        """停用时清理关联批次的引用。"""
        sid = str(schedule["id"]).zfill(4)
        for batch in store.rows("request_batch"):
            if batch.get("批次编号") == f"REQU-SCHED-{sid}":
                batch["status"] = "已完成"
                batch["pending"] = False

    def check_schedule_coverage(self) -> dict:
        """跨模块调度覆盖面统计。"""
        schedules = store.rows("task_schedule")
        batches = store.rows("request_batch")
        trackers = store.rows("incr_tracker")
        versions = [v for v in store.rows("data_version") if "SCHED" in str(v.get("版本编号", ""))]
        return {
            "total_schedules": len(schedules),
            "active_schedules": sum(1 for s in schedules if s.get("status") == "执行中"),
            "linked_batches": len(batches),
            "linked_trackers": sum(1 for t in trackers if "SCHED" in str(t.get("标记编号", ""))),
            "version_snapshots": len(versions),
        }