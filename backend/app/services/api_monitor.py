"""接口监控领域服务：管理API监控指标，与 call_log、retry_queue、rate_limit、govern_report 联动。"""
from __future__ import annotations

from app.services import call_duration
from app.services.base import BaseModuleService
from app.store import store


class ApiMonitorService(BaseModuleService):
    MODULE = "api_monitor"
    ENTITY = "监控指标"
    OPERATION_TERM = "采集"
    LIST_FIELDS = [
        "指标编号", "目标端点", "监控类型", "采集间隔", "告警阈值", "告警渠道",
        "静默时段", "最近采样", call_duration.CANONICAL_FIELD,
        "耗时单位", "耗时换算依据",
    ]
    REQUIRED_FIELDS = ["指标编号", "目标端点", "监控类型"]
    STATUS_ORDER = ["待启用", "监控中", "已告警", "已静默"]
    ACTION_RULES = {
        "启用监控": "监控中", "静默告警": "已告警", "废弃监控": "已静默",
        "重算延迟": None,
    }
    NEGATIVE_ACTIONS = []

    def run_action(self, entry_id: int, action: str):
        """启用监控时采集调用日志统计；已告警时联动重放队列和治理报告。"""
        if action == "重算延迟":
            entry = store.find(self.MODULE, entry_id)
            if entry is None:
                return None, f"{self.ENTITY} {entry_id} 不存在或已归档"
            latency = self._collect_call_log_stats(entry)
            return entry, (
                f"已按规范耗时重算延迟：均值{latency['avg_latency_ms']}ms"
                f"，样本{latency['sample_used']}/{latency['sample_total']}"
                f"，未转换{latency['unconverted']}条"
            )
        entry, message = super().run_action(entry_id, action)
        if entry is None:
            return entry, message
        if action == "启用监控":
            self._collect_call_log_stats(entry)
            message += "，已采集调用日志统计"
        elif action == "静默告警":
            self._on_alert_trigger(entry)
            message += "，已创建重放任务和治理报告"
        return entry, message

    def _collect_call_log_stats(self, monitor: dict):
        """启用时从 call_log 读取关联端点统计数据。

        延迟统一按规范耗时（``规范耗时(ms)``）计算，回填中断时对未迁移
        记录实时回退旧字段；缺单位记录只计入分母外的未转换项，不按零延迟
        入账。计算结果回写到监控统计，供报告与告警复用。
        """
        endpoint = monitor.get("目标端点", "")
        logs = [l for l in store.rows("call_log") if l.get("接口端点") == endpoint]
        total = len(logs)
        success = sum(1 for l in logs if l.get("status") == "成功")
        timeout = sum(1 for l in logs if l.get("status") == "超时")
        rate_limited = sum(1 for l in logs if l.get("status") == "限流拒绝")
        latency = call_duration.latency_summary(logs)
        monitor["最近采样"] = "2026-11-17"
        monitor["stats"] = {
            "total": total, "success": success,
            "timeout": timeout, "rate_limited": rate_limited,
            "latency_ms": latency["avg_latency_ms"],
            "latency_p50_ms": latency["p50_latency_ms"],
            "latency_p95_ms": latency["p95_latency_ms"],
            "latency_sample_used": latency["sample_used"],
            "latency_unconverted": latency["unconverted"],
            "latency_coverage": latency["coverage"],
            "latency_basis": "规范耗时(ms)，分母剔除单位不可确定记录",
        }
        # 回写规范耗时结果到监控指标行，便于列表/导出直接核对。
        monitor[call_duration.CANONICAL_FIELD] = latency["avg_latency_ms"]
        monitor["耗时单位"] = "ms"
        monitor["耗时换算依据"] = (
            f"规范耗时(ms)均值，样本{latency['sample_used']}/{latency['sample_total']}"
        )
        monitor["未转换耗时记录"] = latency["unconverted"]
        monitor["未转换耗时明细"] = latency["unconverted_records"]
        return latency

    def _on_alert_trigger(self, monitor: dict):
        """告警触发时联动创建重放任务和治理报告。"""
        endpoint = monitor.get("目标端点", "")
        mid = str(monitor["id"]).zfill(4)
        # 创建重放任务
        retry_rows = store.rows("retry_queue")
        retry_rows.append({
            "id": max((int(r.get("id", 0)) for r in retry_rows), default=0) + 1,
            "任务编号": f"RETR-MON-{mid}",
            "失败端点": endpoint,
            "失败原因": f"监控告警:{monitor.get('告警阈值', '')}",
            "重试次数": 0,
            "最大重试": 3,
            "退避策略": "fixed:10s",
            "最后重试": "",
            "队列状态": "告警触发",
            "status": "待重试", "pending": True, "abnormal": True,
        })
        # 创建治理报告
        gov_rows = store.rows("govern_report")
        gov_rows.append({
            "id": max((int(r.get("id", 0)) for r in gov_rows), default=0) + 1,
            "报告编号": f"GOVE-MON-{mid}",
            "评估范围": f"监控告警:{endpoint}",
            "合规指标": "API可用性",
            "当前得分": 40.0,
            "改进事项": f"端点{endpoint}触发告警，阈值{monitor.get('告警阈值', '')}",
            "评估周期": "2026-Q4",
            "负责人": "值班SRE",
            "生成时间": "2026-11-17",
            "status": "待生成", "pending": True, "abnormal": True,
        })

    def analyze_chain_health(self) -> dict:
        """跨模块链路健康分析：汇总监控+日志+重试+限流+治理全景。"""
        monitors = store.rows("api_monitor")
        call_logs = store.rows("call_log")
        retries = store.rows("retry_queue")
        rate_limits = store.rows("rate_limit")
        gov_reports = store.rows("govern_report")
        latency = call_duration.latency_summary(call_logs)
        return {
            "monitor_metrics": sum(1 for m in monitors if m.get("status") == "监控中"),
            "alert_count": sum(1 for m in monitors if m.get("status") == "已告警"),
            "total_calls": len(call_logs),
            "abnormal_calls": sum(1 for c in call_logs if c.get("abnormal")),
            "avg_latency_ms": latency["avg_latency_ms"],
            "latency_sample_used": latency["sample_used"],
            "latency_unconverted": latency["unconverted"],
            "pending_retries": sum(1 for r in retries if r.get("status") == "待重试"),
            "circuit_broken": sum(1 for rl in rate_limits if rl.get("status") == "已熔断"),
            "govern_issues": sum(1 for g in gov_reports if g.get("status") == "待生成"),
            "overall_health": "healthy" if sum(1 for m in monitors if m.get("status") == "已告警") < 3 else "degraded",
        }