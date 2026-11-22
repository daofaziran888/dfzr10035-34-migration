"""API提供方领域服务：管理API源注册，编排下游鉴权、限流、端点、监控的联动创建。"""
from __future__ import annotations

from app.services import call_duration
from app.services.base import BaseModuleService
from app.store import store


class ApiProviderService(BaseModuleService):
    MODULE = "api_provider"
    ENTITY = "API提供方"
    OPERATION_TERM = "接入"
    LIST_FIELDS = ["提供方编号", "提供方名称", "认证方式", "BaseURL", "SLA承诺", "QPS上限", "接入状态", "注册时间"]
    REQUIRED_FIELDS = ["提供方编号", "提供方名称", "认证方式"]
    STATUS_ORDER = ["待审核", "已注册", "SLA降级", "已停用"]
    ACTION_RULES = {"审核注册": "已注册", "标记降级": "SLA降级", "停用来源": "已停用"}
    NEGATIVE_ACTIONS = ["停用来源"]

    def run_action(self, entry_id: int, action: str):
        """审核注册时联动创建鉴权方案、端点定义和限流策略。"""
        entry, message = super().run_action(entry_id, action)
        if entry is None:
            return entry, message
        if action == "审核注册":
            self._orchestrate_downstream(entry)
            message += "，已联动创建鉴权/端点/限流/监控/用量管线"
        return entry, message

    def _orchestrate_downstream(self, provider: dict):
        """上游注册通过后，一次性联动6个下游模块的记录创建。"""
        pid = str(provider["id"])
        pname = str(provider["提供方名称"])
        pbase = str(provider["BaseURL"])
        # 1) 鉴权方案
        auth_rows = store.rows("auth_profile")
        auth_rows.append({
            "id": max((int(r.get("id", 0)) for r in auth_rows), default=0) + 1,
            "方案编号": f"AUTH-{pid.zfill(4)}",
            "提供方": pname,
            "鉴权类型": "BearerToken",
            "密钥版本": "v1",
            "Token过期(秒)": 7200,
            "刷新策略": "自动刷新",
            "轮换周期": "30天",
            "最近轮换": provider.get("注册时间", ""),
            "status": "草稿", "pending": True, "abnormal": False,
        })
        # 2) 端点定义
        ep_rows = store.rows("endpoint_def")
        ep_rows.append({
            "id": max((int(r.get("id", 0)) for r in ep_rows), default=0) + 1,
            "端点编号": f"ENDP-{pid.zfill(4)}",
            "提供方": pname,
            "请求路径": f"{pbase}/v1/data",
            "请求方法": "GET",
            "参数Schema": '{"page": "int", "size": "int"}',
            "响应Schema": '{"code": "int", "data": "array"}',
            "分页策略": "page_size",
            "描述文档": f"{pname}数据查询接口",
            "status": "草稿", "pending": True, "abnormal": False,
        })
        # 3) 限流策略
        rl_rows = store.rows("rate_limit")
        rl_rows.append({
            "id": max((int(r.get("id", 0)) for r in rl_rows), default=0) + 1,
            "策略编号": f"RATE-{pid.zfill(4)}",
            "目标端点": f"ENDP-{pid.zfill(4)}",
            "QPS上限": str(provider.get("QPS上限", "100")),
            "滑动窗口": "60s",
            "熔断阈值(fail%)": 30.0,
            "恢复时长(s)": 60,
            "排队队列": "FIFO-1000",
            "当前状态": "待生效",
            "status": "草稿", "pending": True, "abnormal": False,
        })
        # 4) 监控指标
        mon_rows = store.rows("api_monitor")
        mon_rows.append({
            "id": max((int(r.get("id", 0)) for r in mon_rows), default=0) + 1,
            "指标编号": f"API_-{pid.zfill(4)}",
            "目标端点": f"ENDP-{pid.zfill(4)}",
            "监控类型": "可用性+延迟",
            "采集间隔": "30s",
            "告警阈值": "延迟>2000ms",
            "告警渠道": "webhook",
            "静默时段": provider.get("注册时间", ""),
            "最近采样": provider.get("注册时间", ""),
            "status": "待启用", "pending": True, "abnormal": False,
        })
        # 5) 用量账单
        cost_rows = store.rows("cost_account")
        cost_rows.append({
            "id": max((int(r.get("id", 0)) for r in cost_rows), default=0) + 1,
            "账单编号": f"COST-{pid.zfill(4)}",
            "提供方": pname,
            "计费周期": "monthly",
            "调用次数": 0,
            "数据流量(MB)": 0,
            "超出配额": "否",
            "费用预估": 0.0,
            "账单状态": "初始",
            "status": "统计中", "pending": True, "abnormal": False,
        })
        # 6) 调用日志（注册事件）
        log_rows = store.rows("call_log")
        log_rows.append({
            "id": max((int(r.get("id", 0)) for r in log_rows), default=0) + 1,
            "记录编号": f"CALL-{pid.zfill(4)}",
            "接口端点": f"ENDP-{pid.zfill(4)}",
            "请求参数": provider.get("注册时间", ""),
            "响应状态": "200",
            "响应耗时(ms)": 0,
            "返回行数": 0,
            "错误信息": "",
            "调用时间": provider.get("注册时间", ""),
            "status": "成功", "pending": True, "abnormal": False,
        })
        call_duration.standardize_record(log_rows[-1], batch_id="PIPELINE")

    def check_sla_health(self, provider_id: int) -> dict:
        """跨模块SLA巡检：读取关联的限流/监控/用量数据评估健康度。"""
        provider = store.find("api_provider", provider_id)
        if not provider:
            return {"ok": False, "message": "提供方不存在"}
        pid = str(provider_id).zfill(4)
        # 从限流模块读取策略
        rate_rows = [
            r for r in store.rows("rate_limit")
            if r.get("策略编号") == f"RATE-{pid}"
        ]
        rate_ok = any(r.get("status") == "生效中" for r in rate_rows)
        # 从监控模块读取告警
        mon_rows = [
            m for m in store.rows("api_monitor")
            if m.get("指标编号") == f"API_-{pid}"
        ]
        mon_alert = any(m.get("status") == "已告警" for m in mon_rows)
        # 从用量模块读取超额
        cost_rows = [
            c for c in store.rows("cost_account")
            if c.get("账单编号") == f"COST-{pid}"
        ]
        cost_over = any(c.get("status") == "超额告警" for c in cost_rows)
        return {
            "provider_id": provider_id,
            "provider_name": provider["提供方名称"],
            "sla_ok": rate_ok and not mon_alert and not cost_over,
            "rate_limit_active": rate_ok,
            "monitor_alert": mon_alert,
            "cost_overrun": cost_over,
        }