"""传输加密领域服务：管理数据传输加密配置，与 auth_profile、call_log、subscriber、govern_report 联动。"""
from __future__ import annotations

from app.services import call_duration
from app.services.base import BaseModuleService
from app.store import store


class DataEncryptService(BaseModuleService):
    MODULE = "data_encrypt"
    ENTITY = "加密配置"
    OPERATION_TERM = "校验"
    LIST_FIELDS = ["配置编号", "数据范围", "加密算法", "密钥标识", "签名模式", "证书指纹", "生效时段", "最后更新"]
    REQUIRED_FIELDS = ["配置编号", "数据范围", "加密算法"]
    STATUS_ORDER = ["草稿", "已启用", "证书过期", "已更新"]
    ACTION_RULES = {"启用加密": "已启用", "更新证书": "已更新", "标记过期": "证书过期"}
    NEGATIVE_ACTIONS = []

    def run_action(self, entry_id: int, action: str):
        """启用加密时检查鉴权方案链和写调用日志；证书过期时发治理报告和告警订阅方。"""
        entry, message = super().run_action(entry_id, action)
        if entry is None:
            return entry, message
        if action == "启用加密":
            self._check_auth_chain(entry)
            self._log_encrypt_event(entry, "enabled")
            message += "，已核对鉴权链路并记录调用日志"
        elif action == "更新证书":
            self._notify_subscribers_cert_update(entry)
            message += "，已通知订阅方证书更新"
        elif action == "标记过期":
            self._trigger_cert_expiry_govern(entry)
            self._alert_monitor(entry)
            message += "，已触发治理警告和监控告警"
        return entry, message

    def _check_auth_chain(self, encrypt: dict):
        """检查加密范围内所有提供方的鉴权方案是否完备。"""
        data_range = encrypt.get("数据范围", "")
        for provider in store.rows("api_provider"):
            if provider.get("提供方名称") and data_range in str(provider.get("提供方名称", "")):
                auths = [a for a in store.rows("auth_profile") if a.get("提供方") == provider["提供方名称"]]
                if not auths:
                    # 补建鉴权方案
                    auth_rows = store.rows("auth_profile")
                    pid = str(provider["id"]).zfill(4)
                    auth_rows.append({
                        "id": max((int(r.get("id", 0)) for r in auth_rows), default=0) + 1,
                        "方案编号": f"AUTH-ENC-{pid}",
                        "提供方": provider["提供方名称"],
                        "鉴权类型": "mTLS",
                        "密钥版本": "v2",
                        "Token过期(秒)": 3600,
                        "刷新策略": "自动轮换",
                        "轮换周期": "7天",
                        "最近轮换": "2026-11-17",
                        "status": "草稿", "pending": True, "abnormal": False,
                    })

    def _log_encrypt_event(self, encrypt: dict, event: str):
        log_rows = store.rows("call_log")
        eid = str(encrypt["id"]).zfill(4)
        log_rows.append({
            "id": max((int(r.get("id", 0)) for r in log_rows), default=0) + 1,
            "记录编号": f"CALL-ENC-{eid}",
            "接口端点": "data_encrypt",
            "请求参数": encrypt.get("加密算法", ""),
            "响应状态": "200",
            "响应耗时(ms)": 30,
            "返回行数": 0,
            "错误信息": "",
            "调用时间": "2026-11-17",
            "status": "成功", "pending": False, "abnormal": False,
        })
        call_duration.standardize_record(log_rows[-1], batch_id="PIPELINE")

    def _notify_subscribers_cert_update(self, encrypt: dict):
        """通知订阅方证书已更新。"""
        for sub in store.rows("subscriber"):
            sub["最后推送"] = "2026-11-17"
            sub["消费状态"] = "待消费"
        for wh in store.rows("webhook_router"):
            wh["最后触发"] = "2026-11-17"

    def _trigger_cert_expiry_govern(self, encrypt: dict):
        gov_rows = store.rows("govern_report")
        eid = str(encrypt["id"]).zfill(4)
        gov_rows.append({
            "id": max((int(r.get("id", 0)) for r in gov_rows), default=0) + 1,
            "报告编号": f"GOVE-ENC-{eid}",
            "评估范围": f"证书过期:{encrypt.get('数据范围', '')}",
            "合规指标": "传输安全合规",
            "当前得分": 20.0,
            "改进事项": f"加密配置{eid}:{encrypt.get('加密算法', '')}证书过期",
            "评估周期": "2026-Q4",
            "负责人": "安全组",
            "生成时间": "2026-11-17",
            "status": "待生成", "pending": True, "abnormal": True,
        })

    def _alert_monitor(self, encrypt: dict):
        """登记监控告警。"""
        mon_rows = store.rows("api_monitor")
        eid = str(encrypt["id"]).zfill(4)
        mon_rows.append({
            "id": max((int(r.get("id", 0)) for r in mon_rows), default=0) + 1,
            "指标编号": f"API_-ENC-{eid}",
            "目标端点": "data_encrypt",
            "监控类型": "证书过期检测",
            "采集间隔": "60s",
            "告警阈值": encrypt.get("证书指纹", "EXPIRED"),
            "告警渠道": "webhook+Pager",
            "静默时段": encrypt.get("生效时段", ""),
            "最近采样": "2026-11-17",
            "status": "已告警", "pending": True, "abnormal": True,
        })

    def check_encrypt_coverage(self) -> dict:
        """跨模块加密覆盖度统计。"""
        encrypts = store.rows("data_encrypt")
        auth_profiles = store.rows("auth_profile")
        subscribers = store.rows("subscriber")
        return {
            "total_configs": len(encrypts),
            "active_configs": sum(1 for e in encrypts if e.get("status") == "已启用"),
            "expired_certs": sum(1 for e in encrypts if e.get("status") == "证书过期"),
            "auth_profiles": len(auth_profiles),
            "subscribers_affected": len(subscribers),
        }