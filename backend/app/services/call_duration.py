"""调用耗时标准化：把新老耗时字段收敛到带单位溯源的标准耗时。

背景
----
调用日志里同时存在两类历史耗时：

* ``响应耗时``：人工登记的自由文本，可能带单位（``0.2 s``），也可能只有
  数值（``32``）；
* ``响应耗时(ms)``：采集动作自动写入的毫秒数（``120``）。

监控早期直接读取 ``响应耗时(ms)``，字段缺失时落到 0，于是人工登记的
真实耗时会被算成零延迟。本模块统一做三件事：

1. 把可确定单位的耗时换算成毫秒，写入 ``规范耗时(ms)``，并保留
   ``耗时单位`` / ``耗时换算依据`` / ``耗时来源字段`` 等溯源信息；
2. 缺单位的旧值不按 0、也不按默认单位入账，而是标记为未换算，供报告和
   监控分母剔除；
3. 回填支持分批（`limit`）、影子预览（`dry_run`）、中断续跑、校对、
   回滚和重算；重复提交同一 ``记录编号`` 的调用只保留一条记录。

标准字段
--------
``规范耗时(ms)``   换算后的毫秒数（整数或小数）
``耗时单位``       原始单位（ms/s/us/ns/min/h）
``耗时来源字段``   实际采用的旧字段名
``耗时换算依据``   单位与换算关系，如 ``1 s = 1000 ms``
``耗时迁移状态``   已换算 / 未换算 / 已回滚
``耗时回填批次``   写入该标准值的回填批次
``耗时修订``       每重算一次递增，便于追溯
``耗时校对单位``   人工校对时给缺单位旧值指定的单位
``耗时未换算原因`` 未转换时的可读原因
"""
from __future__ import annotations

import re
from datetime import datetime
from typing import Any

from app.store import store

# ---- 标准字段与旧字段常量 -------------------------------------------------

CANONICAL_FIELD = "规范耗时(ms)"
UNIT_FIELD = "耗时单位"
SOURCE_FIELD = "耗时来源字段"
BASIS_FIELD = "耗时换算依据"
STATUS_FIELD = "耗时迁移状态"
BATCH_FIELD = "耗时回填批次"
REVISION_FIELD = "耗时修订"
REASON_FIELD = "耗时未换算原因"
OVERRIDE_UNIT_FIELD = "耗时校对单位"
OVERRIDE_BASIS_FIELD = "耗时校对依据"
CONFLICT_FIELD = "耗时来源冲突"
ROLLBACK_BATCH_FIELD = "耗时回滚批次"

LEGACY_MANUAL = "响应耗时"
LEGACY_AUTO = "响应耗时(ms)"

STATUS_CONVERTED = "已换算"
STATUS_UNCONVERTED = "未换算"
STATUS_ROLLED_BACK = "已回滚"

# 标准单位 -> (换算到毫秒的系数, 展示名)
_UNIT_TABLE: dict[str, tuple[float, str]] = {
    "ms": (1.0, "ms"),
    "millisecond": (1.0, "ms"),
    "milliseconds": (1.0, "ms"),
    "毫秒": (1.0, "ms"),
    "s": (1000.0, "s"),
    "sec": (1000.0, "s"),
    "secs": (1000.0, "s"),
    "second": (1000.0, "s"),
    "seconds": (1000.0, "s"),
    "秒": (1000.0, "s"),
    "us": (0.001, "us"),
    "µs": (0.001, "us"),
    "μs": (0.001, "us"),
    "microsecond": (0.001, "us"),
    "microseconds": (0.001, "us"),
    "微秒": (0.001, "us"),
    "ns": (0.000001, "ns"),
    "nanosecond": (0.000001, "ns"),
    "nanoseconds": (0.000001, "ns"),
    "纳秒": (0.000001, "ns"),
    "m": (60000.0, "min"),
    "min": (60000.0, "min"),
    "mins": (60000.0, "min"),
    "minute": (60000.0, "min"),
    "minutes": (60000.0, "min"),
    "分钟": (60000.0, "min"),
    "h": (3600000.0, "h"),
    "hr": (3600000.0, "h"),
    "hrs": (3600000.0, "h"),
    "hour": (3600000.0, "h"),
    "hours": (3600000.0, "h"),
    "小时": (3600000.0, "h"),
}

_QUANTITY = re.compile(
    r"^\s*([+-]?(?:\d+(?:\.\d*)?|\.\d+))\s*([A-Za-zµμ一-鿿]*)\s*$"
)

# 运行期状态：批次、回滚快照、部署阶段。数据仓库本身仍是内存实现，这里同样
# 只依赖进程内状态，保证与 /api 资源接口的现有一致性。
_STATE: dict[str, Any] = {
    "phase": "未开始",
    "batches": {},
    "last_batch": None,
    "rollback_snapshot": [],
    "rollback_batches": [],
    "batch_seq": 0,
}

_MIGRATION_FIELDS = (
    CANONICAL_FIELD,
    UNIT_FIELD,
    SOURCE_FIELD,
    BASIS_FIELD,
    STATUS_FIELD,
    BATCH_FIELD,
    REVISION_FIELD,
    REASON_FIELD,
    OVERRIDE_UNIT_FIELD,
    OVERRIDE_BASIS_FIELD,
    CONFLICT_FIELD,
    "耗时冲突明细",
)


# ---- 换算 -----------------------------------------------------------------


def _clean_number(value: float) -> int | float:
    """整数值输出 int，避免 200.0 与 200 的展示差异。"""
    if abs(value - round(value)) < 1e-9:
        return int(round(value))
    return round(value, 6)


def _normalise_unit(token: str) -> str | None:
    token = token.strip().strip("()（）").strip().casefold()
    token = token.replace(" ", "")
    if not token:
        return None
    return token


def parse_duration(
    value: Any,
    *,
    default_unit: str | None = None,
    basis_note: str | None = None,
) -> dict[str, Any]:
    """解析单个历史耗时值。

    返回值中 ``resolved`` 表示单位是否可确定。缺单位时返回未解析，
    ``reason`` 会明确写出原因，调用方不得把它当成 0 或默认毫秒。
    """
    raw = value
    if value is None:
        return _unresolved(raw, "耗时为 null")
    if isinstance(value, bool):
        return _unresolved(raw, "耗时不是数值")
    if isinstance(value, (int, float)):
        text = str(value)
    else:
        text = str(value).strip()
        if not text:
            return _unresolved(raw, "耗时为空值")
        text = text.replace(",", "").replace("，", "")

    match = _QUANTITY.match(text)
    if match is None:
        return _unresolved(raw, f"无法解析耗时文本「{text}」")

    number = float(match.group(1))
    token = _normalise_unit(match.group(2))
    unit = token or (_normalise_unit(default_unit) if default_unit else None)

    if unit is None:
        return _unresolved(
            raw,
            "缺少单位，不能按零延迟或默认单位入账",
            number=number,
        )
    if unit not in _UNIT_TABLE:
        return _unresolved(raw, f"无法识别耗时单位「{match.group(2)}」", number=number)
    if number < 0:
        return _unresolved(raw, "耗时为负值", number=number)

    factor, display_unit = _UNIT_TABLE[unit]
    milliseconds = _clean_number(number * factor)
    if basis_note:
        basis = basis_note
    elif display_unit == "ms":
        basis = "毫秒原值，1 ms = 1 ms"
    else:
        basis = f"1 {display_unit} = {factor:g} ms"
    return {
        "resolved": True,
        "ms": milliseconds,
        "unit": display_unit,
        "basis": basis,
        "number": _clean_number(number),
        "reason": None,
        "raw": raw,
    }


def _unresolved(raw: Any, reason: str, *, number: float | None = None) -> dict[str, Any]:
    return {
        "resolved": False,
        "ms": None,
        "unit": None,
        "basis": None,
        "number": number,
        "reason": reason,
        "raw": raw,
    }


def _unit_token_from_raw(raw: Any) -> str | None:
    """从原始旧值里取单位词；只有数值时返回 None。"""
    match = _QUANTITY.match(str(raw).strip().replace(",", "").replace("，", ""))
    if match is None:
        return None
    return _normalise_unit(match.group(2)) or None


def _has_legacy(record: dict[str, Any], field: str) -> bool:
    if field not in record:
        return False
    value = record.get(field)
    return value is not None and str(value).strip() != ""


def _legacy_snapshot(record: dict[str, Any]) -> dict[str, Any]:
    """旧字段只读快照，用于报告核对可读取性，绝不参与写回。"""
    snapshot: dict[str, Any] = {}
    for field in (LEGACY_MANUAL, LEGACY_AUTO):
        if field in record:
            snapshot[field] = record.get(field)
    return snapshot


def _override_unit(record: dict[str, Any]) -> str | None:
    unit = record.get(OVERRIDE_UNIT_FIELD)
    if unit is None:
        return None
    token = _normalise_unit(str(unit))
    return token if token in _UNIT_TABLE else None


def resolve_legacy(record: dict[str, Any]) -> dict[str, Any]:
    """仅依据旧字段（以及人工校对单位）解析，不读取标准字段。

    返回 ``resolved/ms/unit/basis/source_field/raw/reason/conflict``。
    """
    manual_present = _has_legacy(record, LEGACY_MANUAL)
    auto_present = _has_legacy(record, LEGACY_AUTO)
    manual_raw = record.get(LEGACY_MANUAL)
    auto_raw = record.get(LEGACY_AUTO)

    manual = (
        parse_duration(manual_raw, default_unit=_override_unit(record))
        if manual_present
        else None
    )
    if manual is not None and manual["resolved"]:
        override = _override_unit(record)
        if override and _unit_token_from_raw(manual_raw) is None:
            manual["basis"] = (
                f"人工校对单位: 1 {manual['unit']} = {_UNIT_TABLE[override][0]:g} ms"
            )
    auto = parse_duration(auto_raw, default_unit="ms") if auto_present else None

    if manual and manual["resolved"] and auto and auto["resolved"]:
        conflict = manual["ms"] != auto["ms"]
        chosen = auto
        return {
            **chosen,
            "source_field": LEGACY_AUTO,
            "source_kind": "legacy",
            "conflict": conflict,
            "manual": manual,
            "auto": auto,
            "reason": "两类历史耗时不一致，标准值采用自动写入的毫秒字段" if conflict else None,
        }
    if auto and auto["resolved"]:
        return {**auto, "source_field": LEGACY_AUTO, "source_kind": "legacy",
                "conflict": False, "manual": manual, "auto": auto}
    if manual and manual["resolved"]:
        return {**manual, "source_field": LEGACY_MANUAL, "source_kind": "legacy",
                "conflict": False, "manual": manual, "auto": auto}

    # 都不可用：合并出最可读的原因。
    reason = None
    for item in (auto, manual):
        if item is not None and item.get("reason"):
            reason = item["reason"]
            break
    if reason is None:
        reason = "缺少可用的响应耗时字段"
    source = LEGACY_AUTO if auto_present else (LEGACY_MANUAL if manual_present else None)
    return {
        "resolved": False, "ms": None, "unit": None, "basis": None,
        "source_field": source, "source_kind": "legacy", "raw":
            record.get(source) if source else None,
        "reason": reason, "conflict": False, "manual": manual, "auto": auto,
    }


def resolve_effective(record: dict[str, Any]) -> dict[str, Any]:
    """解析一条记录当前实际可采用的耗时。

    优先使用已回填的 ``规范耗时(ms)``；标准值缺失（回填中断、回滚后或
    尚未迁移）时实时回退到旧字段，保证监控对混合记录仍能计算。
    """
    canonical = record.get(CANONICAL_FIELD)
    legacy = resolve_legacy(record)
    status = record.get(STATUS_FIELD)

    if canonical is not None and status != STATUS_UNCONVERTED:
        ms = _clean_number(float(canonical))
        source_field = record.get(SOURCE_FIELD) or "规范耗时(ms)"
        return {
            "resolved": True,
            "ms": ms,
            "unit": record.get(UNIT_FIELD),
            "basis": record.get(BASIS_FIELD),
            "source_field": source_field,
            "source_kind": "canonical",
            "raw": record.get(source_field),
            "reason": None,
            "conflict": bool(record.get(CONFLICT_FIELD)),
            # 两类历史字段数值不一致时也要人工复核，避免只报不审。
            "needs_review": bool(
                record.get(CONFLICT_FIELD)
                or (legacy.get("resolved") and legacy.get("ms") != ms)
            ),
            "legacy": _legacy_snapshot(record),
        }

    return {
        **legacy,
        "source_kind": legacy.get("source_kind", "legacy"),
        "needs_review": False,
        "legacy": _legacy_snapshot(record),
    }




# ---- 回填 -----------------------------------------------------------------


def _next_batch_id(prefix: str = "DUR") -> str:
    _STATE["batch_seq"] = int(_STATE["batch_seq"]) + 1
    return f"{prefix}-{int(_STATE['batch_seq']):04d}"


def _today() -> str:
    return datetime.now().strftime("%Y-%m-%d")


def _set_standard(record: dict[str, Any], resolution: dict[str, Any], batch_id: str) -> str:
    """把解析结果写入标准字段，返回本次处理动作。

    旧字段在此函数中只读，不会被覆盖或删除。同一批次内对同一条记录重
    复提交是幂等的：值一致只做校对，不重复累加修订号。
    """
    previous = record.get(CANONICAL_FIELD)
    action = "converted"
    expected_unit = resolution.get("unit")
    expected_basis = resolution.get("basis")
    if previous is not None:
        same = (
            _clean_number(float(previous)) == resolution["ms"]
            and record.get(UNIT_FIELD) == expected_unit
            and record.get(BASIS_FIELD) == expected_basis
            and record.get(SOURCE_FIELD) == resolution.get("source_field")
            and record.get(STATUS_FIELD) == STATUS_CONVERTED
        )
        if same:
            if record.get(BATCH_FIELD) != batch_id:
                record[BATCH_FIELD] = batch_id
            return "verified"
        action = "corrected"

    record[CANONICAL_FIELD] = resolution["ms"]
    record[UNIT_FIELD] = expected_unit
    record[SOURCE_FIELD] = resolution.get("source_field")
    record[BASIS_FIELD] = expected_basis
    record[STATUS_FIELD] = STATUS_CONVERTED
    record[BATCH_FIELD] = batch_id
    record[REVISION_FIELD] = int(record.get(REVISION_FIELD, 0) or 0) + 1
    record.pop(REASON_FIELD, None)
    record[CONFLICT_FIELD] = bool(resolution.get("conflict"))
    if resolution.get("manual") and resolution.get("auto") and resolution.get("conflict"):
        record["耗时冲突明细"] = (
            f"{LEGACY_MANUAL}={resolution['manual'].get('raw')}"
            f"({resolution['manual'].get('ms')}ms) / "
            f"{LEGACY_AUTO}={resolution['auto'].get('raw')}"
            f"({resolution['auto'].get('ms')}ms)"
        )
    else:
        record.pop("耗时冲突明细", None)
    return action


def _set_unconverted(record: dict[str, Any], resolution: dict[str, Any], batch_id: str) -> str:
    previous = record.get(CANONICAL_FIELD)
    reason = resolution.get("reason") or "无法确定耗时单位"
    unchanged = (
        previous is None
        and record.get(STATUS_FIELD) == STATUS_UNCONVERTED
        and record.get(REASON_FIELD) == reason
        and record.get(SOURCE_FIELD) == resolution.get("source_field")
    )
    if unchanged:
        if record.get(BATCH_FIELD) != batch_id:
            record[BATCH_FIELD] = batch_id
        return "verified"
    record[CANONICAL_FIELD] = None
    record[UNIT_FIELD] = None
    record[SOURCE_FIELD] = resolution.get("source_field")
    record[BASIS_FIELD] = None
    record[STATUS_FIELD] = STATUS_UNCONVERTED
    record[REASON_FIELD] = reason
    record[BATCH_FIELD] = batch_id
    record[REVISION_FIELD] = int(record.get(REVISION_FIELD, 0) or 0) + 1
    record[CONFLICT_FIELD] = False
    return "invalidated" if previous is not None else "unconverted"


def standardize_record(
    record: dict[str, Any],
    *,
    batch_id: str | None = None,
    dry_run: bool = False,
) -> dict[str, Any]:
    """按旧字段重算一条记录的标准耗时。"""
    resolution = resolve_legacy(record)
    if dry_run:
        # 预览不消耗批次号，保证影子核对不干扰真实回填批次。
        action = "would_convert" if resolution["resolved"] else "would_skip"
        return {"action": action, "resolution": resolution, "batch_id": None}
    batch_id = batch_id or _next_batch_id()
    if resolution["resolved"]:
        action = _set_standard(record, resolution, batch_id)
    else:
        action = _set_unconverted(record, resolution, batch_id)
    return {"action": action, "resolution": resolution, "batch_id": batch_id}


def _batch_summary(batch: dict[str, Any]) -> dict[str, Any]:
    return {
        "batch_id": batch.get("batch_id"),
        "status": batch.get("status"),
        "phase": batch.get("phase"),
        "limit": batch.get("limit"),
        "processed": batch.get("processed", 0),
        "converted": batch.get("converted", 0),
        "verified": batch.get("verified", 0),
        "corrected": batch.get("corrected", 0),
        "unconverted": batch.get("unconverted", 0),
        "invalidated": batch.get("invalidated", 0),
        "remaining": batch.get("remaining", 0),
        "total_records": batch.get("total_records", 0),
        "cursor": batch.get("cursor", -1),
        "started_at": batch.get("started_at"),
        "finished_at": batch.get("finished_at"),
        "verified_basis": batch.get("verified_basis", []),
    }


def run_backfill(
    *,
    limit: int | None = None,
    dry_run: bool = False,
    batch_id: str | None = None,
) -> dict[str, Any]:
    """执行/续跑耗时回填。

    * ``limit``：本次最多处理多少条，便于采集流水线分期上线；
    * ``dry_run``：影子核对，只预览不写标准字段；
    * ``batch_id``：指定批次可校对上次中断的结果，不传则自动续跑最近
      一个中断批次；
    * 重复执行是幂等的：已一致的标准值只做校对，不重复入账。
    """
    records = list(store.rows("call_log"))
    if limit is not None:
        limit = max(0, int(limit))

    if dry_run:
        preview = [standardize_record(record, dry_run=True) for record in records]
        converted = sum(1 for item in preview if item["action"] == "would_convert")
        _STATE["phase"] = "影子核对"
        unconverted = [
            {
                "id": record.get("id"),
                "记录编号": record.get("记录编号"),
                "raw": item["resolution"].get("raw"),
                "reason": item["resolution"].get("reason"),
            }
            for record, item in zip(records, preview)
            if not item["resolution"]["resolved"]
        ]
        return {
            "batch_id": None,
            "status": "预览",
            "phase": _STATE["phase"],
            "dry_run": True,
            "processed": 0,
            "would_convert": converted,
            "would_skip": len(preview) - converted,
            "total_records": len(records),
            "remaining": len(records),
            "unconverted_records": unconverted,
        }

    # 选择批次：显式指定 > 自动续跑当前未完成批次 > 新建批次。
    batch: dict[str, Any] | None = None
    if batch_id:
        batch = _STATE["batches"].get(batch_id)
        if batch is None:
            batch = _new_batch(batch_id)
    else:
        last_id = _STATE.get("last_batch")
        last = _STATE["batches"].get(last_id) if last_id else None
        if last and last.get("status") == "中断":
            batch = last
        else:
            batch = _new_batch(_next_batch_id())
    batch_id = str(batch["batch_id"])

    cursor = int(batch.get("cursor", -1))
    start = cursor + 1
    if limit is not None:
        batch["limit"] = limit
    remaining_before = max(0, len(records) - start)
    process_count = remaining_before if limit is None else min(limit, remaining_before)

    counters = {"processed": 0, "converted": 0, "verified": 0,
                "corrected": 0, "unconverted": 0, "invalidated": 0}
    processed_ids: list[int] = []
    for index in range(start, start + process_count):
        record = records[index]
        result = standardize_record(record, batch_id=batch_id)
        action = result["action"]
        counters["processed"] += 1
        if action in counters:
            counters[action] += 1
        processed_ids.append(int(record.get("id", 0)))

    new_cursor = start + process_count - 1 if process_count else cursor
    remaining = max(0, len(records) - (new_cursor + 1))
    finished = remaining == 0

    # 完成后续跑一遍校对：逐条重算旧字段，核对单位与换算依据。
    verified_basis: list[dict[str, Any]] = []
    if finished and not batch.get("verified_at"):
        for record in records:
            before = (
                record.get(CANONICAL_FIELD), record.get(UNIT_FIELD),
                record.get(BASIS_FIELD), record.get(STATUS_FIELD),
            )
            standardize_record(record, batch_id=batch_id)
            after = (
                record.get(CANONICAL_FIELD), record.get(UNIT_FIELD),
                record.get(BASIS_FIELD), record.get(STATUS_FIELD),
            )
            if before != after:
                counters["corrected"] += 1
            else:
                counters["verified"] += 1
            verified_basis.append({
                "record_id": record.get("id"),
                "记录编号": record.get("记录编号"),
                "unit": record.get(UNIT_FIELD),
                "basis": record.get(BASIS_FIELD),
                "canonical_ms": record.get(CANONICAL_FIELD),
                "result": "修正" if before != after else "一致",
            })
        batch["verified_at"] = _today()

    for key, value in counters.items():
        batch[key] = int(batch.get(key, 0)) + value
    batch["cursor"] = new_cursor
    batch["remaining"] = remaining
    batch["total_records"] = len(records)
    batch["last_run_at"] = _today()
    batch["verified_basis"] = verified_basis
    if finished:
        batch["status"] = "已完成"
        batch["finished_at"] = _today()
        _STATE["phase"] = "已切换"
    else:
        batch["status"] = "中断"
        batch["finished_at"] = None
        _STATE["phase"] = "回填中"
    batch["phase"] = _STATE["phase"]
    _STATE["last_batch"] = batch_id

    summary = _batch_summary(batch)
    summary.update({
        "processed_this_run": counters["processed"],
        "processed_ids": processed_ids,
        "sample_total": len(records),
        "sample_used": sum(1 for record in records if resolve_effective(record)["resolved"]),
        "unconverted_records": _unconverted_entries(records),
    })
    return summary


def _new_batch(batch_id: str) -> dict[str, Any]:
    batch = {
        "batch_id": batch_id,
        "status": "新建",
        "phase": "回填中",
        "limit": None,
        "processed": 0,
        "converted": 0,
        "verified": 0,
        "corrected": 0,
        "unconverted": 0,
        "invalidated": 0,
        "remaining": None,
        "total_records": 0,
        "cursor": -1,
        "started_at": _today(),
        "finished_at": None,
        "verified_basis": [],
    }
    _STATE["batches"][batch_id] = batch
    _STATE["phase"] = "回填中"
    return batch


# ---- 回滚与重算 -----------------------------------------------------------


def rollback(*, batch_id: str | None = None) -> dict[str, Any]:
    """回滚标准字段，恢复迁移前的调用日志形态。

    只清除 ``规范耗时(ms)`` 及溯源字段，旧字段 ``响应耗时`` /
    ``响应耗时(ms)`` 原样保留，便于再次重算与对账。
    """
    records = list(store.rows("call_log"))
    rollback_id = _next_batch_id("RBK")
    affected = 0
    snapshot: list[dict[str, Any]] = []
    for record in records:
        record_batch = record.get(BATCH_FIELD)
        if batch_id and record_batch != batch_id:
            continue
        has_standard = any(field in record for field in _MIGRATION_FIELDS)
        if not has_standard:
            continue
        snapshot.append({
            "id": record.get("id"),
            "记录编号": record.get("记录编号"),
            "batch_id": record_batch,
            **{field: record.get(field) for field in _MIGRATION_FIELDS},
            "legacy": _legacy_snapshot(record),
        })
        for field in _MIGRATION_FIELDS:
            record.pop(field, None)
        record[STATUS_FIELD] = STATUS_ROLLED_BACK
        record[ROLLBACK_BATCH_FIELD] = rollback_id
        affected += 1

    if batch_id and batch_id in _STATE["batches"]:
        _STATE["batches"][batch_id]["status"] = "已回滚"
        _STATE["batches"][batch_id]["phase"] = "已回滚"
    for item in _STATE["batches"].values():
        if item.get("status") == "中断":
            item["status"] = "已回滚"
    _STATE["last_batch"] = None
    _STATE["phase"] = "已回滚"
    _STATE["rollback_snapshot"] = snapshot
    _STATE["rollback_batches"].append({
        "rollback_id": rollback_id,
        "batch_id": batch_id,
        "affected": affected,
        "at": _today(),
    })
    return {
        "rollback_id": rollback_id,
        "batch_id": batch_id,
        "affected": affected,
        "phase": _STATE["phase"],
        "snapshot_count": len(snapshot),
        "legacy_readable": _legacy_readable(records),
        "records": snapshot,
    }


def recompute(
    *,
    record_no: str | None = None,
    unit: str | None = None,
    batch_id: str | None = None,
    dry_run: bool = False,
) -> dict[str, Any]:
    """按旧字段重算标准耗时，可对缺单位旧值校对单位。

    ``record_no`` 为空时重算全部调用记录；``unit`` 用于人工校对（例如把
    只有数值 ``32`` 的旧值按 ``ms`` 重新入账），换算依据会写明人工校对。
    """
    records = list(store.rows("call_log"))
    if record_no:
        records = [
            record for record in records
            if str(record.get("记录编号")) == str(record_no)
            or str(record.get("id")) == str(record_no)
        ]
    if not records:
        return {"ok": False, "message": f"未找到调用记录：{record_no}", "processed": 0}

    batch_id = batch_id or _next_batch_id("REC")
    counters = {"processed": 0, "converted": 0, "verified": 0,
                "corrected": 0, "unconverted": 0, "invalidated": 0}
    preview: list[dict[str, Any]] = []
    if unit is not None:
        token = _normalise_unit(str(unit))
        if token not in _UNIT_TABLE:
            return {"ok": False, "message": f"无法识别的校对单位：{unit}", "processed": 0}
    for record in records:
        if unit is not None:
            # 只给缺单位的旧值补校对单位；已写明单位的旧值以原文为准。
            raw = record.get(LEGACY_MANUAL)
            if _has_legacy(record, LEGACY_MANUAL) and _unit_token_from_raw(raw) is None:
                record[OVERRIDE_UNIT_FIELD] = token
                record[OVERRIDE_BASIS_FIELD] = (
                    f"人工校对单位: 1 {_UNIT_TABLE[token][1]} = {_UNIT_TABLE[token][0]:g} ms"
                )
        result = standardize_record(record, batch_id=batch_id, dry_run=dry_run)
        counters["processed"] += 1
        if dry_run:
            preview.append({
                "记录编号": record.get("记录编号"),
                "resolution": result["resolution"],
            })
        else:
            action = result["action"]
            if action in counters:
                counters[action] += 1
    if dry_run:
        return {
            "ok": True, "dry_run": True, "batch_id": None,
            "processed": counters["processed"], "preview": preview,
        }
    return {
        "ok": True,
        "batch_id": batch_id,
        "unit": unit,
        **counters,
        "sample_total": len(store.rows("call_log")),
        "sample_used": sum(
            1 for record in store.rows("call_log") if resolve_effective(record)["resolved"]
        ),
        "unconverted_records": _unconverted_entries(store.rows("call_log")),
    }


# ---- 幂等写入 -------------------------------------------------------------


def ingest_call(values: dict[str, Any]) -> dict[str, Any]:
    """幂等登记一笔调用：同一 ``记录编号`` 只保留一条记录。

    重复提交不新增行；旧字段以首次登记值为准（只补空缺），标准耗时则按
    最新旧值重新校对，保证重试不会重复入账。
    """
    record_no = str(values.get("记录编号") or "").strip()
    if not record_no:
        return {"ok": False, "message": "缺少必填字段：记录编号", "created": False,
                "entry": None, "duplicate": False}

    existing = next(
        (row for row in store.rows("call_log")
         if str(row.get("记录编号")) == record_no),
        None,
    )
    if existing is not None:
        # 只补旧字段空缺，不覆盖首次登记值；同时重算标准耗时。
        for field in (LEGACY_MANUAL, LEGACY_AUTO):
            if _has_legacy(values, field) and not _has_legacy(existing, field):
                existing[field] = values[field]
        standardize_record(existing, batch_id="INGEST")
        return {
            "ok": True,
            "message": f"调用记录 {record_no} 已存在，本次按重复提交处理，未新增记录",
            "created": False,
            "duplicate": True,
            "entry": existing,
        }

    rows = store.rows("call_log")
    entry: dict[str, Any] = {
        "id": max((int(row.get("id", 0)) for row in rows), default=0) + 1,
        "status": str(values.get("status") or "成功"),
        "pending": bool(values.get("pending", False)),
        "abnormal": bool(values.get("abnormal", False)),
    }
    # 调用日志的所有可读字段原样保留，包含两类历史耗时。
    for field, value in values.items():
        if field in {"id", "status", "pending", "abnormal"}:
            continue
        entry[field] = value
    entry.setdefault("记录编号", record_no)
    rows.append(entry)
    standardize_record(entry, batch_id="INGEST")
    return {
        "ok": True,
        "message": f"调用记录 {record_no} 已登记并完成耗时换算",
        "created": True,
        "duplicate": False,
        "entry": entry,
    }


# ---- 统计与报告 -----------------------------------------------------------


def _percentile(sorted_values: list[float], ratio: float) -> int | float | None:
    if not sorted_values:
        return None
    if len(sorted_values) == 1:
        return _clean_number(sorted_values[0])
    position = (len(sorted_values) - 1) * ratio
    lower = int(position)
    upper = min(lower + 1, len(sorted_values) - 1)
    fraction = position - lower
    value = sorted_values[lower] + (sorted_values[upper] - sorted_values[lower]) * fraction
    return _clean_number(value)


def latency_summary(records: list[dict[str, Any]]) -> dict[str, Any]:
    """按规范耗时计算区间延迟。

    分母只包含单位可确定的记录；缺单位记录计入 ``unconverted`` 并从
    平均/分位数中剔除，绝不按 0 或默认单位补齐。
    """
    resolved: list[float] = []
    unconverted: list[dict[str, Any]] = []
    for record in records:
        item = resolve_effective(record)
        if item["resolved"]:
            resolved.append(float(item["ms"]))
        else:
            unconverted.append({
                "id": record.get("id"),
                "记录编号": record.get("记录编号"),
                "raw": item.get("raw"),
                "source_field": item.get("source_field"),
                "reason": item.get("reason"),
            })
    ordered = sorted(resolved)
    used = len(resolved)
    total = len(records)
    return {
        "sample_total": total,
        "sample_used": used,
        "unconverted": len(unconverted),
        "coverage": round(used / total, 4) if total else None,
        "avg_latency_ms": _clean_number(sum(resolved) / used) if used else None,
        "min_latency_ms": _clean_number(ordered[0]) if ordered else None,
        "max_latency_ms": _clean_number(ordered[-1]) if ordered else None,
        "p50_latency_ms": _percentile(ordered, 0.50),
        "p95_latency_ms": _percentile(ordered, 0.95),
        "total_latency_ms": _clean_number(sum(resolved)) if used else None,
        "unconverted_records": unconverted,
    }


def _unconverted_entries(records: list[dict[str, Any]]) -> list[dict[str, Any]]:
    entries: list[dict[str, Any]] = []
    for record in records:
        item = resolve_effective(record)
        if not item["resolved"]:
            entries.append({
                "id": record.get("id"),
                "记录编号": record.get("记录编号"),
                "接口端点": record.get("接口端点"),
                "source_field": item.get("source_field"),
                "raw": item.get("raw"),
                "legacy": item.get("legacy"),
                "reason": item.get("reason"),
                "status": record.get(STATUS_FIELD),
            })
    return entries


def _legacy_readable(records: list[dict[str, Any]]) -> dict[str, Any]:
    checked = 0
    readable = 0
    missing: list[dict[str, Any]] = []
    for record in records:
        if not any(field in record for field in (LEGACY_MANUAL, LEGACY_AUTO)):
            continue
        checked += 1
        if any(_has_legacy(record, field) for field in (LEGACY_MANUAL, LEGACY_AUTO)):
            readable += 1
        else:
            missing.append({
                "id": record.get("id"),
                "记录编号": record.get("记录编号"),
                "legacy": _legacy_snapshot(record),
            })
    return {"checked": checked, "readable": readable, "missing": missing}


def migration_report() -> dict[str, Any]:
    """部署后报告：实际采用的耗时、换算依据、样本分母和未转换记录。"""
    records = list(store.rows("call_log"))
    adopted: list[dict[str, Any]] = []
    for record in records:
        item = resolve_effective(record)
        if not item["resolved"]:
            continue
        adopted.append({
            "id": record.get("id"),
            "记录编号": record.get("记录编号"),
            "接口端点": record.get("接口端点"),
            "source_kind": item.get("source_kind"),
            "source_field": item.get("source_field"),
            "raw": item.get("raw"),
            "unit": item.get("unit"),
            "basis": item.get("basis"),
            "canonical_ms": item.get("ms"),
            "status": record.get(STATUS_FIELD),
            "legacy": item.get("legacy"),
            "needs_review": item.get("needs_review", False),
            "conflict": item.get("conflict", False),
            "conflict_detail": record.get("耗时冲突明细"),
        })
    summary = latency_summary(records)
    return {
        "phase": _STATE["phase"],
        "generated_at": _today(),
        "batches": [_batch_summary(batch) for batch in _STATE["batches"].values()],
        "last_rollback": _STATE["rollback_batches"][-1] if _STATE["rollback_batches"] else None,
        "totals": {
            "records": len(records),
            "adopted": len(adopted),
            "unconverted": len(summary["unconverted_records"]),
            "needs_review": sum(1 for item in adopted if item["needs_review"]),
            "conflicts": sum(1 for item in adopted if item["conflict"]),
        },
        "sample": {
            "denominator": summary["sample_used"],
            "total_records": summary["sample_total"],
            "unconverted": summary["unconverted"],
            "coverage": summary["coverage"],
            "avg_latency_ms": summary["avg_latency_ms"],
            "p50_latency_ms": summary["p50_latency_ms"],
            "p95_latency_ms": summary["p95_latency_ms"],
        },
        "adopted": adopted,
        "unconverted": summary["unconverted_records"],
        "legacy_readable": _legacy_readable(records),
        "monitor_stats": [
            {
                "指标编号": monitor.get("指标编号"),
                "目标端点": monitor.get("目标端点"),
                "stats": monitor.get("stats"),
            }
            for monitor in store.rows("api_monitor")
            if monitor.get("stats")
        ],
    }


def migration_status() -> dict[str, Any]:
    """当前部署阶段与批次状态。"""
    records = list(store.rows("call_log"))
    return {
        "phase": _STATE["phase"],
        "standard_field": CANONICAL_FIELD,
        "legacy_fields": [LEGACY_MANUAL, LEGACY_AUTO],
        "batch_seq": _STATE["batch_seq"],
        "batches": [_batch_summary(batch) for batch in _STATE["batches"].values()],
        "last_rollback": _STATE["rollback_batches"][-1] if _STATE["rollback_batches"] else None,
        "sample_total": len(records),
        "sample_used": sum(1 for record in records if resolve_effective(record)["resolved"]),
        "unconverted": len(_unconverted_entries(records)),
    }


def reset_runtime_state() -> None:
    """测试或重新部署时清理迁移批次与回滚快照。"""
    _STATE.update({
        "phase": "未开始",
        "batches": {},
        "last_batch": None,
        "rollback_snapshot": [],
        "rollback_batches": [],
        "batch_seq": 0,
    })

