from .domain import DomainError

ENTITY_TYPE = "water_contamination"
INITIAL_STATUS = "detected"
CREATE_ROLES = {"analyst", "dispatcher"}
SOURCE_ROLES = {"analyst", "dispatcher", "field_operator", "lab"}
ACTION_ROLES = {
    "verify": {"analyst", "dispatcher"},
    "advise": {"coordinator", "dispatcher"},
    "switch_source": {"coordinator"},
    "flush": {"field_operator"},
    "disinfect": {"field_operator"},
    "sample": {"lab", "field_operator"},
    "restore": {"coordinator", "regulator"},
    "cancel": {"coordinator"},
}
ENFORCE_REGION = False
REGION_SENSITIVE_ACTIONS = set()
ACTION_REQUIRES_VERSION = {"advise", "switch_source", "flush", "disinfect", "sample", "restore", "cancel"}

# ---- 消毒批次（冲洗+消毒共用一台移动消毒机组）----
BATCH_ROLES = {
    "submit_batch": {"field_operator"},
    "submit_receipt": {"field_operator"},
    "retry_batch": {"field_operator", "coordinator"},
    "release_batch": {"regulator", "coordinator"},
    "set_branch_upstream": {"coordinator", "regulator"},
    "list_batches": {"analyst", "dispatcher", "field_operator", "lab", "coordinator", "regulator"},
}
# 现场角色受支路边界约束；监管可以跨支路放行
FIELD_SCOPED_ROLES = {"field_operator", "lab"}
CROSS_BRANCH_ROLES = {"regulator", "coordinator"}

BATCH_QUEUED = "queued"                 # 已排队，拿到排队号，等待机组
BATCH_IN_PROGRESS = "in_progress"       # 开工，占用机组+支路
BATCH_AWAITING_RECEIPT = "awaiting_receipt"  # 作业完成，等待回执
BATCH_COMPLETED = "completed"           # 回执成功，实际加药量已记录，释放
BATCH_RECEIPT_FAILED = "receipt_failed"  # 回执失败，保留未完成批次，占用机组待重试
BATCH_INVALIDATED = "invalidated"       # 污染来源更新，整批失效

BATCH_ACTIVE_STATUSES = {BATCH_IN_PROGRESS, BATCH_AWAITING_RECEIPT, BATCH_RECEIPT_FAILED}
BATCH_TERMINAL_STATUSES = {BATCH_COMPLETED, BATCH_INVALIDATED}
BATCH_OCCUPYING_STATUSES = BATCH_ACTIVE_STATUSES  # 占用机组/支路的状态

# 污染事件在消毒批次联动下的待复检状态
REINSPECTION_PENDING = "reinspection_pending"
ZONE_CLEARED = "cleared"
ZONE_REINSPECTION = "reinspection"


def assess(payload):
    concentration = float(payload.get("concentration", 0))
    limit = max(float(payload.get("limit", 0.000001)), 0.000001)
    ratio = concentration / limit
    population = int(payload.get("population", 0))
    score = min(100.0, ratio * 35.0 + min(population / 1000.0, 40.0))
    if score >= 80:
        level = "critical"
    elif score >= 50:
        level = "high"
    elif score >= 20:
        level = "medium"
    else:
        level = "low"
    return {"score": round(score, 2), "level": level, "ratio": round(ratio, 3)}


def _need_status(item, allowed):
    if item["status"] not in allowed:
        raise DomainError("invalid_state", "当前状态 %s 不允许执行该操作" % item["status"])


def _text(payload, name):
    value = payload.get(name)
    if not isinstance(value, str) or not value.strip():
        raise DomainError("field_required", "%s 不能为空" % name)
    return value.strip()


def apply_action(item, action, payload, actor, role):
    status = item["status"]
    current = dict(item["payload"])

    if action == "verify":
        _need_status(item, {"detected", "verified"})
        sample_count = int(payload.get("sample_count", 0) or 0)
        if sample_count < 1:
            raise DomainError("sample_required", "需要至少一份复检样本", 409)
        current["assessment"] = assess(current)
        current["verification"] = {"sample_count": sample_count, "note": payload.get("note", "")}
        return "verified", current, {"assessment": current["assessment"], "verification": current["verification"]}

    if action == "advise":
        _need_status(item, {"verified", "advisory"})
        notice_id = _text(payload, "notice_id")
        notice = {
            "notice_id": notice_id,
            "kind": _text(payload, "kind"),
            "message": _text(payload, "message"),
        }
        notices = current.setdefault("notifications", [])
        if any(existing.get("notice_id") == notice_id for existing in notices):
            raise DomainError("duplicate_notification", "同一通知编号不能重复发送", 409)
        notices.append(notice)
        return "advisory", current, {"notice": notice}

    if action == "switch_source":
        _need_status(item, {"verified", "advisory", "flushing", "disinfected", "sampled", "switched"})
        alternate = _text(payload, "alternate_source_id")
        current["alternate_source_id"] = alternate
        return "switched", current, {"alternate_source_id": alternate}

    if action == "flush":
        _need_status(item, {"advisory", "flushing", "switched"})
        zone_id = _text(payload, "zone_id")
        current.setdefault("response_actions", []).append({"type": "flush", "zone_id": zone_id})
        return "flushing", current, {"zone_id": zone_id, "type": "flush"}

    if action == "disinfect":
        _need_status(item, {"flushing", "disinfected"})
        if not payload.get("completed"):
            raise DomainError("disinfection_incomplete", "消毒尚未完成", 409)
        zone_id = _text(payload, "zone_id")
        current.setdefault("response_actions", []).append({"type": "disinfect", "zone_id": zone_id})
        return "disinfected", current, {"zone_id": zone_id, "type": "disinfect"}

    if action == "sample":
        _need_status(item, {"disinfected", "sampled", REINSPECTION_PENDING})
        result = {
            "sample_id": _text(payload, "sample_id"),
            "zone_id": _text(payload, "zone_id"),
            "concentration": float(payload.get("concentration", 0)),
        }
        if result["concentration"] < 0:
            raise DomainError("invalid_concentration", "浓度不能为负数")
        current.setdefault("sample_results", []).append(result)
        current.setdefault("zone_state", {})[result["zone_id"]] = ZONE_CLEARED
        return "sampled", current, {"sample_result": result}

    if action == "restore":
        _need_status(item, {"sampled"})
        if not payload.get("all_zones_cleared"):
            raise DomainError("zones_not_cleared", "仍有区域未完成水质恢复", 409)
        limit = float(current.get("limit", 0))
        results = current.get("sample_results", [])
        if not results or any(float(result["concentration"]) > limit for result in results):
            raise DomainError("quality_not_met", "复检结果未全部达到限值", 409)
        affected_zones = current.get("zone_ids", [])
        if not zone_all_cleared(current, affected_zones):
            raise DomainError("zones_not_cleared", "下游区域仍在待复检，不能放行", 409)
        current["restoration"] = {"actor": actor, "note": payload.get("note", "")}
        for zone in affected_zones:
            current.setdefault("zone_state", {})[zone] = ZONE_CLEARED
        return "restored", current, {"restoration": current["restoration"]}

    if action == "cancel":
        _need_status(item, {"detected", "verified"})
        reason = _text(payload, "reason")
        current["cancellation"] = {"reason": reason, "actor": actor}
        return "cancelled", current, {"reason": reason}

    raise DomainError("unknown_action", "不支持的操作")


def batch_is_active(status):
    return status in BATCH_ACTIVE_STATUSES


def batch_is_terminal(status):
    return status in BATCH_TERMINAL_STATUSES


def batch_occupies(status):
    """占用机组与支路的状态（开工后到结案前）。"""
    return status in BATCH_OCCUPYING_STATUSES


def downstream_codes(branches_by_code, start_code):
    """返回 start_code 及其所有下游支路编码（沿 upstream 指针反向传递）。

    branches_by_code: {code: {"upstream": code_or_None, ...}}
    下游 = 以上游身份指向 start_code 的支路，递归传递。
    """
    affected = {start_code}
    frontier = [start_code]
    while frontier:
        current = frontier.pop()
        for code, info in branches_by_code.items():
            if code in affected:
                continue
            if info.get("upstream") == current:
                affected.add(code)
                frontier.append(code)
    return affected


def apply_source_invalidation(payload, affected_zones):
    """污染来源更新后：整批消毒失效，下游已恢复区域一起退回待复检。

    返回 (new_payload, new_status)。不删除历史记录，只把相关区域置为待复检，
    并清除放行结论——必须重新采样才能再次放行。
    """
    new_payload = dict(payload)
    zone_state = dict(new_payload.get("zone_state", {}))
    for zone in affected_zones:
        zone_state[zone] = ZONE_REINSPECTION
    new_payload["zone_state"] = zone_state
    new_payload.pop("restoration", None)
    status = REINSPECTION_PENDING
    return new_payload, status


def zone_all_cleared(payload, zone_ids):
    """所有受影响区域都已结束待复检（放行）。"""
    zone_state = payload.get("zone_state", {})
    return all(zone_state.get(zone, ZONE_CLEARED) == ZONE_CLEARED for zone in zone_ids)


def zone_cleared(payload, zone_id):
    return payload.get("zone_state", {}).get(zone_id, ZONE_CLEARED) == ZONE_CLEARED
