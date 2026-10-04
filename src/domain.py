from datetime import datetime


class DomainError(Exception):
    def __init__(self, code, message, status=400):
        super().__init__(message)
        self.code = code
        self.status = status


class ConflictError(DomainError):
    def __init__(self, code, message):
        super().__init__(code, message, 409)


class NotFoundError(DomainError):
    def __init__(self, code, message):
        super().__init__(code, message, 404)


def require_text(payload, name):
    value = payload.get(name)
    if not isinstance(value, str) or not value.strip():
        raise DomainError("field_required", "%s 不能为空" % name)
    return value.strip()


def number(payload, name, minimum=None):
    value = payload.get(name)
    if isinstance(value, bool):
        raise DomainError("invalid_number", "%s 必须是数字" % name)
    try:
        value = float(value)
    except (TypeError, ValueError):
        raise DomainError("invalid_number", "%s 必须是数字" % name)
    if minimum is not None and value < minimum:
        raise DomainError("invalid_number", "%s 不能小于 %s" % (name, minimum))
    return value


def parse_timestamp(payload, name):
    value = require_text(payload, name)
    try:
        datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        raise DomainError("invalid_timestamp", "%s 必须是 ISO 时间" % name)
    return value


def normalize_create(payload):
    source_id = require_text(payload, "source_id")
    contaminant = require_text(payload, "contaminant")
    detected_at = parse_timestamp(payload, "detected_at")
    concentration = number(payload, "concentration", 0)
    limit = number(payload, "limit", 0.000001)
    zones = payload.get("zone_ids", [])
    if not isinstance(zones, list) or not zones:
        raise DomainError("zones_required", "至少需要一个受影响区域")
    if any(not isinstance(zone, str) or not zone.strip() for zone in zones):
        raise DomainError("invalid_zones", "区域编号必须是字符串列表")
    population = int(payload.get("population", 0) or 0)
    if population < 0:
        raise DomainError("invalid_population", "受影响人数不能为负数")
    region = payload.get("region")
    if region is not None and (not isinstance(region, str) or not region.strip()):
        raise DomainError("invalid_region", "区域编号必须是字符串")
    stable_key = "%s|%s|%s" % (source_id, contaminant, detected_at)
    return {
        "source_id": source_id,
        "contaminant": contaminant,
        "detected_at": detected_at,
        "concentration": concentration,
        "limit": limit,
        "zone_ids": [zone.strip() for zone in zones],
        "population": population,
        "complaints": int(payload.get("complaints", 0) or 0),
        "notifications": [],
        "response_actions": [],
        "sample_results": [],
        "region": region.strip() if isinstance(region, str) else None,
        "_stable_key": stable_key,
    }


def normalize_source(payload):
    source_type = require_text(payload, "source_type")
    external_id = require_text(payload, "external_id")
    observed_at = parse_timestamp(payload, "observed_at")
    result = {
        "source_type": source_type,
        "external_id": external_id,
        "observed_at": observed_at,
        "concentration": number(payload, "concentration", 0) if "concentration" in payload else None,
        "zone_id": payload.get("zone_id"),
        "note": payload.get("note", ""),
    }
    return result


DEFAULT_UNIT_CODE = "UNIT-1"


def normalize_batch(payload):
    """开工批次：绑定污染事件、支路与机组，带回计划加药量。"""
    branch_code = require_text(payload, "zone_id")
    unit_code = payload.get("unit_id") or DEFAULT_UNIT_CODE
    if not isinstance(unit_code, str) or not unit_code.strip():
        raise DomainError("invalid_unit", "机组编号必须是字符串")
    planned_dosage = number(payload, "planned_dosage", 0)
    note = payload.get("note", "")
    return {
        "branch_code": branch_code.strip(),
        "unit_code": unit_code.strip(),
        "planned_dosage": planned_dosage,
        "note": note if isinstance(note, str) else "",
    }


def normalize_receipt(payload):
    """回执：带回实际加药量与成败。"""
    actual_dosage = number(payload, "actual_dosage", 0)
    if actual_dosage <= 0:
        raise DomainError("invalid_dosage", "实际加药量必须大于零")
    if "success" not in payload:
        raise DomainError("receipt_status_required", "回执必须包含 success 字段", 400)
    success = bool(payload.get("success"))
    note = payload.get("note", "")
    return {
        "actual_dosage": actual_dosage,
        "success": success,
        "note": note if isinstance(note, str) else "",
    }


def normalize_release(payload):
    """放行：监管/协调对批次强制结案。"""
    note = payload.get("note", "")
    return {"note": note if isinstance(note, str) else ""}
