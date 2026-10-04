from . import domain, rules
from .domain import DomainError


class Service:
    def __init__(self, repository):
        self.repository = repository

    def _require_identity(self, actor, role):
        if not actor or not role:
            raise DomainError("identity_required", "需要用户身份和角色", 401)

    def create_item(self, payload, actor, role, region=None):
        self._require_identity(actor, role)
        if role not in rules.CREATE_ROLES:
            raise DomainError("forbidden", "当前角色不能创建此类业务记录", 403)
        normalized = domain.normalize_create(payload)
        stable_key = normalized.pop("_stable_key")
        return self.repository.create_item(
            rules.ENTITY_TYPE, stable_key, rules.INITIAL_STATUS, normalized, actor, role
        )

    def add_source(self, item_id, payload, actor, role, region=None):
        self._require_identity(actor, role)
        if role not in rules.SOURCE_ROLES:
            raise DomainError("forbidden", "当前角色不能提交来源记录", 403)
        item = self.repository.get_item(item_id)
        normalized = domain.normalize_source(payload)
        if region and rules.ENFORCE_REGION and role != "regulator" and normalized.get("region") and normalized["region"] != region:
            raise DomainError("region_mismatch", "来源记录不属于当前管辖区域", 403)
        result = self.repository.add_source(
            item_id,
            normalized.pop("source_type"),
            normalized.pop("external_id"),
            normalized,
            normalized.pop("observed_at"),
            actor,
            role,
        )
        return result

    def act(self, item_id, action, payload, actor, role, expected_version=None, region=None):
        self._require_identity(actor, role)
        item = self.repository.get_item(item_id)
        allowed = rules.ACTION_ROLES.get(action, set())
        if role not in allowed:
            raise DomainError("forbidden", "当前角色不能执行该操作", 403)
        if rules.ENFORCE_REGION and action in rules.REGION_SENSITIVE_ACTIONS and region and role != "regulator":
            if item["payload"].get("region") != region:
                raise DomainError("region_mismatch", "不能处理其他区域的记录", 403)
        if action in rules.ACTION_REQUIRES_VERSION and expected_version is None:
            raise DomainError("expected_version_required", "该操作需要 expected_version", 400)
        new_status, new_payload, event_payload = rules.apply_action(item, action, payload, actor, role)
        self.repository.apply_action(
            item_id, action, actor, role, new_status, new_payload, event_payload, expected_version
        )
        return self.get_item(item_id)

    # ---- 移动消毒机组 / 管网支路 / 消毒批次联动 ----

    def register_unit(self, payload, actor, role, region=None):
        self._require_identity(actor, role)
        if role not in rules.UNIT_REGISTER_ROLES:
            raise DomainError("forbidden", "当前角色不能登记移动消毒机组", 403)
        code = domain.require_text(payload, "code")
        name = str(payload.get("name", "") or "")
        return self.repository.register_unit(code, name)

    def register_branch(self, payload, actor, role, region=None):
        self._require_identity(actor, role)
        if role not in rules.BRANCH_REGISTER_ROLES:
            raise DomainError("forbidden", "当前角色不能登记管网支路", 403)
        code = domain.require_text(payload, "code")
        name = str(payload.get("name", "") or "")
        zone_ids = payload.get("zone_ids", [])
        if not isinstance(zone_ids, list) or not zone_ids or any(not isinstance(z, str) or not z.strip() for z in zone_ids):
            raise DomainError("zones_required", "支路必须覆盖至少一个区域")
        zone_ids = [z.strip() for z in zone_ids]
        downstream = payload.get("downstream_codes", [])
        if not isinstance(downstream, list) or any(not isinstance(c, str) or not c.strip() for c in downstream):
            raise DomainError("invalid_downstream", "下游支路编号必须是字符串列表")
        branch_region = payload.get("region") or region
        if branch_region is not None and role != "regulator" and region and branch_region != region:
            raise DomainError("region_mismatch", "不能跨管辖区域登记支路", 403)
        return self.repository.register_branch(
            code, name, zone_ids, branch_region, [c.strip() for c in downstream], actor, role
        )

    def start_batch(self, payload, actor, role, region=None):
        self._require_identity(actor, role)
        if role not in rules.BATCH_START_ROLES:
            raise DomainError("forbidden", "当前角色不能开工消毒批次", 403)
        unit_code = domain.require_text(payload, "unit_code")
        branch_code = domain.require_text(payload, "branch_code")
        item_id = payload.get("item_id")
        if item_id is not None:
            item_id = int(item_id)
            self.repository.get_item(item_id)
        branch = self.repository.get_branch(branch_code)
        # 监管可跨支路放行；现场角色越过支路边界返回权限拒绝
        if role not in rules.CROSS_BRANCH_ROLES and region and branch.get("region") and branch["region"] != region:
            raise DomainError("branch_boundary_denied", "现场角色不能越过支路边界作业", 403)
        return self.repository.start_batch(unit_code, branch_code, item_id, actor, role)

    def submit_receipt(self, batch_id, payload, actor, role, region=None):
        self._require_identity(actor, role)
        if role not in rules.BATCH_RECEIPT_ROLES:
            raise DomainError("forbidden", "当前角色不能提交消毒回执", 403)
        batch = self.repository.get_batch(batch_id)
        if role not in rules.CROSS_BRANCH_ROLES and region:
            branch = self.repository.get_branch(batch["branch_code"])
            if branch.get("region") and branch["region"] != region:
                raise DomainError("branch_boundary_denied", "现场角色不能越过支路边界提交回执", 403)
        key = domain.require_text(payload, "receipt_id")
        success = bool(payload.get("success", True))
        dose = domain.number(payload, "dose", 0) if success and "dose" in payload else None
        note = str(payload.get("note", "") or "")
        return self.repository.submit_receipt(batch_id, key, success, dose, note, actor, role)

    def record_retest(self, item_id, payload, actor, role, region=None):
        self._require_identity(actor, role)
        if role not in rules.RETEST_ROLES:
            raise DomainError("forbidden", "当前角色不能登记复检结果", 403)
        zone_id = domain.require_text(payload, "zone_id")
        concentration = domain.number(payload, "concentration", 0)
        item = self.repository.get_item(item_id)
        limit = float(item["payload"].get("limit", 0.000001))
        return self.repository.record_retest(item_id, zone_id, concentration, limit, actor, role)

    def get_batch(self, batch_id):
        return self.repository.get_batch(batch_id)

    def list_batches(self, unit_code=None, status=None):
        return self.repository.list_batches(unit_code, status)

    def list_branches(self):
        return self.repository.list_branches()

    def get_branch(self, code):
        return self.repository.get_branch(code)

    def get_unit(self, code):
        return self.repository.get_unit(code)

    def zone_states(self, item_id=None):
        return self.repository.zone_states(item_id)

    def get_item(self, item_id):
        item = self.repository.get_item(item_id)
        item["sources"] = self.repository.list_sources(item_id)
        item["audit"] = self.repository.audit_trail(item_id)
        item["assessment"] = rules.assess(item["payload"])
        item["zone_states"] = self.repository.zone_states(item_id)
        return item

    def list_items(self, status=None):
        return self.repository.list_items(status)

    def state(self):
        return self.repository.state_summary()
