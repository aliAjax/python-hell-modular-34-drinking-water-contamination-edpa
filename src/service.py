from . import domain, rules
from .domain import DomainError


class Service:
    def __init__(self, repository):
        self.repository = repository

    def _identity(self, actor, role):
        if not actor or not role:
            raise DomainError("identity_required", "需要用户身份和角色", 401)

    def _role_allowed(self, role, allowed, action_label):
        if role not in allowed:
            raise DomainError("forbidden", "当前角色不能执行该操作", 403)

    def _check_batch_scope(self, batch, actor, role, region):
        """现场角色越过支路边界返回权限拒绝；监管/协调可以跨支路放行。"""
        if role in rules.CROSS_BRANCH_ROLES:
            return
        if role in rules.FIELD_SCOPED_ROLES and region and batch.get("branch_region"):
            if batch["branch_region"] != region:
                raise DomainError("branch_scope_denied", "现场角色不能越过支路边界作业", 403)

    def create_item(self, payload, actor, role, region=None):
        if not actor or not role:
            raise DomainError("identity_required", "需要用户身份和角色", 401)
        if role not in rules.CREATE_ROLES:
            raise DomainError("forbidden", "当前角色不能创建此类业务记录", 403)
        normalized = domain.normalize_create(payload)
        stable_key = normalized.pop("_stable_key")
        return self.repository.create_item(
            rules.ENTITY_TYPE, stable_key, rules.INITIAL_STATUS, normalized, actor, role
        )

    def add_source(self, item_id, payload, actor, role, region=None):
        if not actor or not role:
            raise DomainError("identity_required", "需要用户身份和角色", 401)
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
        # 覆盖区域污染来源更新 → 整批消毒失效，下游已恢复区域一起退回待复检
        zone_id = normalized.get("zone_id")
        if zone_id:
            self.repository.invalidate_for_source(item_id, zone_id, "覆盖区域污染来源更新", actor, role)
        return result

    def act(self, item_id, action, payload, actor, role, expected_version=None, region=None):
        if not actor or not role:
            raise DomainError("identity_required", "需要用户身份和角色", 401)
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

    def get_item(self, item_id):
        item = self.repository.get_item(item_id)
        item["sources"] = self.repository.list_sources(item_id)
        item["audit"] = self.repository.audit_trail(item_id)
        item["assessment"] = rules.assess(item["payload"])
        return item

    def list_items(self, status=None):
        return self.repository.list_items(status)

    def state(self):
        return self.repository.state_summary()

    # ============================= 消毒批次联动账 =============================

    def submit_batch(self, item_id, payload, actor, role, region=None):
        self._identity(actor, role)
        self._role_allowed(role, rules.BATCH_ROLES["submit_batch"], "开工")
        normalized = domain.normalize_batch(payload)
        # 先校验支路边界，越权则不创建批次、不占用机组/支路
        branch_region = self.repository.branch_region_for(item_id, normalized["branch_code"])
        if role in rules.FIELD_SCOPED_ROLES and region and branch_region and branch_region != region:
            raise DomainError("branch_scope_denied", "现场角色不能越过支路边界作业", 403)
        batch = self.repository.create_batch(
            item_id, normalized["branch_code"], normalized["unit_code"],
            normalized["planned_dosage"], normalized["note"], actor, role,
        )
        return batch

    def submit_receipt(self, batch_id, payload, actor, role, region=None):
        self._identity(actor, role)
        self._role_allowed(role, rules.BATCH_ROLES["submit_receipt"], "回执")
        normalized = domain.normalize_receipt(payload)
        batch = self.repository.get_batch(batch_id)
        self._check_batch_scope(batch, actor, role, region)
        return self.repository.submit_receipt(
            batch_id, normalized["actual_dosage"], normalized["success"], normalized["note"], actor, role
        )

    def retry_batch(self, batch_id, payload, actor, role, region=None):
        self._identity(actor, role)
        self._role_allowed(role, rules.BATCH_ROLES["retry_batch"], "重试")
        actual_dosage = None
        if payload.get("actual_dosage") is not None:
            actual_dosage = domain.number(payload, "actual_dosage", 0)
        note = payload.get("note", "") if isinstance(payload.get("note", ""), str) else ""
        batch = self.repository.get_batch(batch_id)
        self._check_batch_scope(batch, actor, role, region)
        return self.repository.retry_batch(batch_id, actual_dosage, note, actor, role)

    def release_batch(self, batch_id, payload, actor, role, region=None):
        self._identity(actor, role)
        self._role_allowed(role, rules.BATCH_ROLES["release_batch"], "放行")
        normalized = domain.normalize_release(payload)
        batch = self.repository.get_batch(batch_id)
        self._check_batch_scope(batch, actor, role, region)
        return self.repository.release_batch(batch_id, normalized["note"], actor, role)

    def list_batches(self, status=None, item_id=None, actor=None, role=None, region=None):
        if actor or role:
            self._identity(actor, role)
            self._role_allowed(role, rules.BATCH_ROLES["list_batches"], "查询批次")
        return self.repository.list_batches(status=status, item_id=item_id)

    def get_batch(self, batch_id):
        return self.repository.get_batch(batch_id)

    def list_branches(self):
        return self.repository.list_branches()

    def list_units(self):
        return self.repository.list_units()

    def set_branch_upstream(self, branch_code, upstream_code, actor, role):
        self._identity(actor, role)
        self._role_allowed(role, rules.BATCH_ROLES["set_branch_upstream"], "配置支路拓扑")
        return self.repository.set_branch_upstream(branch_code, upstream_code)
