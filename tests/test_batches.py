import os
import sys
import tempfile
import unittest

sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

from src.repository import Repository
from src.service import Service
from src.domain import DomainError, ConflictError
from src import rules


class BatchLedgerTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.NamedTemporaryFile(suffix=".db", delete=False)
        self.tmp.close()
        self.repo = Repository(self.tmp.name)
        self.repo.initialize()
        self.repo.migrate()
        self.service = Service(self.repo)

    def tearDown(self):
        os.unlink(self.tmp.name)

    def _event(self, zones, region=None, source_id="SRC-1"):
        payload = {
            "source_id": source_id,
            "contaminant": "nitrate",
            "detected_at": "2026-10-01T06:00:00+00:00",
            "concentration": 20,
            "limit": 10,
            "zone_ids": list(zones),
            "population": 5000,
        }
        if region:
            payload["region"] = region
        item = self.service.create_item(payload, "analyst-1", "analyst", region)
        item = self.service.act(item["id"], "verify", {"sample_count": 1}, "analyst-1", "analyst", item["version"], region)
        item = self.service.act(item["id"], "advise", {"notice_id": "N-1", "kind": "boil", "message": "x"}, "disp-1", "dispatcher", item["version"], region)
        return item

    def _restore(self, item):
        item = self.service.get_item(item["id"])
        for zone in item["payload"]["zone_ids"]:
            item = self.service.act(item["id"], "sample", {"sample_id": "S-%s" % zone, "zone_id": zone, "concentration": 1}, "lab-1", "lab", item["version"])
        item = self.service.act(item["id"], "restore", {"all_zones_cleared": True}, "coord-1", "coordinator", item["version"])
        return item

    def test_batch_start_occupies_unit_and_branch_receipt_records_dosage(self):
        item = self._event(["Z-1"])
        batch = self.service.submit_batch(item["id"], {"zone_id": "Z-1", "planned_dosage": 5.0}, "field-1", "field_operator")
        self.assertEqual(batch["status"], rules.BATCH_IN_PROGRESS)
        self.assertIsNone(batch["queue_number"])
        # 机组被占用
        unit = [u for u in self.service.list_units() if u["unit_code"] == "UNIT-1"][0]
        self.assertEqual(unit["status"], "occupied")
        # 回执带回实际加药量
        batch = self.service.submit_receipt(batch["id"], {"actual_dosage": 4.5, "success": True}, "field-1", "field_operator")
        self.assertEqual(batch["status"], rules.BATCH_COMPLETED)
        self.assertEqual(batch["actual_dosage"], 4.5)
        # 机组释放
        unit = [u for u in self.service.list_units() if u["unit_code"] == "UNIT-1"][0]
        self.assertEqual(unit["status"], "available")
        # 污染事件推进到 disinfected
        self.assertEqual(self.service.get_item(item["id"])["status"], "disinfected")

    def test_concurrent_unit_submit_first_claims_second_gets_queue_number(self):
        item = self._event(["Z-1", "Z-2"])
        b1 = self.service.submit_batch(item["id"], {"zone_id": "Z-1", "planned_dosage": 5.0}, "field-1", "field_operator")
        b2 = self.service.submit_batch(item["id"], {"zone_id": "Z-2", "planned_dosage": 3.0}, "field-2", "field_operator")
        self.assertEqual(b1["status"], rules.BATCH_IN_PROGRESS)
        self.assertIsNone(b1["queue_number"])
        self.assertEqual(b2["status"], rules.BATCH_QUEUED)
        self.assertEqual(b2["queue_number"], 1)
        # 再来一个排队号递增
        b3 = self.service.submit_batch(item["id"], {"zone_id": "Z-3", "planned_dosage": 2.0}, "field-3", "field_operator")
        self.assertEqual(b3["queue_number"], 2)
        # b1 回执成功 → b2 按顺序自动派发，b3 仍排队
        self.service.submit_receipt(b1["id"], {"actual_dosage": 4.5, "success": True}, "field-1", "field_operator")
        b2 = self.service.get_batch(b2["id"])
        b3 = self.service.get_batch(b3["id"])
        self.assertEqual(b2["status"], rules.BATCH_IN_PROGRESS)
        self.assertIsNone(b2["queue_number"])
        self.assertEqual(b3["status"], rules.BATCH_QUEUED)
        self.assertEqual(b3["queue_number"], 2)

    def test_branch_occupied_rejected(self):
        item = self._event(["Z-1", "Z-2"])
        self.service.submit_batch(item["id"], {"zone_id": "Z-1", "planned_dosage": 5.0}, "field-1", "field_operator")
        with self.assertRaises(ConflictError) as context:
            self.service.submit_batch(item["id"], {"zone_id": "Z-1", "planned_dosage": 2.0}, "field-2", "field_operator")
        self.assertEqual(context.exception.code, "branch_occupied")
        # Z-2 仍可排队（支路空闲）
        b2 = self.service.submit_batch(item["id"], {"zone_id": "Z-2", "planned_dosage": 3.0}, "field-2", "field_operator")
        self.assertEqual(b2["status"], rules.BATCH_QUEUED)

    def test_receipt_failure_keeps_batch_retry_in_order_no_redose(self):
        item = self._event(["Z-1", "Z-2"])
        b1 = self.service.submit_batch(item["id"], {"zone_id": "Z-1", "planned_dosage": 5.0}, "field-1", "field_operator")
        b2 = self.service.submit_batch(item["id"], {"zone_id": "Z-2", "planned_dosage": 3.0}, "field-2", "field_operator")
        # b1 回执失败
        b1 = self.service.submit_receipt(b1["id"], {"actual_dosage": 4.0, "success": False, "note": "dose fault"}, "field-1", "field_operator")
        self.assertEqual(b1["status"], rules.BATCH_RECEIPT_FAILED)
        # 未完成批次保留，机组仍占用，b2 不能插队
        b2 = self.service.get_batch(b2["id"])
        self.assertEqual(b2["status"], rules.BATCH_QUEUED)
        unit = [u for u in self.service.list_units() if u["unit_code"] == "UNIT-1"][0]
        self.assertEqual(unit["status"], "occupied")
        # 按原顺序重试 b1，不重复加药
        b1 = self.service.retry_batch(b1["id"], {"note": "retry ok"}, "field-1", "field_operator")
        self.assertEqual(b1["status"], rules.BATCH_COMPLETED)
        self.assertEqual(b1["retry_count"], 1)
        self.assertEqual(b1["dosed"], 1)
        self.assertEqual(b1["actual_dosage"], 4.0)
        # b1 只在开工时加过一次药，重试不再加药
        audit = self.service.repository.audit_trail(item["id"])
        dose_events = [e for e in audit if e["event_type"] == "batch_dosed" and e["payload"].get("batch_id") == b1["id"]]
        self.assertEqual(len(dose_events), 1)
        # b2 在 b1 之后派发
        b2 = self.service.get_batch(b2["id"])
        self.assertEqual(b2["status"], rules.BATCH_IN_PROGRESS)

    def test_source_update_invalidates_whole_batch_and_downstream_reverts(self):
        # 拓扑：Z-1（上游）→ Z-2 → Z-3（下游）
        self.service.set_branch_upstream("Z-2", "Z-1", "coord-1", "coordinator")
        self.service.set_branch_upstream("Z-3", "Z-2", "coord-1", "coordinator")
        item = self._event(["Z-1", "Z-2", "Z-3"])
        b1 = self.service.submit_batch(item["id"], {"zone_id": "Z-1", "planned_dosage": 5.0}, "field-1", "field_operator")
        b2 = self.service.submit_batch(item["id"], {"zone_id": "Z-2", "planned_dosage": 3.0}, "field-2", "field_operator")
        self.service.submit_receipt(b1["id"], {"actual_dosage": 4.5, "success": True}, "field-1", "field_operator")
        self.service.submit_receipt(b2["id"], {"actual_dosage": 2.5, "success": True}, "field-2", "field_operator")
        item = self._restore(item)
        self.assertEqual(item["status"], "restored")
        # 上游 Z-1 污染来源更新
        self.service.add_source(item["id"], {"source_type": "grab", "external_id": "GRAB-1", "observed_at": "2026-10-02T00:00:00+00:00", "concentration": 50, "zone_id": "Z-1"}, "analyst-1", "analyst")
        item = self.service.get_item(item["id"])
        # 整批消毒失效（含已完成批次）
        for batch in self.service.list_batches(item_id=item["id"]):
            self.assertEqual(batch["status"], rules.BATCH_INVALIDATED)
            self.assertEqual(batch["invalidated"], 1)
        # 下游已恢复区域一起退回待复检（不是只回退 Z-1）
        self.assertEqual(item["status"], rules.REINSPECTION_PENDING)
        self.assertNotIn("restoration", item["payload"])
        state = item["payload"]["zone_state"]
        self.assertEqual(state["Z-1"], rules.ZONE_REINSPECTION)
        self.assertEqual(state["Z-2"], rules.ZONE_REINSPECTION)
        self.assertEqual(state["Z-3"], rules.ZONE_REINSPECTION)
        # 机组释放
        unit = [u for u in self.service.list_units() if u["unit_code"] == "UNIT-1"][0]
        self.assertEqual(unit["status"], "available")

    def test_source_update_only_new_zone_when_no_downstream(self):
        # 无拓扑（独立支路）：只影响该支路
        item = self._event(["Z-1", "Z-2"])
        b1 = self.service.submit_batch(item["id"], {"zone_id": "Z-1", "planned_dosage": 5.0}, "field-1", "field_operator")
        self.service.submit_receipt(b1["id"], {"actual_dosage": 4.5, "success": True}, "field-1", "field_operator")
        item = self._restore(item)
        self.service.add_source(item["id"], {"source_type": "grab", "external_id": "GRAB-2", "observed_at": "2026-10-02T00:00:00+00:00", "concentration": 50, "zone_id": "Z-1"}, "analyst-1", "analyst")
        item = self.service.get_item(item["id"])
        self.assertEqual(item["status"], rules.REINSPECTION_PENDING)
        self.assertEqual(item["payload"]["zone_state"]["Z-1"], rules.ZONE_REINSPECTION)
        # Z-2 未受影响（独立支路，无上下游关系）
        self.assertEqual(item["payload"]["zone_state"].get("Z-2", rules.ZONE_CLEARED), rules.ZONE_CLEARED)

    def test_migration_fills_branches_from_zones_and_keeps_history(self):
        # 旧数据：有事件、无支路
        item = self._event(["Z-1", "Z-2", "Z-3"])
        # 清空已迁移的支路，模拟升级前
        conn = self.repo.connect()
        try:
            conn.execute("DELETE FROM pipe_branches")
            conn.commit()
        finally:
            conn.close()
        self.assertEqual(len(self.service.list_branches()), 0)
        # 执行升级
        self.repo.migrate()
        codes = sorted(b["branch_code"] for b in self.service.list_branches())
        self.assertEqual(codes, ["Z-1", "Z-2", "Z-3"])
        # 默认机组补齐
        self.assertTrue(any(u["unit_code"] == "UNIT-1" for u in self.service.list_units()))
        # 历史记录完整
        history = self.service.get_item(item["id"])
        self.assertEqual(history["status"], "advisory")
        self.assertGreaterEqual(len(history["audit"]), 2)
        # 幂等
        self.repo.migrate()
        self.assertEqual(len(self.service.list_branches()), 3)

    def test_field_role_cross_branch_denied_regulator_allowed(self):
        item = self._event(["Z-1"], region="R1")
        # 现场角色越区 → 403，且不创建批次
        with self.assertRaises(DomainError) as context:
            self.service.submit_batch(item["id"], {"zone_id": "Z-1", "planned_dosage": 5.0}, "field-2", "field_operator", "R2")
        self.assertEqual(context.exception.code, "branch_scope_denied")
        self.assertEqual(context.exception.status, 403)
        self.assertEqual(len(self.service.list_batches()), 0)
        # 本区现场角色可开工
        batch = self.service.submit_batch(item["id"], {"zone_id": "Z-1", "planned_dosage": 5.0}, "field-1", "field_operator", "R1")
        self.assertEqual(batch["status"], rules.BATCH_IN_PROGRESS)
        # 监管跨支路放行
        batch = self.service.release_batch(batch["id"], {"note": "regulator cross-branch release"}, "reg-1", "regulator", "R2")
        self.assertEqual(batch["status"], rules.BATCH_COMPLETED)

    def test_queued_batch_does_not_occupy_branch(self):
        item = self._event(["Z-1", "Z-2"])
        b1 = self.service.submit_batch(item["id"], {"zone_id": "Z-1", "planned_dosage": 5.0}, "field-1", "field_operator")
        b2 = self.service.submit_batch(item["id"], {"zone_id": "Z-2", "planned_dosage": 3.0}, "field-2", "field_operator")
        # b2 排队但不占支路：Z-2 上再来一个批次应能排队（而非 branch_occupied）
        b3 = self.service.submit_batch(item["id"], {"zone_id": "Z-2", "planned_dosage": 1.0}, "field-3", "field_operator")
        self.assertEqual(b3["status"], rules.BATCH_QUEUED)
        self.assertEqual(b3["queue_number"], 2)


if __name__ == "__main__":
    unittest.main()
