import json
import os
import sqlite3
import sys
import tempfile
import threading
import unittest
from http.server import ThreadingHTTPServer
from urllib import request as urlrequest

sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

from src.repository import (
    Repository,
    BATCH_QUEUED,
    BATCH_ACTIVE,
    BATCH_COMPLETED,
    BATCH_INVALIDATED,
    ZONE_AFFECTED,
    ZONE_PENDING_RETEST,
    ZONE_RESTORED,
)
from src.service import Service
from src.domain import DomainError


def build_legacy_db(path):
    """模拟旧版（无支路/机组/批次表）数据库。"""
    conn = sqlite3.connect(path)
    conn.executescript(
        """
        CREATE TABLE items (id INTEGER PRIMARY KEY AUTOINCREMENT, entity_type TEXT, stable_key TEXT,
            status TEXT, version INTEGER, payload TEXT, created_by TEXT, created_role TEXT, created_at TEXT, updated_at TEXT);
        CREATE TABLE sources (id INTEGER PRIMARY KEY AUTOINCREMENT, item_id INTEGER, source_type TEXT,
            external_id TEXT, payload TEXT, observed_at TEXT, created_at TEXT);
        CREATE TABLE actions (id INTEGER PRIMARY KEY AUTOINCREMENT, item_id INTEGER, action TEXT,
            actor TEXT, role TEXT, payload TEXT, created_at TEXT);
        CREATE TABLE audit_events (id INTEGER PRIMARY KEY AUTOINCREMENT, item_id INTEGER, event_type TEXT,
            actor TEXT, role TEXT, payload TEXT, previous_hash TEXT, event_hash TEXT, created_at TEXT);
        """
    )
    payload = {
        "zone_ids": ["Z1", "Z2"],
        "limit": 10,
        "notifications": [],
        "response_actions": [],
        "sample_results": [],
    }
    conn.execute(
        "INSERT INTO items(entity_type,stable_key,status,version,payload,created_by,created_role,created_at,updated_at)"
        " VALUES('water_contamination','k','restored',9,?,'a','analyst','t','t')",
        (json.dumps(payload, ensure_ascii=False),),
    )
    conn.execute(
        "INSERT INTO actions(item_id,action,actor,role,payload,created_at) VALUES(1,'disinfect','f','f','{}','t')"
    )
    conn.commit()
    conn.close()


class LinkedLedgerTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.NamedTemporaryFile(suffix=".db", delete=False)
        self.tmp.close()
        self.repo = Repository(self.tmp.name)
        self.repo.initialize()
        self.service = Service(self.repo)
        self.item = self.service.create_item(
            {
                "source_id": "SRC-1",
                "contaminant": "bacteria",
                "detected_at": "2026-10-01T00:00:00+00:00",
                "concentration": 30,
                "limit": 10,
                "zone_ids": ["Z1", "Z2"],
                "population": 100,
            },
            "analyst-1",
            "analyst",
        )
        self.item_id = self.item["id"]
        self.service.register_unit({"code": "U1", "name": "移动消毒1号"}, "d", "dispatcher")
        # BR-A 为上游支路，BR-B 为下游支路
        self.service.register_branch(
            {"code": "BR-B", "zone_ids": ["Z2"], "region": "R1"}, "d", "dispatcher", region="R1"
        )
        self.service.register_branch(
            {"code": "BR-A", "zone_ids": ["Z1"], "downstream_codes": ["BR-B"], "region": "R1"},
            "d",
            "dispatcher",
            region="R1",
        )

    def tearDown(self):
        os.unlink(self.tmp.name)

    def _finish_batches(self, first_branch="BR-A", second_branch="BR-B"):
        b1 = self.service.start_batch(
            {"unit_code": "U1", "branch_code": first_branch, "item_id": self.item_id},
            "f1",
            "field_operator",
            region="R1",
        )
        b2 = self.service.start_batch(
            {"unit_code": "U1", "branch_code": second_branch, "item_id": self.item_id},
            "f2",
            "field_operator",
            region="R1",
        )
        self.assertTrue(b1["acquired"])
        self.service.submit_receipt(
            b1["id"], {"receipt_id": "RC-1", "success": True, "dose": 12.5}, "f1", "field_operator", region="R1"
        )
        self.service.submit_receipt(
            b2["id"], {"receipt_id": "RC-2", "success": True, "dose": 7.0}, "f2", "field_operator", region="R1"
        )
        return b1, b2

    def test_first_acquires_later_gets_queue_number_and_auto_promotes(self):
        b1 = self.service.start_batch(
            {"unit_code": "U1", "branch_code": "BR-A", "item_id": self.item_id}, "f1", "field_operator", region="R1"
        )
        b2 = self.service.start_batch(
            {"unit_code": "U1", "branch_code": "BR-B", "item_id": self.item_id}, "f2", "field_operator", region="R1"
        )
        self.assertEqual(b1["status"], BATCH_ACTIVE)
        self.assertTrue(b1["acquired"])
        self.assertEqual(b2["status"], BATCH_QUEUED)
        self.assertFalse(b2["acquired"])
        self.assertEqual(b2["queue_no"], 1)
        # 值班员视角：机组与支路均显示被占用
        self.assertEqual(self.service.get_unit("U1")["active_batch_id"], b1["id"])
        self.assertEqual(self.service.get_unit("U1")["queue"], [b2["id"]])
        self.assertEqual(self.service.get_branch("BR-A")["occupied_by_batch"], b1["id"])
        # 上一批回执带回实际加药量后，排队批次自动领批
        receipt = self.service.submit_receipt(
            b1["id"], {"receipt_id": "RC-1", "success": True, "dose": 12.5}, "f1", "field_operator", region="R1"
        )
        self.assertEqual(receipt["dose"], 12.5)
        self.assertEqual(self.service.get_batch(b2["id"])["status"], BATCH_ACTIVE)
        self.assertEqual(self.service.get_unit("U1")["active_batch_id"], b2["id"])
        self.assertEqual(self.service.get_unit("U1")["queue"], [])

    def test_branch_occupied_by_other_unit_also_queues(self):
        self.service.register_unit({"code": "U2"}, "d", "dispatcher")
        b1 = self.service.start_batch(
            {"unit_code": "U1", "branch_code": "BR-A", "item_id": self.item_id}, "f1", "field_operator", region="R1"
        )
        # U2 想用同一支路：机组空闲但支路被占 -> 排队
        b2 = self.service.start_batch(
            {"unit_code": "U2", "branch_code": "BR-A", "item_id": self.item_id}, "f2", "field_operator", region="R1"
        )
        self.assertFalse(b2["acquired"])
        self.assertEqual(b2["status"], BATCH_QUEUED)
        self.service.submit_receipt(
            b1["id"], {"receipt_id": "RC-1", "success": True, "dose": 1.0}, "f1", "field_operator", region="R1"
        )
        self.assertEqual(self.service.get_batch(b2["id"])["status"], BATCH_ACTIVE)

    def test_concurrent_submissions_first_wins(self):
        results = []

        def submit(branch):
            service = Service(Repository(self.tmp.name))
            results.append(
                service.start_batch(
                    {"unit_code": "U1", "branch_code": branch, "item_id": self.item_id},
                    "f-%s" % branch,
                    "field_operator",
                    region="R1",
                )
            )

        t1 = threading.Thread(target=submit, args=("BR-A",))
        t2 = threading.Thread(target=submit, args=("BR-B",))
        t1.start(); t2.start(); t1.join(); t2.join()
        acquired = [r for r in results if r["acquired"]]
        queued = [r for r in results if not r["acquired"]]
        self.assertEqual(len(acquired), 1)
        self.assertEqual(len(queued), 1)
        self.assertEqual(queued[0]["queue_no"], 1)

    def test_receipt_failure_keeps_batch_and_retry_does_not_double_dose(self):
        b1, b2 = self._finish_batches()
        # 再开一批，回执失败：批次保留在 active，不记加药量
        self.service.register_branch(
            {"code": "BR-C", "zone_ids": ["Z3"], "region": "R1"}, "d", "dispatcher", region="R1"
        )
        b3 = self.service.start_batch(
            {"unit_code": "U1", "branch_code": "BR-C", "item_id": self.item_id}, "f3", "field_operator", region="R1"
        )
        failed = self.service.submit_receipt(
            b3["id"], {"receipt_id": "RC-F", "success": False, "note": "通信超时"}, "f3", "field_operator", region="R1"
        )
        self.assertEqual(failed["status"], BATCH_ACTIVE)
        self.assertIsNone(failed["dose"])
        self.assertEqual(failed["receipt"]["attempts"], 1)
        # 失败期间仍占用机组
        other = self.service.start_batch(
            {"unit_code": "U1", "branch_code": "BR-A", "item_id": self.item_id}, "f4", "field_operator", region="R1"
        )
        self.assertFalse(other["acquired"])
        # 按原顺序重试成功：只记录一次实际加药量
        done = self.service.submit_receipt(
            b3["id"], {"receipt_id": "RC-F2", "success": True, "dose": 3.2}, "f3", "field_operator", region="R1"
        )
        self.assertEqual(done["status"], BATCH_COMPLETED)
        self.assertEqual(done["dose"], 3.2)
        self.assertEqual(self.service.get_batch(other["id"])["status"], BATCH_ACTIVE)

    def test_duplicate_receipt_is_idempotent(self):
        b1 = self.service.start_batch(
            {"unit_code": "U1", "branch_code": "BR-A", "item_id": self.item_id}, "f1", "field_operator", region="R1"
        )
        first = self.service.submit_receipt(
            b1["id"], {"receipt_id": "RC-1", "success": True, "dose": 12.5}, "f1", "field_operator", region="R1"
        )
        again = self.service.submit_receipt(
            b1["id"], {"receipt_id": "RC-1", "success": True, "dose": 999.0}, "f1", "field_operator", region="R1"
        )
        self.assertTrue(again["deduplicated"])
        self.assertEqual(again["dose"], 12.5)
        self.assertEqual(first["dose"], 12.5)

    def test_source_update_invalidates_batches_and_resets_downstream_restored_zones(self):
        b1, b2 = self._finish_batches()
        # 两区复检恢复
        self.service.record_retest(
            self.item_id, {"zone_id": "Z1", "concentration": 1}, "lab-1", "lab"
        )
        self.service.record_retest(
            self.item_id, {"zone_id": "Z2", "concentration": 1}, "lab-1", "lab"
        )
        states = {z["zone_id"]: z["status"] for z in self.repo.zone_states(self.item_id)}
        self.assertEqual(states, {"Z1": ZONE_RESTORED, "Z2": ZONE_RESTORED})
        # 上游 Z1 污染来源更新：整批立即失效，下游已恢复的 Z2 一并退回待复检
        result = self.service.add_source(
            self.item_id,
            {
                "source_type": "pipe_break",
                "external_id": "PB-9",
                "observed_at": "2026-10-02T00:00:00+00:00",
                "zone_id": "Z1",
            },
            "analyst-1",
            "analyst",
        )
        self.assertEqual(set(result["cascade"]["invalidated_batch_ids"]), {b1["id"], b2["id"]})
        self.assertEqual(self.service.get_batch(b1["id"])["status"], BATCH_INVALIDATED)
        self.assertEqual(self.service.get_batch(b2["id"])["status"], BATCH_INVALIDATED)
        states = {z["zone_id"]: z["status"] for z in self.repo.zone_states(self.item_id)}
        self.assertEqual(states["Z1"], ZONE_AFFECTED)
        self.assertEqual(states["Z2"], ZONE_PENDING_RETEST)
        # 失效批次拒绝再补回执
        with self.assertRaises(DomainError) as ctx:
            self.service.submit_receipt(
                b1["id"], {"receipt_id": "RC-X", "success": True, "dose": 1.0}, "f1", "field_operator", region="R1"
            )
        self.assertEqual(ctx.exception.code, "batch_invalidated")

    def test_unrelated_branch_is_not_reset(self):
        self.service.register_branch(
            {"code": "BR-C", "zone_ids": ["Z3"], "region": "R1"}, "d", "dispatcher", region="R1"
        )
        for branch, rc in (("BR-A", "RC-1"), ("BR-B", "RC-2"), ("BR-C", "RC-3")):
            batch = self.service.start_batch(
                {"unit_code": "U1", "branch_code": branch, "item_id": self.item_id},
                "f-%s" % branch,
                "field_operator",
                region="R1",
            )
            self.assertTrue(batch["acquired"])
            self.service.submit_receipt(
                batch["id"], {"receipt_id": rc, "success": True, "dose": 1.0},
                "f-%s" % branch, "field_operator", region="R1",
            )
        self.service.record_retest(self.item_id, {"zone_id": "Z3", "concentration": 1}, "lab-1", "lab")
        brc = next(b for b in self.service.list_batches() if b["branch_code"] == "BR-C")
        self.service.add_source(
            self.item_id,
            {"source_type": "pipe_break", "external_id": "PB-1", "observed_at": "2026-10-02T00:00:00+00:00", "zone_id": "Z1"},
            "analyst-1",
            "analyst",
        )
        # BR-C 不在 Z1 下游：批次保持完成、Z3 保持恢复
        self.assertEqual(self.service.get_batch(brc["id"])["status"], BATCH_COMPLETED)
        z3 = next(z for z in self.repo.zone_states(self.item_id) if z["zone_id"] == "Z3")
        self.assertEqual(z3["status"], ZONE_RESTORED)

    def test_field_operator_cross_boundary_denied_regulator_allowed(self):
        with self.assertRaises(DomainError) as ctx:
            self.service.start_batch(
                {"unit_code": "U1", "branch_code": "BR-A", "item_id": self.item_id},
                "fx",
                "field_operator",
                region="R2",
            )
        self.assertEqual(ctx.exception.code, "branch_boundary_denied")
        self.assertEqual(ctx.exception.status, 403)
        batch = self.service.start_batch(
            {"unit_code": "U1", "branch_code": "BR-A", "item_id": self.item_id},
            "reg-1",
            "regulator",
            region="R2",
        )
        self.assertTrue(batch["acquired"])

    def test_retest_failure_keeps_zone_affected(self):
        b1, _ = self._finish_batches()
        result = self.service.record_retest(
            self.item_id, {"zone_id": "Z1", "concentration": 50}, "lab-1", "lab"
        )
        self.assertEqual(result["status"], ZONE_AFFECTED)

    def test_audit_chain_records_batch_lifecycle(self):
        b1 = self.service.start_batch(
            {"unit_code": "U1", "branch_code": "BR-A", "item_id": self.item_id}, "f1", "field_operator", region="R1"
        )
        self.service.submit_receipt(
            b1["id"], {"receipt_id": "RC-1", "success": True, "dose": 1.0}, "f1", "field_operator", region="R1"
        )
        self.service.add_source(
            self.item_id,
            {"source_type": "pipe_break", "external_id": "PB-1", "observed_at": "2026-10-02T00:00:00+00:00", "zone_id": "Z1"},
            "analyst-1",
            "analyst",
        )
        item = self.service.get_item(self.item_id)
        types = [event["event_type"] for event in item["audit"]]
        for expected in ("batch_acquired", "batch_receipt", "batch_invalidated"):
            self.assertIn(expected, types)


class MigrationTest(unittest.TestCase):
    def test_legacy_data_gets_single_zone_branches(self):
        tmp = tempfile.NamedTemporaryFile(suffix=".db", delete=False)
        tmp.close()
        try:
            build_legacy_db(tmp.name)
            repo = Repository(tmp.name)
            repo.initialize()
            service = Service(repo)
            codes = {branch["code"] for branch in service.list_branches()}
            self.assertEqual(codes, {"BR-Z1", "BR-Z2"})
            zones = {branch["code"]: branch["zone_ids"] for branch in service.list_branches()}
            self.assertEqual(zones["BR-Z1"], ["Z1"])
            self.assertEqual(zones["BR-Z2"], ["Z2"])
            # 历史已消毒/已恢复记录以完成批次与恢复状态兼容落账
            batches = service.list_batches(status=BATCH_COMPLETED)
            self.assertEqual(len(batches), 2)
            self.assertTrue(all(b["unit_code"] == "U-LEGACY" for b in batches))
            states = {z["zone_id"]: z["status"] for z in repo.zone_states(1)}
            self.assertEqual(states, {"Z1": ZONE_RESTORED, "Z2": ZONE_RESTORED})
            # 升级可重复执行
            repo.initialize()
            self.assertEqual(len(service.list_batches(status=BATCH_COMPLETED)), 2)
        finally:
            os.unlink(tmp.name)


if __name__ == "__main__":
    unittest.main()
