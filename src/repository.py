import json
import sqlite3
from datetime import datetime, timezone

from . import rules
from .audit import audit_hash, canonical_json
from .domain import ConflictError, NotFoundError, DomainError, DEFAULT_UNIT_CODE


def now_iso():
    return datetime.now(timezone.utc).isoformat()


class Repository:
    def __init__(self, path):
        self.path = path

    def connect(self):
        conn = sqlite3.connect(self.path, timeout=30, isolation_level=None)
        conn.row_factory = sqlite3.Row
        return conn

    def initialize(self):
        conn = self.connect()
        try:
            conn.execute("PRAGMA journal_mode=WAL")
            conn.executescript(
                """
                CREATE TABLE IF NOT EXISTS items (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    entity_type TEXT NOT NULL,
                    stable_key TEXT NOT NULL,
                    status TEXT NOT NULL,
                    version INTEGER NOT NULL DEFAULT 1,
                    payload TEXT NOT NULL,
                    created_by TEXT NOT NULL,
                    created_role TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    UNIQUE(entity_type, stable_key)
                );
                CREATE TABLE IF NOT EXISTS sources (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    item_id INTEGER NOT NULL,
                    source_type TEXT NOT NULL,
                    external_id TEXT NOT NULL,
                    payload TEXT NOT NULL,
                    observed_at TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    UNIQUE(item_id, source_type, external_id),
                    FOREIGN KEY(item_id) REFERENCES items(id)
                );
                CREATE TABLE IF NOT EXISTS actions (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    item_id INTEGER NOT NULL,
                    action TEXT NOT NULL,
                    actor TEXT NOT NULL,
                    role TEXT NOT NULL,
                    payload TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    FOREIGN KEY(item_id) REFERENCES items(id)
                );
                CREATE TABLE IF NOT EXISTS audit_events (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    item_id INTEGER,
                    event_type TEXT NOT NULL,
                    actor TEXT,
                    role TEXT,
                    payload TEXT NOT NULL,
                    previous_hash TEXT NOT NULL,
                    event_hash TEXT NOT NULL,
                    created_at TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS pipe_branches (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    branch_code TEXT NOT NULL UNIQUE,
                    name TEXT NOT NULL DEFAULT '',
                    upstream_id INTEGER,
                    region TEXT,
                    status TEXT NOT NULL DEFAULT 'active',
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    FOREIGN KEY(upstream_id) REFERENCES pipe_branches(id)
                );
                CREATE TABLE IF NOT EXISTS disinfection_units (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    unit_code TEXT NOT NULL UNIQUE,
                    name TEXT NOT NULL DEFAULT '',
                    status TEXT NOT NULL DEFAULT 'available',
                    current_batch_id INTEGER,
                    region TEXT,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS disinfection_batches (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    batch_code TEXT NOT NULL UNIQUE,
                    item_id INTEGER NOT NULL,
                    branch_id INTEGER NOT NULL,
                    unit_id INTEGER NOT NULL,
                    queue_number INTEGER,
                    sequence INTEGER NOT NULL,
                    status TEXT NOT NULL,
                    planned_dosage REAL NOT NULL DEFAULT 0,
                    actual_dosage REAL,
                    receipt_status TEXT NOT NULL DEFAULT 'pending',
                    receipt_note TEXT NOT NULL DEFAULT '',
                    invalidated INTEGER NOT NULL DEFAULT 0,
                    invalidation_reason TEXT NOT NULL DEFAULT '',
                    retry_count INTEGER NOT NULL DEFAULT 0,
                    dosed INTEGER NOT NULL DEFAULT 0,
                    note TEXT NOT NULL DEFAULT '',
                    created_by TEXT NOT NULL,
                    created_role TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    started_at TEXT,
                    receipted_at TEXT,
                    completed_at TEXT,
                    updated_at TEXT NOT NULL,
                    FOREIGN KEY(item_id) REFERENCES items(id),
                    FOREIGN KEY(branch_id) REFERENCES pipe_branches(id),
                    FOREIGN KEY(unit_id) REFERENCES disinfection_units(id)
                );
                CREATE INDEX IF NOT EXISTS idx_batches_unit_status ON disinfection_batches(unit_id, status);
                CREATE INDEX IF NOT EXISTS idx_batches_branch_status ON disinfection_batches(branch_id, status);
                CREATE INDEX IF NOT EXISTS idx_batches_item ON disinfection_batches(item_id);
                CREATE INDEX IF NOT EXISTS idx_batches_sequence ON disinfection_batches(sequence);
                """
            )
        finally:
            conn.close()

    def _row_to_item(self, row):
        if row is None:
            return None
        result = dict(row)
        result["payload"] = json.loads(result["payload"])
        return result

    def _last_hash(self, conn, item_id):
        row = conn.execute(
            "SELECT event_hash FROM audit_events WHERE item_id IS ? ORDER BY id DESC LIMIT 1",
            (item_id,),
        ).fetchone()
        return row["event_hash"] if row else "GENESIS"

    def append_audit(self, conn, item_id, event_type, actor, role, payload):
        previous = self._last_hash(conn, item_id)
        event = {
            "item_id": item_id,
            "event_type": event_type,
            "actor": actor,
            "role": role,
            "payload": payload,
            "created_at": now_iso(),
        }
        event_hash = audit_hash(previous, event)
        conn.execute(
            "INSERT INTO audit_events(item_id,event_type,actor,role,payload,previous_hash,event_hash,created_at) VALUES(?,?,?,?,?,?,?,?)",
            (item_id, event_type, actor, role, canonical_json(payload), previous, event_hash, event["created_at"]),
        )

    def create_item(self, entity_type, stable_key, initial_status, payload, actor, role):
        conn = self.connect()
        try:
            conn.execute("BEGIN IMMEDIATE")
            try:
                conn.execute(
                    "INSERT INTO items(entity_type,stable_key,status,version,payload,created_by,created_role,created_at,updated_at) VALUES(?,?,?,?,?,?,?,?,?)",
                    (
                        entity_type,
                        stable_key,
                        initial_status,
                        1,
                        canonical_json(payload),
                        actor,
                        role,
                        now_iso(),
                        now_iso(),
                    ),
                )
            except sqlite3.IntegrityError:
                raise ConflictError("duplicate_item", "同一业务实体已经存在")
            item_id = conn.execute("SELECT last_insert_rowid() AS id").fetchone()["id"]
            self.append_audit(conn, item_id, "created", actor, role, {"stable_key": stable_key})
            conn.execute("COMMIT")
            return self.get_item(item_id)
        except Exception:
            try:
                conn.execute("ROLLBACK")
            except sqlite3.Error:
                pass
            raise
        finally:
            conn.close()

    def get_item(self, item_id):
        conn = self.connect()
        try:
            row = conn.execute("SELECT * FROM items WHERE id=?", (item_id,)).fetchone()
            if row is None:
                raise NotFoundError("item_not_found", "业务实体不存在")
            return self._row_to_item(row)
        finally:
            conn.close()

    def list_items(self, status=None):
        conn = self.connect()
        try:
            if status:
                rows = conn.execute("SELECT * FROM items WHERE status=? ORDER BY id DESC", (status,)).fetchall()
            else:
                rows = conn.execute("SELECT * FROM items ORDER BY id DESC").fetchall()
            return [self._row_to_item(row) for row in rows]
        finally:
            conn.close()

    def add_source(self, item_id, source_type, external_id, payload, observed_at, actor, role):
        conn = self.connect()
        try:
            conn.execute("BEGIN IMMEDIATE")
            item = conn.execute("SELECT id FROM items WHERE id=?", (item_id,)).fetchone()
            if item is None:
                raise NotFoundError("item_not_found", "业务实体不存在")
            try:
                conn.execute(
                    "INSERT INTO sources(item_id,source_type,external_id,payload,observed_at,created_at) VALUES(?,?,?,?,?,?)",
                    (item_id, source_type, external_id, canonical_json(payload), observed_at, now_iso()),
                )
            except sqlite3.IntegrityError:
                raise ConflictError("duplicate_source", "同一来源记录已经提交")
            source_id = conn.execute("SELECT last_insert_rowid() AS id").fetchone()["id"]
            self.append_audit(
                conn,
                item_id,
                "source_recorded",
                actor,
                role,
                {"source_id": source_id, "source_type": source_type, "external_id": external_id},
            )
            conn.execute("COMMIT")
            return {"id": source_id, "item_id": item_id, "source_type": source_type, "external_id": external_id, "payload": payload, "observed_at": observed_at}
        except Exception:
            try:
                conn.execute("ROLLBACK")
            except sqlite3.Error:
                pass
            raise
        finally:
            conn.close()

    def list_sources(self, item_id):
        conn = self.connect()
        try:
            rows = conn.execute("SELECT * FROM sources WHERE item_id=? ORDER BY id DESC", (item_id,)).fetchall()
            result = []
            for row in rows:
                value = dict(row)
                value["payload"] = json.loads(value["payload"])
                result.append(value)
            return result
        finally:
            conn.close()

    def apply_action(self, item_id, action, actor, role, new_status, new_payload, event_payload, expected_version=None):
        conn = self.connect()
        try:
            conn.execute("BEGIN IMMEDIATE")
            row = conn.execute("SELECT * FROM items WHERE id=?", (item_id,)).fetchone()
            if row is None:
                raise NotFoundError("item_not_found", "业务实体不存在")
            if expected_version is not None and int(expected_version) != int(row["version"]):
                raise ConflictError("version_conflict", "记录已被其他操作更新，请重新读取")
            version = int(row["version"]) + 1
            conn.execute(
                "UPDATE items SET status=?,version=?,payload=?,updated_at=? WHERE id=?",
                (new_status, version, canonical_json(new_payload), now_iso(), item_id),
            )
            conn.execute(
                "INSERT INTO actions(item_id,action,actor,role,payload,created_at) VALUES(?,?,?,?,?,?)",
                (item_id, action, actor, role, canonical_json(event_payload), now_iso()),
            )
            self.append_audit(conn, item_id, action, actor, role, event_payload)
            conn.execute("COMMIT")
            return self.get_item(item_id)
        except Exception:
            try:
                conn.execute("ROLLBACK")
            except sqlite3.Error:
                pass
            raise
        finally:
            conn.close()

    def audit_trail(self, item_id):
        conn = self.connect()
        try:
            rows = conn.execute("SELECT * FROM audit_events WHERE item_id=? ORDER BY id", (item_id,)).fetchall()
            result = []
            for row in rows:
                value = dict(row)
                value["payload"] = json.loads(value["payload"])
                result.append(value)
            return result
        finally:
            conn.close()

    def state_summary(self):
        conn = self.connect()
        try:
            counts = {}
            for row in conn.execute("SELECT status, COUNT(*) AS total FROM items GROUP BY status").fetchall():
                counts[row["status"]] = row["total"]
            return {"counts": counts, "items": self.list_items()}
        finally:
            conn.close()

    # ============================= 管网支路 / 机组 =============================

    def _branch_row_to_dict(self, row):
        if row is None:
            return None
        value = dict(row)
        return value

    def _ensure_branch(self, conn, branch_code, region=None, name=""):
        row = conn.execute("SELECT * FROM pipe_branches WHERE branch_code=?", (branch_code,)).fetchone()
        if row is not None:
            return self._branch_row_to_dict(row)
        now = now_iso()
        cur = conn.execute(
            "INSERT INTO pipe_branches(branch_code,name,upstream_id,region,status,created_at,updated_at) VALUES(?,?,NULL,?, 'active', ?, ?)",
            (branch_code, name, region, now, now),
        )
        return {
            "id": cur.lastrowid,
            "branch_code": branch_code,
            "name": name,
            "upstream_id": None,
            "region": region,
            "status": "active",
        }

    def _ensure_unit(self, conn, unit_code):
        row = conn.execute("SELECT * FROM disinfection_units WHERE unit_code=?", (unit_code,)).fetchone()
        if row is not None:
            return dict(row)
        now = now_iso()
        cur = conn.execute(
            "INSERT INTO disinfection_units(unit_code,name,status,current_batch_id,region,created_at,updated_at) VALUES(?, ?, 'available', NULL, NULL, ?, ?)",
            (unit_code, "移动消毒机组", now, now),
        )
        return {
            "id": cur.lastrowid,
            "unit_code": unit_code,
            "name": "移动消毒机组",
            "status": "available",
            "current_batch_id": None,
            "region": None,
        }

    def _branch_code(self, conn, branch_id):
        if branch_id is None:
            return None
        row = conn.execute("SELECT branch_code FROM pipe_branches WHERE id=?", (branch_id,)).fetchone()
        return row["branch_code"] if row else None

    def list_branches(self):
        conn = self.connect()
        try:
            rows = conn.execute("SELECT * FROM pipe_branches ORDER BY id").fetchall()
            result = []
            for row in rows:
                value = dict(row)
                value["upstream_code"] = self._branch_code(conn, value["upstream_id"])
                result.append(value)
            return result
        finally:
            conn.close()

    def branch_region_for(self, item_id, branch_code):
        """支路归属区域：已存在支路以其为准，否则继承污染事件区域。"""
        conn = self.connect()
        try:
            item = conn.execute("SELECT payload FROM items WHERE id=?", (item_id,)).fetchone()
            if item is None:
                raise NotFoundError("item_not_found", "业务实体不存在")
            row = conn.execute("SELECT region FROM pipe_branches WHERE branch_code=?", (branch_code,)).fetchone()
            if row is not None:
                return row["region"]
            return json.loads(item["payload"]).get("region")
        finally:
            conn.close()

    def list_units(self):
        conn = self.connect()
        try:
            rows = conn.execute("SELECT * FROM disinfection_units ORDER BY id").fetchall()
            return [dict(row) for row in rows]
        finally:
            conn.close()

    def set_branch_upstream(self, branch_code, upstream_code):
        """配置支路拓扑：upstream_code 为 None 表示独立支路。支路不存在时自动补齐。"""
        conn = self.connect()
        try:
            conn.execute("BEGIN IMMEDIATE")
            branch = self._ensure_branch(conn, branch_code)
            upstream_id = None
            if upstream_code:
                upstream = self._ensure_branch(conn, upstream_code)
                if upstream["id"] == branch["id"]:
                    raise DomainError("invalid_topology", "支路不能以上游身份指向自己")
                upstream_id = upstream["id"]
            conn.execute(
                "UPDATE pipe_branches SET upstream_id=?, updated_at=? WHERE id=?",
                (upstream_id, now_iso(), branch["id"]),
            )
            conn.execute("COMMIT")
            return self._branch_row_to_dict(
                conn.execute("SELECT * FROM pipe_branches WHERE id=?", (branch["id"],)).fetchone()
            )
        except Exception:
            try:
                conn.execute("ROLLBACK")
            except sqlite3.Error:
                pass
            raise
        finally:
            conn.close()

    # ============================= 消毒批次台账 =============================

    def _row_to_batch(self, conn, row):
        value = dict(row)
        branch = conn.execute("SELECT branch_code, region FROM pipe_branches WHERE id=?", (value["branch_id"],)).fetchone()
        value["branch_code"] = branch["branch_code"] if branch else None
        value["branch_region"] = branch["region"] if branch else None
        unit = conn.execute("SELECT unit_code FROM disinfection_units WHERE id=?", (value["unit_id"],)).fetchone()
        value["unit_code"] = unit["unit_code"] if unit else None
        return value

    def get_batch(self, batch_id):
        conn = self.connect()
        try:
            row = conn.execute("SELECT * FROM disinfection_batches WHERE id=?", (batch_id,)).fetchone()
            if row is None:
                raise NotFoundError("batch_not_found", "消毒批次不存在")
            return self._row_to_batch(conn, row)
        finally:
            conn.close()

    def list_batches(self, status=None, item_id=None):
        conn = self.connect()
        try:
            sql = "SELECT * FROM disinfection_batches WHERE 1=1"
            params = []
            if status:
                sql += " AND status=?"
                params.append(status)
            if item_id is not None:
                sql += " AND item_id=?"
                params.append(item_id)
            sql += " ORDER BY sequence"
            rows = conn.execute(sql, params).fetchall()
            return [self._row_to_batch(conn, row) for row in rows]
        finally:
            conn.close()

    def _release_unit(self, conn, unit_id):
        conn.execute(
            "UPDATE disinfection_units SET status='available', current_batch_id=NULL, updated_at=? WHERE id=?",
            (now_iso(), unit_id),
        )

    def _set_item_status(self, conn, item_id, new_status, new_payload):
        row = conn.execute("SELECT version FROM items WHERE id=?", (item_id,)).fetchone()
        if row is None:
            raise NotFoundError("item_not_found", "业务实体不存在")
        version = int(row["version"]) + 1
        conn.execute(
            "UPDATE items SET status=?, version=?, payload=?, updated_at=? WHERE id=?",
            (new_status, version, canonical_json(new_payload), now_iso(), item_id),
        )

    def _on_batch_started(self, conn, item_id, batch, actor, role):
        item = conn.execute("SELECT * FROM items WHERE id=?", (item_id,)).fetchone()
        if item is None:
            return
        payload = json.loads(item["payload"])
        payload.setdefault("batch_actions", []).append(
            {"type": "batch_start", "batch_id": batch["id"], "batch_code": batch["batch_code"],
             "zone_id": batch["branch_code"], "planned_dosage": batch["planned_dosage"]}
        )
        new_status = item["status"]
        if new_status in ("detected", "verified", "advisory", "switched"):
            new_status = "flushing"
        self._set_item_status(conn, item_id, new_status, payload)

    def _on_batch_completed(self, conn, item_id, batch, actor, role):
        item = conn.execute("SELECT * FROM items WHERE id=?", (item_id,)).fetchone()
        if item is None:
            return
        payload = json.loads(item["payload"])
        payload.setdefault("batch_actions", []).append(
            {"type": "batch_disinfect", "batch_id": batch["id"], "batch_code": batch["batch_code"],
             "zone_id": batch["branch_code"], "actual_dosage": batch["actual_dosage"]}
        )
        new_status = item["status"]
        if new_status in ("flushing", "reinspection_pending"):
            new_status = "disinfected"
        self._set_item_status(conn, item_id, new_status, payload)

    def create_batch(self, item_id, branch_code, unit_code, planned_dosage, note, actor, role):
        conn = self.connect()
        try:
            conn.execute("BEGIN IMMEDIATE")
            item = conn.execute("SELECT * FROM items WHERE id=?", (item_id,)).fetchone()
            if item is None:
                raise NotFoundError("item_not_found", "业务实体不存在")
            item_payload = json.loads(item["payload"])
            region = item_payload.get("region")
            branch = self._ensure_branch(conn, branch_code, region)
            unit = self._ensure_unit(conn, unit_code)

            sequence = conn.execute("SELECT COALESCE(MAX(sequence),0)+1 AS next FROM disinfection_batches").fetchone()["next"]

            active_unit = conn.execute(
                "SELECT * FROM disinfection_batches WHERE unit_id=? AND status IN (?,?,?) ORDER BY sequence LIMIT 1",
                (unit["id"], rules.BATCH_IN_PROGRESS, rules.BATCH_AWAITING_RECEIPT, rules.BATCH_RECEIPT_FAILED),
            ).fetchone()
            queued_count = conn.execute(
                "SELECT COUNT(*) AS c FROM disinfection_batches WHERE unit_id=? AND status=?",
                (unit["id"], rules.BATCH_QUEUED),
            ).fetchone()["c"]
            active_branch = conn.execute(
                "SELECT * FROM disinfection_batches WHERE branch_id=? AND status IN (?,?,?) ORDER BY sequence LIMIT 1",
                (branch["id"], rules.BATCH_IN_PROGRESS, rules.BATCH_AWAITING_RECEIPT, rules.BATCH_RECEIPT_FAILED),
            ).fetchone()

            now = now_iso()
            if active_branch is not None:
                raise ConflictError(
                    "branch_occupied",
                    "支路 %s 已被批次 %s 占用，无法开工" % (branch_code, active_branch["batch_code"]),
                )
            if active_unit is not None or queued_count > 0:
                q_row = conn.execute(
                    "SELECT COALESCE(MAX(queue_number),0)+1 AS next FROM disinfection_batches WHERE unit_id=? AND status=?",
                    (unit["id"], rules.BATCH_QUEUED),
                ).fetchone()
                queue_number = q_row["next"]
                status = rules.BATCH_QUEUED
                dosed = 0
                started_at = None
            else:
                status = rules.BATCH_IN_PROGRESS
                dosed = 1
                started_at = now
                queue_number = None

            cur = conn.execute(
                """INSERT INTO disinfection_batches
                   (batch_code,item_id,branch_id,unit_id,queue_number,sequence,status,planned_dosage,actual_dosage,
                    receipt_status,receipt_note,invalidated,invalidation_reason,retry_count,dosed,note,
                    created_by,created_role,created_at,started_at,receipted_at,completed_at,updated_at)
                   VALUES(?,?,?,?,?,?,?,?,NULL,'pending','',0,'',0,?,?,?,?,?,?,NULL,NULL,?)""",
                (
                    "", item_id, branch["id"], unit["id"], queue_number, sequence, status, planned_dosage,
                    dosed, note, actor, role, now, started_at, now,
                ),
            )
            batch_id = cur.lastrowid
            batch_code = "B-%04d" % batch_id
            conn.execute("UPDATE disinfection_batches SET batch_code=? WHERE id=?", (batch_code, batch_id))
            batch = self._row_to_batch(conn, conn.execute("SELECT * FROM disinfection_batches WHERE id=?", (batch_id,)).fetchone())

            if status == rules.BATCH_IN_PROGRESS:
                conn.execute(
                    "UPDATE disinfection_units SET status='occupied', current_batch_id=?, updated_at=? WHERE id=?",
                    (batch_id, now, unit["id"]),
                )
                self.append_audit(conn, item_id, "batch_started", actor, role,
                                  {"batch_id": batch_id, "batch_code": batch_code, "branch_code": branch_code,
                                   "unit_code": unit_code, "planned_dosage": planned_dosage})
                self.append_audit(conn, item_id, "batch_dosed", actor, role,
                                  {"batch_id": batch_id, "batch_code": batch_code, "planned_dosage": planned_dosage})
                self._on_batch_started(conn, item_id, batch, actor, role)
            else:
                self.append_audit(conn, item_id, "batch_queued", actor, role,
                                  {"batch_id": batch_id, "batch_code": batch_code, "branch_code": branch_code,
                                   "unit_code": unit_code, "queue_number": queue_number, "sequence": sequence})

            conn.execute("COMMIT")
            return self.get_batch(batch_id)
        except Exception:
            try:
                conn.execute("ROLLBACK")
            except sqlite3.Error:
                pass
            raise
        finally:
            conn.close()

    def _pump(self, unit_id):
        """机组释放后，按原顺序派发排队批次；支路仍被占用则保留排队（不跳号）。"""
        conn = self.connect()
        try:
            conn.execute("BEGIN IMMEDIATE")
            unit = conn.execute("SELECT * FROM disinfection_units WHERE id=?", (unit_id,)).fetchone()
            if unit is None:
                conn.execute("COMMIT")
                return
            active = conn.execute(
                "SELECT id FROM disinfection_batches WHERE unit_id=? AND status IN (?,?,?) LIMIT 1",
                (unit_id, rules.BATCH_IN_PROGRESS, rules.BATCH_AWAITING_RECEIPT, rules.BATCH_RECEIPT_FAILED),
            ).fetchone()
            if active is not None:
                conn.execute("COMMIT")
                return
            queued = conn.execute(
                "SELECT * FROM disinfection_batches WHERE unit_id=? AND status=? ORDER BY sequence LIMIT 1",
                (unit_id, rules.BATCH_QUEUED),
            ).fetchone()
            now = now_iso()
            if queued is None:
                conn.execute(
                    "UPDATE disinfection_units SET status='available', current_batch_id=NULL, updated_at=? WHERE id=?",
                    (now, unit_id),
                )
                conn.execute("COMMIT")
                return
            branch_active = conn.execute(
                "SELECT id FROM disinfection_batches WHERE branch_id=? AND status IN (?,?,?) LIMIT 1",
                (queued["branch_id"], rules.BATCH_IN_PROGRESS, rules.BATCH_AWAITING_RECEIPT, rules.BATCH_RECEIPT_FAILED),
            ).fetchone()
            if branch_active is not None:
                conn.execute("COMMIT")
                return
            conn.execute(
                "UPDATE disinfection_batches SET status=?, queue_number=NULL, dosed=1, started_at=?, updated_at=? WHERE id=?",
                (rules.BATCH_IN_PROGRESS, now, now, queued["id"]),
            )
            conn.execute(
                "UPDATE disinfection_units SET status='occupied', current_batch_id=?, updated_at=? WHERE id=?",
                (queued["id"], now, unit_id),
            )
            self.append_audit(conn, queued["item_id"], "batch_started", "system", "scheduler",
                              {"batch_id": queued["id"], "batch_code": queued["batch_code"], "dispatched": True})
            self.append_audit(conn, queued["item_id"], "batch_dosed", "system", "scheduler",
                              {"batch_id": queued["id"], "batch_code": queued["batch_code"],
                               "planned_dosage": queued["planned_dosage"], "queued": True})
            dispatched = self._row_to_batch(conn, conn.execute("SELECT * FROM disinfection_batches WHERE id=?", (queued["id"],)).fetchone())
            self._on_batch_started(conn, queued["item_id"], dispatched, "system", "scheduler")
            conn.execute("COMMIT")
        except Exception:
            try:
                conn.execute("ROLLBACK")
            except sqlite3.Error:
                pass
            raise
        finally:
            conn.close()

    def submit_receipt(self, batch_id, actual_dosage, success, note, actor, role):
        conn = self.connect()
        try:
            conn.execute("BEGIN IMMEDIATE")
            batch = conn.execute("SELECT * FROM disinfection_batches WHERE id=?", (batch_id,)).fetchone()
            if batch is None:
                raise NotFoundError("batch_not_found", "消毒批次不存在")
            if batch["status"] not in (rules.BATCH_IN_PROGRESS, rules.BATCH_AWAITING_RECEIPT):
                raise DomainError("batch_not_started", "批次尚未开工或已结案，不能提交回执", 409)
            now = now_iso()
            if success:
                conn.execute(
                    """UPDATE disinfection_batches
                       SET status=?, actual_dosage=?, receipt_status=?, receipt_note=?, receipted_at=?, completed_at=?, updated_at=?
                       WHERE id=?""",
                    (rules.BATCH_COMPLETED, actual_dosage, "succeeded", note, now, now, now, batch_id),
                )
                self.append_audit(conn, batch["item_id"], "batch_receipted", actor, role,
                                  {"batch_id": batch_id, "batch_code": batch["batch_code"], "actual_dosage": actual_dosage})
                self.append_audit(conn, batch["item_id"], "batch_completed", actor, role,
                                  {"batch_id": batch_id, "batch_code": batch["batch_code"]})
                self._release_unit(conn, batch["unit_id"])
                updated = self._row_to_batch(conn, conn.execute("SELECT * FROM disinfection_batches WHERE id=?", (batch_id,)).fetchone())
                self._on_batch_completed(conn, batch["item_id"], updated, actor, role)
                conn.execute("COMMIT")
                self._pump(batch["unit_id"])
            else:
                conn.execute(
                    """UPDATE disinfection_batches
                       SET status=?, actual_dosage=?, receipt_status=?, receipt_note=?, updated_at=?
                       WHERE id=?""",
                    (rules.BATCH_RECEIPT_FAILED, actual_dosage, "failed", note, now, batch_id),
                )
                self.append_audit(conn, batch["item_id"], "batch_receipt_failed", actor, role,
                                  {"batch_id": batch_id, "batch_code": batch["batch_code"], "actual_dosage": actual_dosage})
                # 保留未完成批次，机组仍占用，不释放、不跳转
                conn.execute("COMMIT")
            return self.get_batch(batch_id)
        except Exception:
            try:
                conn.execute("ROLLBACK")
            except sqlite3.Error:
                pass
            raise
        finally:
            conn.close()

    def retry_batch(self, batch_id, actual_dosage, note, actor, role):
        conn = self.connect()
        try:
            conn.execute("BEGIN IMMEDIATE")
            batch = conn.execute("SELECT * FROM disinfection_batches WHERE id=?", (batch_id,)).fetchone()
            if batch is None:
                raise NotFoundError("batch_not_found", "消毒批次不存在")
            if batch["status"] != rules.BATCH_RECEIPT_FAILED:
                raise DomainError("batch_not_retryable", "只有回执失败的批次需要重试", 409)
            dosage = actual_dosage if actual_dosage is not None else batch["actual_dosage"]
            if dosage is None or dosage <= 0:
                raise DomainError("receipt_incomplete", "回执缺少实际加药量，无法重试；不会重复加药", 409)
            now = now_iso()
            retry_count = int(batch["retry_count"]) + 1
            conn.execute(
                """UPDATE disinfection_batches
                   SET status=?, actual_dosage=?, receipt_status=?, retry_count=?, receipt_note=?, completed_at=?, updated_at=?
                   WHERE id=?""",
                (rules.BATCH_COMPLETED, dosage, "succeeded", retry_count, note, now, now, batch_id),
            )
            # 注意：重试不再下发加药指令，不产生 batch_dosed，不重复加药
            self.append_audit(conn, batch["item_id"], "batch_retried", actor, role,
                              {"batch_id": batch_id, "batch_code": batch["batch_code"], "retry_count": retry_count})
            self.append_audit(conn, batch["item_id"], "batch_completed", actor, role,
                              {"batch_id": batch_id, "batch_code": batch["batch_code"], "retry": True})
            self._release_unit(conn, batch["unit_id"])
            updated = self._row_to_batch(conn, conn.execute("SELECT * FROM disinfection_batches WHERE id=?", (batch_id,)).fetchone())
            self._on_batch_completed(conn, batch["item_id"], updated, actor, role)
            conn.execute("COMMIT")
            self._pump(batch["unit_id"])
            return self.get_batch(batch_id)
        except Exception:
            try:
                conn.execute("ROLLBACK")
            except sqlite3.Error:
                pass
            raise
        finally:
            conn.close()

    def release_batch(self, batch_id, note, actor, role):
        """监管/协调强制放行未结案批次。"""
        conn = self.connect()
        try:
            conn.execute("BEGIN IMMEDIATE")
            batch = conn.execute("SELECT * FROM disinfection_batches WHERE id=?", (batch_id,)).fetchone()
            if batch is None:
                raise NotFoundError("batch_not_found", "消毒批次不存在")
            if batch["status"] in rules.BATCH_TERMINAL_STATUSES:
                raise DomainError("batch_already_closed", "批次已结案，不能重复放行", 409)
            now = now_iso()
            conn.execute(
                """UPDATE disinfection_batches
                   SET status=?, receipt_status=?, receipt_note=?, completed_at=?, updated_at=?
                   WHERE id=?""",
                (rules.BATCH_COMPLETED, "released", note, now, now, batch_id),
            )
            self.append_audit(conn, batch["item_id"], "batch_released", actor, role,
                              {"batch_id": batch_id, "batch_code": batch["batch_code"], "note": note})
            self._release_unit(conn, batch["unit_id"])
            updated = self._row_to_batch(conn, conn.execute("SELECT * FROM disinfection_batches WHERE id=?", (batch_id,)).fetchone())
            self._on_batch_completed(conn, batch["item_id"], updated, actor, role)
            conn.execute("COMMIT")
            self._pump(batch["unit_id"])
            return self.get_batch(batch_id)
        except Exception:
            try:
                conn.execute("ROLLBACK")
            except sqlite3.Error:
                pass
            raise
        finally:
            conn.close()

    def invalidate_for_source(self, item_id, zone_id, reason, actor, role):
        """覆盖区域污染来源更新：整批消毒失效，下游已恢复区域一起退回待复检。"""
        conn = self.connect()
        try:
            conn.execute("BEGIN IMMEDIATE")
            item = conn.execute("SELECT * FROM items WHERE id=?", (item_id,)).fetchone()
            if item is None:
                raise NotFoundError("item_not_found", "业务实体不存在")

            branch_rows = conn.execute("SELECT * FROM pipe_branches").fetchall()
            branches_by_code = {}
            branch_id_by_code = {}
            for b in branch_rows:
                code = b["branch_code"]
                branches_by_code[code] = {"upstream": self._branch_code(conn, b["upstream_id"]), "region": b["region"]}
                branch_id_by_code[code] = b["id"]

            affected_codes = rules.downstream_codes(branches_by_code, zone_id)
            affected_ids = [branch_id_by_code[c] for c in affected_codes if c in branch_id_by_code]

            batches = []
            if affected_ids:
                placeholders = ",".join("?" for _ in affected_ids)
                batches = conn.execute(
                    "SELECT * FROM disinfection_batches WHERE item_id=? AND branch_id IN (%s) AND status != ?" % placeholders,
                    [item_id] + affected_ids + [rules.BATCH_INVALIDATED],
                ).fetchall()

            now = now_iso()
            released_units = set()
            for b in batches:
                conn.execute(
                    "UPDATE disinfection_batches SET status=?, invalidated=?, invalidation_reason=?, updated_at=? WHERE id=?",
                    (rules.BATCH_INVALIDATED, 1, reason or "覆盖区域污染来源更新", now, b["id"]),
                )
                self.append_audit(conn, item_id, "batch_invalidated", actor, role,
                                  {"batch_id": b["id"], "batch_code": b["batch_code"],
                                   "branch_code": self._branch_code(conn, b["branch_id"]), "reason": reason})
                if b["unit_id"] not in released_units:
                    self._release_unit(conn, b["unit_id"])
                    released_units.add(b["unit_id"])

            payload = json.loads(item["payload"])
            new_payload, new_status = rules.apply_source_invalidation(payload, sorted(affected_codes))
            if item["status"] in ("disinfected", "sampled", "restored"):
                self._set_item_status(conn, item_id, new_status, new_payload)
            else:
                self._set_item_status(conn, item_id, item["status"], new_payload)
            self.append_audit(conn, item_id, "reinspection_required", actor, role,
                              {"affected_zones": sorted(affected_codes), "reason": reason,
                               "invalidated_batches": [b["batch_code"] for b in batches]})
            conn.execute("COMMIT")
            for uid in released_units:
                self._pump(uid)
            return self.get_item(item_id)
        except Exception:
            try:
                conn.execute("ROLLBACK")
            except sqlite3.Error:
                pass
            raise
        finally:
            conn.close()

    def migrate(self):
        """旧数据升级：按单区支路补齐管网支路，并确保默认机组存在；不改动历史业务记录。"""
        conn = self.connect()
        try:
            conn.execute("BEGIN IMMEDIATE")
            self._ensure_unit(conn, DEFAULT_UNIT_CODE)
            rows = conn.execute("SELECT id, payload FROM items").fetchall()
            for row in rows:
                payload = json.loads(row["payload"])
                region = payload.get("region")
                for zone in payload.get("zone_ids", []):
                    self._ensure_branch(conn, zone, region)
            conn.execute("COMMIT")
        except Exception:
            try:
                conn.execute("ROLLBACK")
            except sqlite3.Error:
                pass
            raise
        finally:
            conn.close()
