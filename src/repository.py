import json
import sqlite3
from datetime import datetime, timezone

from .audit import audit_hash, canonical_json
from .domain import ConflictError, NotFoundError, DomainError

SCHEMA_VERSION = 2

# 消毒批次状态：排队 / 占用中（冲洗消毒作业）/ 已完成回执 / 因来源更新失效
BATCH_QUEUED = "queued"
BATCH_ACTIVE = "active"
BATCH_COMPLETED = "completed"
BATCH_INVALIDATED = "invalidated"

# 区域水质状态：受影响 / 待复检 / 已恢复
ZONE_AFFECTED = "affected"
ZONE_PENDING_RETEST = "pending_retest"
ZONE_RESTORED = "restored"

LEGACY_UNIT_CODE = "U-LEGACY"
LEGACY_BRANCH_PREFIX = "BR-"


def now_iso():
    return datetime.now(timezone.utc).isoformat()


def branch_code_for_zone(zone_id):
    return LEGACY_BRANCH_PREFIX + zone_id


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
                CREATE TABLE IF NOT EXISTS branches (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    code TEXT NOT NULL UNIQUE,
                    name TEXT NOT NULL DEFAULT '',
                    zone_ids TEXT NOT NULL DEFAULT '[]',
                    region TEXT,
                    created_at TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS branch_links (
                    upstream_code TEXT NOT NULL,
                    downstream_code TEXT NOT NULL,
                    PRIMARY KEY(upstream_code, downstream_code)
                );
                CREATE TABLE IF NOT EXISTS units (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    code TEXT NOT NULL UNIQUE,
                    name TEXT NOT NULL DEFAULT '',
                    created_at TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS disinfection_batches (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    unit_code TEXT NOT NULL,
                    branch_code TEXT NOT NULL,
                    item_id INTEGER,
                    status TEXT NOT NULL,
                    queue_no INTEGER,
                    dose REAL,
                    idempotency_key TEXT,
                    receipt_payload TEXT,
                    created_by TEXT NOT NULL,
                    created_role TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    UNIQUE(idempotency_key)
                );
                CREATE TABLE IF NOT EXISTS zone_states (
                    zone_id TEXT NOT NULL,
                    item_id INTEGER NOT NULL,
                    status TEXT NOT NULL,
                    batch_id INTEGER,
                    updated_at TEXT NOT NULL,
                    PRIMARY KEY(zone_id, item_id)
                );
                CREATE TABLE IF NOT EXISTS schema_meta (
                    key TEXT PRIMARY KEY,
                    value TEXT NOT NULL
                );
                """
            )
            self._migrate(conn)
        finally:
            conn.close()

    def _migrate(self, conn):
        """旧数据升级：单区支路补齐，并兼容历史冲洗/消毒记录。"""
        row = conn.execute("SELECT value FROM schema_meta WHERE key='version'").fetchone()
        version = int(row["value"]) if row else 1
        if version >= SCHEMA_VERSION:
            return
        conn.execute("BEGIN IMMEDIATE")
        try:
            if version < 2:
                # 历史记录里没有机组/支路概念：补一台共用移动消毒机组
                conn.execute(
                    "INSERT OR IGNORE INTO units(code,name,created_at) VALUES(?,?,?)",
                    (LEGACY_UNIT_CODE, "历史移动消毒机组（冲洗消毒共用）", now_iso()),
                )
                existing = {r["code"] for r in conn.execute("SELECT code FROM branches").fetchall()}
                zones = set()
                for item_row in conn.execute("SELECT payload FROM items").fetchall():
                    payload = json.loads(item_row["payload"])
                    for zone_id in payload.get("zone_ids", []):
                        zones.add(zone_id)
                for zone_id in sorted(zones):
                    code = branch_code_for_zone(zone_id)
                    if code not in existing:
                        conn.execute(
                            "INSERT OR IGNORE INTO branches(code,name,zone_ids,region,created_at) VALUES(?,?,?,?,?)",
                            (code, "历史支路 %s" % zone_id, canonical_json([zone_id]), None, now_iso()),
                        )
                # 已恢复的历史区域以批次完成态落账，保持与新数据一致的可读状态
                batch_no = 0
                for item_row in conn.execute(
                    "SELECT id, payload, status FROM items WHERE status IN ('sampled','restored') ORDER BY id"
                ).fetchall():
                    item_id = item_row["id"]
                    payload = json.loads(item_row["payload"])
                    for zone_id in payload.get("zone_ids", []):
                        done = any(
                            a["action"] == "disinfect"
                            for a in conn.execute(
                                "SELECT action FROM actions WHERE item_id=?", (item_id,)
                            ).fetchall()
                        )
                        state = ZONE_RESTORED if item_row["status"] == "restored" else ZONE_PENDING_RETEST
                        conn.execute(
                            "INSERT OR IGNORE INTO zone_states(zone_id,item_id,status,batch_id,updated_at) VALUES(?,?,?,?,?)",
                            (zone_id, item_id, state, None, now_iso()),
                        )
                        if done:
                            batch_no += 1
                            conn.execute(
                                "INSERT INTO disinfection_batches(unit_code,branch_code,item_id,status,queue_no,dose,idempotency_key,receipt_payload,created_by,created_role,created_at,updated_at) VALUES(?,?,?,?,?,?,?,?,?,?,?,?)",
                                (
                                    LEGACY_UNIT_CODE,
                                    branch_code_for_zone(zone_id),
                                    item_id,
                                    BATCH_COMPLETED,
                                    None,
                                    None,
                                    "legacy:%d:%s" % (item_id, zone_id),
                                    None,
                                    "system",
                                    "system",
                                    now_iso(),
                                    now_iso(),
                                ),
                            )
            conn.execute(
                "INSERT INTO schema_meta(key,value) VALUES('version',?) ON CONFLICT(key) DO UPDATE SET value=excluded.value",
                (str(SCHEMA_VERSION),),
            )
            conn.execute("COMMIT")
        except Exception:
            conn.execute("ROLLBACK")
            raise

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
            cascade = self.cascade_source_update(
                conn, item_id, payload.get("zone_id"), source_type, external_id, actor, role
            )
            conn.execute("COMMIT")
            return {"id": source_id, "item_id": item_id, "source_type": source_type, "external_id": external_id, "payload": payload, "observed_at": observed_at, "cascade": cascade}
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

    # ---- 管网支路 / 移动消毒机组 / 消毒批次联动账 ----

    def register_unit(self, code, name=""):
        conn = self.connect()
        try:
            conn.execute("BEGIN IMMEDIATE")
            try:
                conn.execute(
                    "INSERT INTO units(code,name,created_at) VALUES(?,?,?)",
                    (code, name, now_iso()),
                )
            except sqlite3.IntegrityError:
                raise ConflictError("duplicate_unit", "该移动消毒机组已登记")
            conn.execute("COMMIT")
            return self.get_unit(code)
        except Exception:
            try:
                conn.execute("ROLLBACK")
            except sqlite3.Error:
                pass
            raise
        finally:
            conn.close()

    def get_unit(self, code):
        conn = self.connect()
        try:
            row = conn.execute("SELECT * FROM units WHERE code=?", (code,)).fetchone()
            if row is None:
                raise NotFoundError("unit_not_found", "移动消毒机组不存在")
            result = dict(row)
            active = conn.execute(
                "SELECT * FROM disinfection_batches WHERE unit_code=? AND status=?", (code, BATCH_ACTIVE)
            ).fetchone()
            result["active_batch_id"] = active["id"] if active else None
            result["queue"] = [
                r["id"]
                for r in conn.execute(
                    "SELECT id FROM disinfection_batches WHERE unit_code=? AND status=? ORDER BY id",
                    (code, BATCH_QUEUED),
                ).fetchall()
            ]
            return result
        finally:
            conn.close()

    def register_branch(self, code, name, zone_ids, region, downstream_codes, actor, role):
        conn = self.connect()
        try:
            conn.execute("BEGIN IMMEDIATE")
            try:
                conn.execute(
                    "INSERT INTO branches(code,name,zone_ids,region,created_at) VALUES(?,?,?,?,?)",
                    (code, name, canonical_json(zone_ids), region, now_iso()),
                )
            except sqlite3.IntegrityError:
                raise ConflictError("duplicate_branch", "该管网支路已登记")
            for downstream in downstream_codes:
                if not conn.execute("SELECT 1 FROM branches WHERE code=?", (downstream,)).fetchone():
                    raise NotFoundError("branch_not_found", "下游支路不存在: %s" % downstream)
                conn.execute(
                    "INSERT OR IGNORE INTO branch_links(upstream_code,downstream_code) VALUES(?,?)",
                    (code, downstream),
                )
            self.append_audit(conn, None, "branch_registered", actor, role, {"code": code, "zone_ids": zone_ids})
            conn.execute("COMMIT")
            return self.get_branch(code)
        except Exception:
            try:
                conn.execute("ROLLBACK")
            except sqlite3.Error:
                pass
            raise
        finally:
            conn.close()

    def get_branch(self, code):
        conn = self.connect()
        try:
            row = conn.execute("SELECT * FROM branches WHERE code=?", (code,)).fetchone()
            if row is None:
                raise NotFoundError("branch_not_found", "管网支路不存在")
            result = dict(row)
            result["zone_ids"] = json.loads(result["zone_ids"])
            result["downstream"] = [
                r["downstream_code"]
                for r in conn.execute(
                    "SELECT downstream_code FROM branch_links WHERE upstream_code=? ORDER BY downstream_code",
                    (code,),
                ).fetchall()
            ]
            active = conn.execute(
                "SELECT * FROM disinfection_batches WHERE branch_code=? AND status=?", (code, BATCH_ACTIVE)
            ).fetchone()
            result["occupied_by_batch"] = active["id"] if active else None
            return result
        finally:
            conn.close()

    def list_branches(self):
        conn = self.connect()
        try:
            codes = [r["code"] for r in conn.execute("SELECT code FROM branches ORDER BY code").fetchall()]
            return [self.get_branch(code) for code in codes]
        finally:
            conn.close()

    def _downstream_codes(self, conn, start_code):
        seen, frontier = set(), [start_code]
        while frontier:
            current = frontier.pop()
            for link in conn.execute(
                "SELECT downstream_code FROM branch_links WHERE upstream_code=?", (current,)
            ).fetchall():
                code = link["downstream_code"]
                if code not in seen:
                    seen.add(code)
                    frontier.append(code)
        return seen

    def _branch_zones(self, conn, codes):
        zones = set()
        for code in codes:
            branch_row = conn.execute("SELECT zone_ids FROM branches WHERE code=?", (code,)).fetchone()
            if branch_row:
                zones.update(json.loads(branch_row["zone_ids"]))
        return zones

    def _promote_next(self, conn, unit_code):
        """机组释放后按原排队顺序先到先领；被其他支路占用的批次顺延。"""
        rows = conn.execute(
            "SELECT * FROM disinfection_batches WHERE unit_code=? AND status=? ORDER BY id",
            (unit_code, BATCH_QUEUED),
        ).fetchall()
        for row in rows:
            busy = conn.execute(
                "SELECT 1 FROM disinfection_batches WHERE branch_code=? AND status=?",
                (row["branch_code"], BATCH_ACTIVE),
            ).fetchone()
            if busy:
                continue
            conn.execute(
                "UPDATE disinfection_batches SET status=?,queue_no=NULL,updated_at=? WHERE id=?",
                (BATCH_ACTIVE, now_iso(), row["id"]),
            )
            self._touch_zones(conn, row, ZONE_AFFECTED)
            self.append_audit(
                conn, row["item_id"], "batch_acquired", "system", "system", {"batch_id": row["id"], "auto": True}
            )
            return row["id"]
        return None

    def _promote_queued(self, conn, unit_codes):
        """批次结束后跨机组扫描：先到者领批，支路仍被占用则顺延，避免饿死。"""
        promoted = []
        for unit_code in unit_codes:
            next_id = self._promote_next(conn, unit_code)
            if next_id is not None:
                promoted.append(next_id)
        return promoted

    def _touch_zones(self, conn, batch_row, status):
        branch = conn.execute(
            "SELECT zone_ids FROM branches WHERE code=?", (batch_row["branch_code"],)
        ).fetchone()
        if branch is None:
            return
        for zone_id in json.loads(branch["zone_ids"]):
            conn.execute(
                "INSERT INTO zone_states(zone_id,item_id,status,batch_id,updated_at) VALUES(?,?,?,?,?) "
                "ON CONFLICT(zone_id,item_id) DO UPDATE SET status=excluded.status,batch_id=excluded.batch_id,updated_at=excluded.updated_at",
                (zone_id, batch_row["item_id"], status, batch_row["id"], now_iso()),
            )

    def start_batch(self, unit_code, branch_code, item_id, actor, role):
        conn = self.connect()
        try:
            conn.execute("BEGIN IMMEDIATE")
            if not conn.execute("SELECT 1 FROM units WHERE code=?", (unit_code,)).fetchone():
                raise NotFoundError("unit_not_found", "移动消毒机组不存在")
            if not conn.execute("SELECT 1 FROM branches WHERE code=?", (branch_code,)).fetchone():
                raise NotFoundError("branch_not_found", "管网支路不存在")
            unit_busy = conn.execute(
                "SELECT 1 FROM disinfection_batches WHERE unit_code=? AND status=?", (unit_code, BATCH_ACTIVE)
            ).fetchone()
            branch_busy = conn.execute(
                "SELECT 1 FROM disinfection_batches WHERE branch_code=? AND status=?",
                (branch_code, BATCH_ACTIVE),
            ).fetchone()
            created = now_iso()
            if unit_busy or branch_busy:
                # 机组或支路仍被上一批占用：后到者只能拿到排队号
                queued = conn.execute(
                    "SELECT COALESCE(MAX(queue_no),0)+1 AS next_no FROM disinfection_batches WHERE unit_code=? AND status=?",
                    (unit_code, BATCH_QUEUED),
                ).fetchone()
                cur = conn.execute(
                    "INSERT INTO disinfection_batches(unit_code,branch_code,item_id,status,queue_no,created_by,created_role,created_at,updated_at) VALUES(?,?,?,?,?,?,?,?,?)",
                    (unit_code, branch_code, item_id, BATCH_QUEUED, queued["next_no"], actor, role, created, created),
                )
                batch_id = cur.lastrowid
                self.append_audit(
                    conn, item_id, "batch_queued", actor, role,
                    {"batch_id": batch_id, "unit_code": unit_code, "branch_code": branch_code, "queue_no": queued["next_no"]},
                )
                conn.execute("COMMIT")
                result = self.get_batch(batch_id)
                result["acquired"] = False
                return result
            cur = conn.execute(
                "INSERT INTO disinfection_batches(unit_code,branch_code,item_id,status,created_by,created_role,created_at,updated_at) VALUES(?,?,?,?,?,?,?,?)",
                (unit_code, branch_code, item_id, BATCH_ACTIVE, actor, role, created, created),
            )
            batch_id = cur.lastrowid
            row = conn.execute("SELECT * FROM disinfection_batches WHERE id=?", (batch_id,)).fetchone()
            self._touch_zones(conn, row, ZONE_AFFECTED)
            self.append_audit(
                conn, item_id, "batch_acquired", actor, role,
                {"batch_id": batch_id, "unit_code": unit_code, "branch_code": branch_code},
            )
            conn.execute("COMMIT")
            result = self.get_batch(batch_id)
            result["acquired"] = True
            return result
        except Exception:
            try:
                conn.execute("ROLLBACK")
            except sqlite3.Error:
                pass
            raise
        finally:
            conn.close()

    def get_batch(self, batch_id):
        conn = self.connect()
        try:
            row = conn.execute("SELECT * FROM disinfection_batches WHERE id=?", (batch_id,)).fetchone()
            if row is None:
                raise NotFoundError("batch_not_found", "消毒批次不存在")
            result = dict(row)
            result["receipt"] = json.loads(result["receipt_payload"]) if result["receipt_payload"] else None
            del result["receipt_payload"]
            branch = conn.execute("SELECT zone_ids FROM branches WHERE code=?", (row["branch_code"],)).fetchone()
            result["zone_ids"] = json.loads(branch["zone_ids"]) if branch else []
            return result
        finally:
            conn.close()

    def list_batches(self, unit_code=None, status=None):
        conn = self.connect()
        try:
            sql = "SELECT id FROM disinfection_batches WHERE 1=1"
            args = []
            if unit_code:
                sql += " AND unit_code=?"
                args.append(unit_code)
            if status:
                sql += " AND status=?"
                args.append(status)
            sql += " ORDER BY id"
            rows = conn.execute(sql, args).fetchall()
            return [self.get_batch(r["id"]) for r in rows]
        finally:
            conn.close()

    def submit_receipt(self, batch_id, idempotency_key, success, dose, note, actor, role):
        """回执带回实际加药量；同一回执号重试不重复加药。"""
        conn = self.connect()
        try:
            conn.execute("BEGIN IMMEDIATE")
            row = conn.execute("SELECT * FROM disinfection_batches WHERE id=?", (batch_id,)).fetchone()
            if row is None:
                raise NotFoundError("batch_not_found", "消毒批次不存在")
            dup = conn.execute(
                "SELECT * FROM disinfection_batches WHERE idempotency_key=? AND status=?",
                (idempotency_key, BATCH_COMPLETED),
            ).fetchone()
            if dup is not None:
                conn.execute("COMMIT")
                result = self.get_batch(dup["id"])
                result["deduplicated"] = True
                return result
            if row["status"] == BATCH_INVALIDATED:
                raise DomainError("batch_invalidated", "该批次已因污染来源更新失效", 409)
            receipt = {"idempotency_key": idempotency_key, "success": bool(success), "note": note, "actor": actor}
            if success:
                if dose is None or float(dose) < 0:
                    raise DomainError("invalid_dose", "回执需要非负的实际加药量")
                receipt["dose"] = float(dose)
                conn.execute(
                    "UPDATE disinfection_batches SET status=?,dose=?,idempotency_key=?,receipt_payload=?,updated_at=? WHERE id=?",
                    (BATCH_COMPLETED, float(dose), idempotency_key, canonical_json(receipt), now_iso(), batch_id),
                )
                self._touch_zones(conn, row, ZONE_PENDING_RETEST)
                waiting_units = [
                    r["unit_code"]
                    for r in conn.execute(
                        "SELECT DISTINCT unit_code FROM disinfection_batches WHERE status=? ORDER BY id",
                        (BATCH_QUEUED,),
                    ).fetchall()
                ]
                unit_order = [row["unit_code"]] + [code for code in waiting_units if code != row["unit_code"]]
                promoted = self._promote_queued(conn, unit_order)
                self.append_audit(
                    conn, row["item_id"], "batch_receipt", actor, role,
                    {"batch_id": batch_id, "dose": float(dose), "promoted_batch_ids": promoted},
                )
            else:
                # 回执失败：保留未完成批次，仍占用机组，等待按原顺序重试，不记加药量
                prev = json.loads(row["receipt_payload"]) if row["receipt_payload"] else {"attempts": 0}
                prev["attempts"] = int(prev.get("attempts", 0)) + 1
                prev["last_failure"] = receipt
                conn.execute(
                    "UPDATE disinfection_batches SET receipt_payload=?,updated_at=? WHERE id=?",
                    (canonical_json(prev), now_iso(), batch_id),
                )
                self.append_audit(
                    conn, row["item_id"], "batch_receipt_failed", actor, role,
                    {"batch_id": batch_id, "attempt": prev["attempts"]},
                )
            conn.execute("COMMIT")
            result = self.get_batch(batch_id)
            result["deduplicated"] = False
            return result
        except Exception:
            try:
                conn.execute("ROLLBACK")
            except sqlite3.Error:
                pass
            raise
        finally:
            conn.close()

    def record_retest(self, item_id, zone_id, concentration, limit, actor, role):
        conn = self.connect()
        try:
            conn.execute("BEGIN IMMEDIATE")
            state = conn.execute(
                "SELECT * FROM zone_states WHERE zone_id=? AND item_id=?", (zone_id, item_id)
            ).fetchone()
            if state is None:
                raise NotFoundError("zone_state_not_found", "该区域没有待复检的消毒记录")
            if state["status"] == ZONE_RESTORED:
                raise DomainError("zone_restored", "该区域已恢复，无需复检", 409)
            new_status = ZONE_RESTORED if float(concentration) <= float(limit) else ZONE_AFFECTED
            conn.execute(
                "UPDATE zone_states SET status=?,updated_at=? WHERE zone_id=? AND item_id=?",
                (new_status, now_iso(), zone_id, item_id),
            )
            self.append_audit(
                conn, item_id, "zone_retest", actor, role,
                {"zone_id": zone_id, "concentration": float(concentration), "result": new_status},
            )
            conn.execute("COMMIT")
            return {"zone_id": zone_id, "item_id": item_id, "status": new_status}
        except Exception:
            try:
                conn.execute("ROLLBACK")
            except sqlite3.Error:
                pass
            raise
        finally:
            conn.close()

    def cascade_source_update(self, conn, item_id, zone_id, source_type, external_id, actor, role):
        """覆盖区域污染来源更新：整批失效，下游已恢复区域一并退回待复检。"""
        if not zone_id:
            return {"invalidated_batch_ids": [], "reset_zone_ids": []}
        # 同一区域可能同时挂在迁移补齐的单区支路和正式拓扑支路上：全部纳入后合并下游
        origin_codes = []
        for branch in conn.execute("SELECT code,zone_ids FROM branches").fetchall():
            if zone_id in json.loads(branch["zone_ids"]):
                origin_codes.append(branch["code"])
        if not origin_codes:
            return {"invalidated_batch_ids": [], "reset_zone_ids": []}
        related_codes = set(origin_codes)
        for code in origin_codes:
            related_codes |= self._downstream_codes(conn, code)
        cascade_zones = self._branch_zones(conn, related_codes)
        batches = conn.execute(
            "SELECT * FROM disinfection_batches WHERE item_id=? AND status IN (?,?)",
            (item_id, BATCH_ACTIVE, BATCH_COMPLETED),
        ).fetchall()
        invalidated, released_units, reset = [], set(), []
        for batch in batches:
            covered = self._branch_zones(conn, {batch["branch_code"]})
            if zone_id in covered or covered & cascade_zones:
                conn.execute(
                    "UPDATE disinfection_batches SET status=?,updated_at=? WHERE id=?",
                    (BATCH_INVALIDATED, now_iso(), batch["id"]),
                )
                invalidated.append(batch["id"])
                released_units.add(batch["unit_code"])
                self.append_audit(
                    conn, item_id, "batch_invalidated", actor, role,
                    {"batch_id": batch["id"], "zone_id": zone_id, "source": "%s:%s" % (source_type, external_id)},
                )
        # 新污染区本身重新受污染；其余级联区域（含下游已恢复区）退回待复检，不能只回退新污染区
        for current_zone in sorted(cascade_zones):
            state = conn.execute(
                "SELECT status FROM zone_states WHERE zone_id=? AND item_id=?", (current_zone, item_id)
            ).fetchone()
            new_status = ZONE_AFFECTED if current_zone == zone_id else ZONE_PENDING_RETEST
            if state is None:
                conn.execute(
                    "INSERT INTO zone_states(zone_id,item_id,status,batch_id,updated_at) VALUES(?,?,?,NULL,?)",
                    (current_zone, item_id, new_status, now_iso()),
                )
                reset.append(current_zone)
            elif current_zone == zone_id or state["status"] == ZONE_RESTORED:
                conn.execute(
                    "UPDATE zone_states SET status=?,updated_at=? WHERE zone_id=? AND item_id=?",
                    (new_status, now_iso(), current_zone, item_id),
                )
                reset.append(current_zone)
        waiting_units = [
            r["unit_code"]
            for r in conn.execute(
                "SELECT DISTINCT unit_code FROM disinfection_batches WHERE status=? ORDER BY id",
                (BATCH_QUEUED,),
            ).fetchall()
        ]
        unit_order = list(released_units) + [code for code in waiting_units if code not in released_units]
        promoted = self._promote_queued(conn, unit_order)
        return {"invalidated_batch_ids": invalidated, "reset_zone_ids": reset, "promoted_batch_ids": promoted}

    def zone_states(self, item_id=None):
        conn = self.connect()
        try:
            if item_id:
                rows = conn.execute(
                    "SELECT * FROM zone_states WHERE item_id=? ORDER BY zone_id", (item_id,)
                ).fetchall()
            else:
                rows = conn.execute("SELECT * FROM zone_states ORDER BY zone_id,item_id").fetchall()
            return [dict(row) for row in rows]
        finally:
            conn.close()

    def state_summary(self):
        conn = self.connect()
        try:
            counts = {}
            for row in conn.execute("SELECT status, COUNT(*) AS total FROM items GROUP BY status").fetchall():
                counts[row["status"]] = row["total"]
            batch_counts = {}
            for row in conn.execute("SELECT status, COUNT(*) AS total FROM disinfection_batches GROUP BY status").fetchall():
                batch_counts[row["status"]] = row["total"]
            return {
                "counts": counts,
                "batch_counts": batch_counts,
                "items": self.list_items(),
            }
        finally:
            conn.close()
