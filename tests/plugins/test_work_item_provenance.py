from __future__ import annotations

import importlib.util
import json
import sqlite3
import tempfile
import threading
import unittest
from pathlib import Path


HERE = Path(__file__).resolve()
SOURCE = HERE.parents[2] / "plugins/platforms/a2a/work_items.py"
if not SOURCE.exists():
    SOURCE = HERE.parents[1] / "work_items.py"
SPEC = importlib.util.spec_from_file_location("work_items_provenance_test", SOURCE)
work_items = importlib.util.module_from_spec(SPEC)
assert SPEC.loader is not None
SPEC.loader.exec_module(work_items)


class WorkItemProvenanceTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        root = Path(self.tmp.name)
        self.db = root / "work-items.sqlite3"
        self.dispatch = root / "dispatch.jsonl"
        self.config = {
            "a2a": {"work_items": {
                "role": "hub", "actor": "watson", "database": str(self.db),
                "dispatch_ledger": str(self.dispatch),
            }},
            "a2a_agents": {"mac2": {}},
        }
        work_items._INITIALIZED_PATHS.discard(str(self.db))

    def payload(self, key="synthetic-key-0001"):
        return {
            "work_item_id": "SYNTHETIC-HANDOFF",
            "status": "blocked",
            "note": "metadata only",
            "idempotency_key": key,
            "provenance": {
                "originating_principal": "unavailable",
                "transport_authenticated_peer": "vps",
                "intake_context": "ctx-synthetic",
                "operational_owner": "producer-maintainer",
            },
            "gates": {
                "account_unlock_state": "verified",
                "password_reset_state": "blocked",
                "prepared_task_artifact_state": "preserved",
            },
        }

    def test_round_trip_and_exact_idempotency(self):
        first = work_items.append(self.payload(), actor="watson", config=self.config)
        second = work_items.append(self.payload(), actor="watson", config=self.config)
        self.assertTrue(first["appended"])
        self.assertTrue(second["duplicate"])
        result = work_items.read({"work_item_id": "SYNTHETIC-HANDOFF"}, actor="watson", config=self.config)
        self.assertEqual(result["entries"][0]["provenance"]["originating_principal"], "unavailable")
        self.assertEqual(result["entries"][0]["gates"]["password_reset_state"], "blocked")

    def test_same_key_with_changed_gate_conflicts(self):
        work_items.append(self.payload(), actor="watson", config=self.config)
        changed = self.payload()
        changed["gates"]["password_reset_state"] = "complete"
        with self.assertRaisesRegex(work_items.WorkItemError, "different content"):
            work_items.append(changed, actor="watson", config=self.config)

    def test_invalid_gate_and_non_string_provenance_fail_closed(self):
        bad_gate = self.payload("synthetic-key-0002")
        bad_gate["gates"]["password_reset_state"] = "done-ish"
        with self.assertRaisesRegex(work_items.WorkItemError, "unsupported state"):
            work_items.append(bad_gate, actor="watson", config=self.config)
        bad_provenance = self.payload("synthetic-key-0003")
        bad_provenance["provenance"]["originating_principal"] = ["vps"]
        with self.assertRaisesRegex(work_items.WorkItemError, "unsupported value"):
            work_items.append(bad_provenance, actor="watson", config=self.config)

    def test_old_database_is_migrated_without_losing_entries(self):
        self.db.parent.mkdir(parents=True, exist_ok=True)
        conn = sqlite3.connect(self.db)
        conn.executescript("""
            CREATE TABLE work_items (
                work_item_id TEXT PRIMARY KEY COLLATE BINARY,
                created_at TEXT NOT NULL, updated_at TEXT NOT NULL, created_by TEXT NOT NULL
            );
            CREATE TABLE work_item_entries (
                work_item_id TEXT NOT NULL, sequence INTEGER NOT NULL, actor TEXT NOT NULL,
                status TEXT NOT NULL, note TEXT NOT NULL, idempotency_key TEXT NOT NULL,
                created_at TEXT NOT NULL, PRIMARY KEY (work_item_id, sequence),
                UNIQUE (work_item_id, actor, idempotency_key)
            );
            CREATE TABLE work_item_audit (
                audit_id INTEGER PRIMARY KEY AUTOINCREMENT, created_at TEXT NOT NULL,
                operation TEXT NOT NULL, actor TEXT NOT NULL, work_item_id TEXT NOT NULL,
                outcome TEXT NOT NULL, sequence INTEGER
            );
            INSERT INTO work_items VALUES ('SYNTHETIC-HANDOFF','t','t','watson');
            INSERT INTO work_item_entries VALUES ('SYNTHETIC-HANDOFF',1,'watson','open','legacy','legacy-key-0001','t');
        """)
        conn.commit()
        conn.close()
        result = work_items.read({"work_item_id": "SYNTHETIC-HANDOFF"}, actor="watson", config=self.config)
        self.assertEqual(len(result["entries"]), 1)
        self.assertEqual(result["entries"][0]["provenance"], {})
        self.assertEqual(result["entries"][0]["gates"], {})

    def test_dispatch_fallback_preserves_typed_metadata_without_report_body(self):
        contract = {
            "provenance": {
                "originating_principal": "unavailable", "transport_authenticated_peer": "vps",
                "intake_context": "ctx-synthetic", "operational_owner": "producer-maintainer",
            },
            "gates": {"account_unlock_state": "verified", "password_reset_state": "blocked"},
        }
        row = {
            "id": "SYNTHETIC-HANDOFF", "status": "blocked", "reported_by": "watson",
            "ts": "2026-10-14T17:00:00Z", "proof_preview": "must not surface",
            "handoff_contract": contract,
        }
        self.dispatch.write_text(json.dumps(row) + "\n")
        result = work_items.read({"work_item_id": "SYNTHETIC-HANDOFF"}, actor="watson", config=self.config)
        self.assertEqual(result["entries"][0]["gates"]["password_reset_state"], "blocked")
        self.assertNotIn("must not surface", json.dumps(result))

    def test_concurrent_distinct_appends_are_serialized(self):
        errors = []

        def append(index):
            try:
                work_items.append(self.payload(f"synthetic-key-{index:04d}"), actor="watson", config=self.config)
            except Exception as exc:  # pragma: no cover - asserted below
                errors.append(exc)

        threads = [threading.Thread(target=append, args=(index,)) for index in range(8)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()
        self.assertEqual(errors, [])
        result = work_items.read({"work_item_id": "SYNTHETIC-HANDOFF", "limit": 20}, actor="watson", config=self.config)
        self.assertEqual(len(result["entries"]), 8)
        self.assertEqual([entry["sequence"] for entry in result["entries"]], list(range(1, 9)))


if __name__ == "__main__":
    unittest.main()
