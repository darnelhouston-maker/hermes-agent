"""Durable async-delegation registry isolation regression coverage."""

from __future__ import annotations

import sqlite3
import threading
import time

import pytest

from tools import async_delegation as ad


def _legacy_schema(conn: sqlite3.Connection) -> None:
    conn.execute(
        """CREATE TABLE async_delegations (
            delegation_id TEXT PRIMARY KEY,
            origin_session TEXT NOT NULL,
            origin_ui_session_id TEXT NOT NULL DEFAULT '',
            parent_session_id TEXT,
            state TEXT NOT NULL,
            dispatched_at REAL NOT NULL,
            completed_at REAL,
            updated_at REAL NOT NULL,
            event_json TEXT,
            result_json TEXT,
            delivery_state TEXT NOT NULL DEFAULT 'pending',
            delivery_attempts INTEGER NOT NULL DEFAULT 0,
            delivered_at REAL,
            owner_pid INTEGER,
            owner_started_at INTEGER,
            task_json TEXT,
            delivery_claim TEXT,
            delivery_claimed_at REAL,
            origin_session_id TEXT NOT NULL DEFAULT ''
        )"""
    )


@pytest.fixture
def isolated_registry(tmp_path, monkeypatch):
    registry = tmp_path / "async_delegations.db"
    legacy = tmp_path / "state.db"
    monkeypatch.setattr(ad, "_db_path", lambda: registry)
    monkeypatch.setattr(ad, "_legacy_db_path", lambda: legacy)
    ad._reset_for_tests()
    yield registry, legacy
    ad._reset_for_tests()


def test_registry_migrates_once_and_leaves_legacy_unchanged(isolated_registry):
    registry, legacy = isolated_registry
    now = time.time()
    with sqlite3.connect(legacy) as conn:
        _legacy_schema(conn)
        conn.execute(
            """INSERT INTO async_delegations
               (delegation_id, origin_session, state, dispatched_at, updated_at,
                event_json, result_json, delivery_state, origin_session_id)
               VALUES ('deleg-old', 'origin', 'completed', ?, ?, '{}', '{}',
                       'pending', 'origin-id')""",
            (now - 2, now - 1),
        )

    with ad._connect() as conn:
        migrated = conn.execute(
            "SELECT delegation_id, state, delivery_state "
            "FROM async_delegations"
        ).fetchall()
        marker = conn.execute(
            "SELECT value FROM async_delegation_store_meta WHERE key=?",
            (ad._LEGACY_MIGRATION_KEY,),
        ).fetchone()[0]

    assert registry.exists()
    assert migrated == [("deleg-old", "completed", "pending")]
    assert marker == "copied:1"

    with ad._transaction() as conn:
        conn.execute(
            "UPDATE async_delegations SET delivery_state='delivered' "
            "WHERE delegation_id='deleg-old'"
        )
    with ad._connect() as conn:
        assert conn.execute(
            "SELECT COUNT(*) FROM async_delegations "
            "WHERE delegation_id='deleg-old'"
        ).fetchone()[0] == 1
    with sqlite3.connect(legacy) as conn:
        assert conn.execute(
            "SELECT delivery_state FROM async_delegations "
            "WHERE delegation_id='deleg-old'"
        ).fetchone()[0] == "pending"


def test_new_writes_never_create_registry_in_state_db(isolated_registry):
    registry, legacy = isolated_registry
    with sqlite3.connect(legacy) as conn:
        conn.execute("CREATE TABLE canonical_messages(id INTEGER PRIMARY KEY)")

    record = {
        "delegation_id": "deleg-new",
        "session_key": "origin",
        "origin_ui_session_id": "",
        "parent_session_id": "parent",
        "origin_session_id": "origin-id",
        "dispatched_at": time.time(),
        "goal": "synthetic",
    }
    ad._persist_dispatch(record)

    with sqlite3.connect(registry) as conn:
        assert conn.execute(
            "SELECT state FROM async_delegations "
            "WHERE delegation_id='deleg-new'"
        ).fetchone()[0] == "running"
    with sqlite3.connect(legacy) as conn:
        assert conn.execute(
            "SELECT 1 FROM sqlite_master "
            "WHERE type='table' AND name='async_delegations'"
        ).fetchone() is None


def test_concurrent_first_open_is_atomic_and_duplicate_free(isolated_registry):
    registry, legacy = isolated_registry
    with sqlite3.connect(legacy) as conn:
        _legacy_schema(conn)
        conn.execute(
            """INSERT INTO async_delegations
               (delegation_id, origin_session, state, dispatched_at, updated_at)
               VALUES ('deleg-race', 'origin', 'completed', 1, 2)"""
        )

    barrier = threading.Barrier(8)
    errors = []

    def open_registry():
        try:
            barrier.wait(timeout=5)
            with ad._connect() as conn:
                assert conn.execute("PRAGMA quick_check").fetchone()[0] == "ok"
        except Exception as exc:  # pragma: no cover - asserted below
            errors.append(exc)

    threads = [threading.Thread(target=open_registry) for _ in range(8)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(timeout=10)

    assert not errors
    with sqlite3.connect(registry) as conn:
        assert conn.execute(
            "SELECT COUNT(*) FROM async_delegations "
            "WHERE delegation_id='deleg-race'"
        ).fetchone()[0] == 1
        assert conn.execute(
            "SELECT COUNT(*) FROM async_delegation_store_meta "
            "WHERE key=?",
            (ad._LEGACY_MIGRATION_KEY,),
        ).fetchone()[0] == 1


def test_malformed_legacy_schema_fails_closed(isolated_registry):
    registry, legacy = isolated_registry
    with sqlite3.connect(legacy) as conn:
        conn.execute("CREATE TABLE async_delegations(not_the_id TEXT)")

    with pytest.raises(
        sqlite3.DatabaseError,
        match="legacy async delegation registry has no delegation_id",
    ):
        ad._connect()

    with sqlite3.connect(registry) as conn:
        assert conn.execute(
            "SELECT 1 FROM async_delegation_store_meta WHERE key=?",
            (ad._LEGACY_MIGRATION_KEY,),
        ).fetchone() is None

