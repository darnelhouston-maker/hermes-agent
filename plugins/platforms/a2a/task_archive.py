"""Protected A2A receipts. No eviction, inferred outcomes, or task execution."""

from __future__ import annotations

from concurrent.futures import Future
from contextlib import contextmanager
import hashlib
import json
import os
from pathlib import Path
import sqlite3
import stat
import threading
import time
import uuid

from . import protocol

_OWNER = f"{os.getpid()}-{uuid.uuid4().hex}"
_INTERRUPTED = "Accepted task owner unavailable; outcome unknown. Do not resend. Original-owner reconciliation required."


def archive_path() -> Path:
    from hermes_constants import get_hermes_home
    return Path(get_hermes_home()) / "a2a_tasks" / "receipts.sqlite3"


class TaskArchive(protocol.TaskStore):
    """SQLite-authoritative TaskStore; in-memory Futures never own retained proof."""

    def __init__(self, path: Path | None = None, *, owner: str = _OWNER):
        super().__init__()
        self.path = path or archive_path()
        self.owner = owner
        self._lock = threading.RLock()
        self.path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
        if self.path.parent.is_symlink():
            raise ValueError("unsafe A2A receipt directory")
        os.chmod(self.path.parent, 0o700)
        fd = os.open(self.path, os.O_CREAT | os.O_RDWR | getattr(os, "O_NOFOLLOW", 0), 0o600)
        try:
            if not stat.S_ISREG(os.fstat(fd).st_mode) or os.fstat(fd).st_nlink != 1:
                raise ValueError("unsafe A2A receipt file")
            os.fchmod(fd, 0o600)
        finally:
            os.close(fd)
        with self._transaction() as db:
            db.execute("CREATE TABLE IF NOT EXISTS tasks (id TEXT PRIMARY KEY, record TEXT NOT NULL)")
            db.execute("CREATE TABLE IF NOT EXISTS requests (key TEXT PRIMARY KEY, fingerprint TEXT NOT NULL, task TEXT NOT NULL REFERENCES tasks(id))")
            db.execute("CREATE TABLE IF NOT EXISTS aliases (peer TEXT, endpoint TEXT, alias TEXT, task TEXT, context TEXT, PRIMARY KEY(peer, endpoint, alias))")

    @contextmanager
    def _transaction(self):
        with self._lock, sqlite3.connect(self.path, timeout=10) as db:
            db.execute("PRAGMA synchronous=FULL")
            db.execute("PRAGMA foreign_keys=ON")
            db.execute("BEGIN IMMEDIATE")
            yield db

    @staticmethod
    def _load(db, task_id):
        row = db.execute("SELECT record FROM tasks WHERE id=?", (task_id,)).fetchone()
        return json.loads(row[0]) if row else None

    @staticmethod
    def _save(db, rec):
        db.execute("INSERT OR REPLACE INTO tasks VALUES (?, ?)", (rec["task_id"], json.dumps(rec)))

    def _view(self, rec):
        if rec and rec["state"] not in protocol.TERMINAL_STATES and rec.get("owner") != self.owner:
            rec = {**rec, "state": protocol.STATE_INPUT_REQUIRED, "reply": _INTERRUPTED,
                   "owner_reconciliation_required": True}
        return rec

    def accept(self, task_id, context_id, peer, agent_slug="", tenant="", *, request_id="", text=""):
        """Commit acceptance and its exact message binding before dispatch. Never replay."""
        key = hashlib.sha256(json.dumps([peer, agent_slug, tenant, request_id]).encode()).hexdigest()
        fingerprint = hashlib.sha256(json.dumps([context_id, text]).encode()).hexdigest()
        with self._transaction() as db:
            if request_id:
                prior = db.execute("SELECT fingerprint, task FROM requests WHERE key=?", (key,)).fetchone()
                if prior:
                    if prior[0] != fingerprint:
                        raise ValueError("A2A message identity conflicts with prior acceptance; do not resend")
                    return self._view(self._load(db, prior[1])), False
            prior = self._load(db, task_id)
            if prior:
                if (prior["context_id"], prior["peer"], prior["agent_slug"], prior["tenant"]) != (context_id, peer, agent_slug, tenant):
                    raise ValueError("A2A task identity conflict")
                return self._view(prior), False
            rec = dict(task_id=task_id, context_id=context_id, peer=peer, agent_slug=agent_slug,
                       tenant=tenant, state=protocol.STATE_SUBMITTED, reply="", created_at=time.time(),
                       created_iso=protocol.now_iso(), push_url="", push_config_id="", owner=self.owner)
            self._save(db, rec)
            if request_id:
                db.execute("INSERT INTO requests VALUES (?, ?, ?)", (key, fingerprint, task_id))
        return rec, True

    def create(self, task_id, context_id, peer, agent_slug="", tenant=""):
        return self.accept(task_id, context_id, peer, agent_slug, tenant)[0]

    def prior_request(self, context_id, peer, scope, request_id, text):
        if not request_id:
            return None
        key = hashlib.sha256(json.dumps([peer, *scope, request_id]).encode()).hexdigest()
        fingerprint = hashlib.sha256(json.dumps([context_id, text]).encode()).hexdigest()
        with self._transaction() as db:
            row = db.execute("SELECT fingerprint, task FROM requests WHERE key=?", (key,)).fetchone()
            if row and row[0] != fingerprint:
                raise ValueError("A2A message identity conflicts with prior acceptance; do not resend")
            return self._view(self._load(db, row[1])) if row else None

    def get(self, task_id, agent_slug="", tenant=""):
        with self._transaction() as db:
            rec = self._load(db, task_id)
            return self._view(rec) if rec and self._in_scope(rec, agent_slug, tenant) else None

    def _update(self, task_id, update, *, scope=("", ""), terminal=False):
        with self._transaction() as db:
            rec = self._load(db, task_id)
            if not rec or not self._in_scope(rec, *scope):
                return None
            if terminal and (rec["state"] in protocol.TERMINAL_STATES or rec.get("owner") != self.owner):
                return None
            update(rec)
            self._save(db, rec)
            return rec

    def set_state(self, task_id, state):
        self._update(task_id, lambda r: r.update(state=state), terminal=True)

    def complete(self, task_id, state, reply=""):
        if state not in protocol.TERMINAL_STATES | {protocol.STATE_INPUT_REQUIRED}:
            raise ValueError("invalid A2A terminal state")
        with self._lock:
            rec = self._update(task_id, lambda r: r.update(state=state, reply=reply, completed_at=time.time()), terminal=True)
            watchers = self._watchers.pop(task_id, []) if rec else []
        for future in watchers:
            if not future.done():
                future.set_result((state, reply))
        return rec

    def watch(self, task_id, agent_slug="", tenant=""):
        with self._lock:
            rec = self.get(task_id, agent_slug, tenant)
            if not rec:
                return None
            future = Future()
            if rec["state"] in protocol.TERMINAL_STATES | {protocol.STATE_INPUT_REQUIRED}:
                future.set_result((rec["state"], rec.get("reply", "")))
            else:
                self._watchers.setdefault(task_id, []).append(future)
            return future

    def list(self, context_id="", state="", page_size=50, offset=0, agent_slug="", tenant="", with_total=False):
        size = max(1, min(int(page_size or 50), 100))
        clauses, args = [], []
        for field, value in (("context_id", context_id), ("agent_slug", agent_slug), ("tenant", tenant)):
            if value:
                clauses.append(f"json_extract(record, '$.{field}') = ?")
                args.append(value)
        if state:
            terminal = ",".join("?" for _ in protocol.TERMINAL_STATES)
            clauses.append(f"(CASE WHEN json_extract(record, '$.owner') != ? AND json_extract(record, '$.state') NOT IN ({terminal}) THEN ? ELSE json_extract(record, '$.state') END) = ?")
            args.extend((self.owner, *protocol.TERMINAL_STATES, protocol.STATE_INPUT_REQUIRED, state))
        where = " WHERE " + " AND ".join(clauses) if clauses else ""
        with self._transaction() as db:
            total = db.execute("SELECT count(*) FROM tasks" + where, args).fetchone()[0]
            page = [self._view(json.loads(row[0])) for row in db.execute(
                "SELECT record FROM tasks" + where + " ORDER BY rowid DESC LIMIT ? OFFSET ?", (*args, size, max(0, offset)))]
        next_offset = offset + size if offset + size < total else 0
        return (page, next_offset, total) if with_total else (page, next_offset)

    def fail_orphans(self, timeout_seconds=300, *, exclude=None):
        # Lost ownership is uncertain, never evidence of failure or permission to retry.
        return []

    def set_push_config(self, task_id, url, agent_slug="", tenant=""):
        rec = self._update(task_id, lambda r: r.update(push_url=url, push_config_id="cfg-" + uuid.uuid4().hex[:12]), scope=(agent_slug, tenant))
        return self._push_config_view(rec) if rec else None

    def get_push_config(self, task_id, config_id="", agent_slug="", tenant=""):
        rec = self.get(task_id, agent_slug, tenant)
        return self._push_config_view(rec) if rec and rec.get("push_url") and (not config_id or rec.get("push_config_id") == config_id) else None

    def list_push_configs(self, task_id, agent_slug="", tenant=""):
        cfg = self.get_push_config(task_id, "", agent_slug, tenant)
        return [cfg] if cfg else []

    def delete_push_config(self, task_id, config_id="", agent_slug="", tenant=""):
        with self._lock:
            if not self.get_push_config(task_id, config_id, agent_slug, tenant):
                return False
            self._update(task_id, lambda r: r.update(push_url="", push_config_id=""), scope=(agent_slug, tenant))
            return True

    def pop_push_url(self, task_id):
        prior = []
        self._update(task_id, lambda r: (prior.append(r.get("push_url", "")), r.update(push_url="")))
        return prior[0] if prior else ""

    def bind_alias(self, peer, endpoint, alias, task_id, context_id):
        """Only the client calls this after observing a validated receiver receipt."""
        with self._transaction() as db:
            prior = db.execute("SELECT task, context FROM aliases WHERE peer=? AND endpoint=? AND alias=?", (peer, endpoint, alias)).fetchone()
            if prior and prior != (task_id, context_id):
                raise ValueError("A2A receipt alias conflict")
            db.execute("INSERT OR IGNORE INTO aliases VALUES (?, ?, ?, ?, ?)", (peer, endpoint, alias, task_id, context_id))

    def resolve_alias(self, peer, endpoint, alias):
        with self._transaction() as db:
            return db.execute("SELECT task, context FROM aliases WHERE peer=? AND endpoint=? AND alias=?", (peer, endpoint, alias)).fetchone()

    def health(self):
        with self._transaction() as db:
            states = dict(db.execute("SELECT json_extract(record, '$.state'), count(*) FROM tasks GROUP BY 1"))
            aliases = db.execute("SELECT count(*) FROM aliases").fetchone()[0]
        return {"storage": "sqlite", "retention": "no_automatic_eviction", "states": states, "alias_bindings": aliases}


def receipt_inventory(path: Path, task_ids=()):
    """Read-only aggregate canary; never queries result, context, or peer columns."""
    if not path.is_file():
        return {"archive_present": False, "lookup_durability_verified": False, "mutations": 0}
    with sqlite3.connect(path.resolve().as_uri() + "?mode=ro", uri=True) as db:
        tables = {row[0] for row in db.execute("SELECT name FROM sqlite_master WHERE type='table'")}
        if not {"tasks", "requests", "aliases"} <= tables:
            raise ValueError("A2A receipt schema unavailable")
        states = dict(db.execute("SELECT json_extract(record, '$.state'), count(*) FROM tasks GROUP BY 1"))
        return {"archive_present": True, "integrity_ok": db.execute("PRAGMA quick_check(1)").fetchone() == ("ok",),
                "states": states, "request_bindings": db.execute("SELECT count(*) FROM requests").fetchone()[0],
                "alias_bindings": db.execute("SELECT count(*) FROM aliases").fetchone()[0],
                "requested_ids_present": sum(bool(db.execute("SELECT 1 FROM tasks WHERE id=?", (tid,)).fetchone()) for tid in task_ids),
                "mutations": 0, "result_reads": 0, "submissions": 0}


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Read-only, payload-free A2A receipt inventory")
    parser.add_argument("--path", type=Path, default=None)
    args = parser.parse_args()
    try:
        print(json.dumps(receipt_inventory(args.path or archive_path()), sort_keys=True))
    except (OSError, ValueError, sqlite3.DatabaseError) as exc:
        print(json.dumps({"healthy": False, "error_class": type(exc).__name__, "submissions": 0}))
        raise SystemExit(1)
