"""Fabricated receipt persistence; no model, network, or business task runs."""

from concurrent.futures import ThreadPoolExecutor
import os
from pathlib import Path
import sqlite3
import subprocess
import sys

import pytest

from plugins.platforms.a2a import protocol as p
from plugins.platforms.a2a.task_archive import TaskArchive, receipt_inventory


@pytest.fixture
def store(tmp_path):
    return TaskArchive(tmp_path / "receipts" / "tasks.sqlite3", owner="owner-1")


def test_terminal_result_survives_new_process(store):
    store.create("task-synthetic", "ctx-synthetic", "synthetic-peer")
    store.complete("task-synthetic", p.STATE_COMPLETED, "synthetic-result")
    code = """
import sys
from pathlib import Path
from plugins.platforms.a2a.task_archive import TaskArchive
from plugins.platforms.a2a import protocol as p
s = TaskArchive(Path(sys.argv[1]), owner='new-process')
r = s.get('task-synthetic')
assert r['state'] == p.STATE_COMPLETED and r['reply'] == 'synthetic-result'
assert s.watch('task-synthetic').result()[1] == 'synthetic-result'
print('retained-terminal-proof-ok')
"""
    result = subprocess.run([sys.executable, "-c", code, str(store.path)], capture_output=True, text=True, check=True)
    assert result.stdout.strip() == "retained-terminal-proof-ok"


def test_terminal_retention_exceeds_old_500_record_cap(store):
    for index in range(503):
        store.create(f"t-{index}", "ctx", "peer")
        store.complete(f"t-{index}", p.STATE_COMPLETED, f"result-{index}")
    assert TaskArchive(store.path).get("t-0")["reply"] == "result-0"
    assert store.health()["states"][p.STATE_COMPLETED] == 503


def test_interrupted_acceptance_requires_owner_reconciliation_without_replay(store):
    store.accept("t-1", "ctx", "peer", request_id="m-1", text="fabricated")
    other = TaskArchive(store.path, owner="after-restart")
    rec, fresh = other.accept("t-new", "ctx", "peer", request_id="m-1", text="fabricated")
    assert not fresh and rec["task_id"] == "t-1"
    assert rec["owner_reconciliation_required"]
    assert rec["state"] == p.STATE_INPUT_REQUIRED and "Do not resend" in rec["reply"]
    assert other.watch("t-1").result()[0] == p.STATE_INPUT_REQUIRED
    assert other.complete("t-1", p.STATE_COMPLETED, "invented") is None
    assert other.fail_orphans(0) == []
    assert store.get("t-1")["state"] == p.STATE_SUBMITTED


def test_same_message_concurrent_acceptance_has_one_owner(store):
    def accept(index):
        other = TaskArchive(store.path, owner="owner-1")
        return other.accept(f"task-{index}", "ctx", "peer", request_id="m-1", text="fabricated")
    with ThreadPoolExecutor(max_workers=6) as pool:
        results = list(pool.map(accept, range(24)))
    assert sum(fresh for _, fresh in results) == 1
    assert len({r["task_id"] for r, _ in results}) == 1


@pytest.mark.parametrize("context,text", [("ctx-other", "fabricated"), ("ctx", "changed")])
def test_same_message_cannot_change_context_or_content(store, context, text):
    store.accept("t-1", "ctx", "peer", request_id="m-1", text="fabricated")
    with pytest.raises(ValueError, match="identity conflicts"):
        store.accept("t-2", context, "peer", request_id="m-1", text=text)
    assert store.list(with_total=True)[2] == 1


def test_terminal_proof_is_immutable_under_late_completion_and_state_write(store):
    store.create("t-1", "ctx", "peer")
    store.complete("t-1", p.STATE_COMPLETED, "authentic")
    assert store.complete("t-1", p.STATE_FAILED, "late") is None
    store.set_state("t-1", p.STATE_WORKING)
    assert store.get("t-1")["reply"] == "authentic"


def test_scope_and_peer_request_identity_remain_isolated(store):
    store.accept("t-1", "ctx", "peer-1", "agent-1", "tenant-1", request_id="m", text="synthetic")
    assert store.get("t-1", "agent-other", "tenant-1") is None
    assert store.get("t-1", "agent-1", "tenant-other") is None
    assert store.prior_request("ctx", "peer-other", ("agent-1", "tenant-1"), "m", "synthetic") is None
    assert store.list(agent_slug="other")[0] == []


def test_alias_binding_survives_restart_and_is_endpoint_and_peer_scoped(store):
    store.bind_alias("peer", "endpoint-a", "audit-id", "owner-id", "ctx")
    other = TaskArchive(store.path)
    assert other.resolve_alias("peer", "endpoint-a", "audit-id") == ("owner-id", "ctx")
    assert other.resolve_alias("other", "endpoint-a", "audit-id") is None
    assert other.resolve_alias("peer", "endpoint-b", "audit-id") is None
    assert other.resolve_alias("peer", "endpoint-a", "unknown") is None
    with pytest.raises(ValueError, match="alias conflict"):
        other.bind_alias("peer", "endpoint-a", "audit-id", "different", "ctx")


def test_push_configuration_and_waiter_preserve_existing_contract(store):
    store.create("t-1", "ctx", "peer", "agent", "tenant")
    future = store.watch("t-1", "agent", "tenant")
    config = store.set_push_config("t-1", "https://synthetic.invalid/hook", "agent", "tenant")
    assert store.get_push_config("t-1", config["configId"], "agent", "tenant") == config
    assert store.get_push_config("t-1", "wrong", "agent", "tenant") is None
    assert store.list_push_configs("t-1", "other", "tenant") == []
    assert not store.delete_push_config("t-1", "wrong", "agent", "tenant")
    assert store.pop_push_url("t-1") == "https://synthetic.invalid/hook"
    assert store.pop_push_url("t-1") == ""
    store.complete("t-1", p.STATE_COMPLETED, "finished")
    assert future.result() == (p.STATE_COMPLETED, "finished")


def test_archive_permissions_and_metadata_health_do_not_disclose_results(store):
    store.create("t-1", "ctx", "peer")
    store.complete("t-1", p.STATE_COMPLETED, "DO-NOT-REPORT-SYNTHETIC")
    assert os.stat(store.path).st_mode & 0o777 == 0o600
    assert os.stat(store.path.parent).st_mode & 0o777 == 0o700
    assert "DO-NOT-REPORT" not in str(store.health())
    with sqlite3.connect(store.path) as db:
        assert db.execute("PRAGMA integrity_check").fetchone()[0] == "ok"


def test_corrupt_archive_fails_closed_and_is_not_replaced(tmp_path):
    path = tmp_path / "store" / "tasks.sqlite3"
    path.parent.mkdir()
    path.write_bytes(b"fabricated-corruption")
    with pytest.raises(sqlite3.DatabaseError):
        TaskArchive(path)
    assert path.read_bytes() == b"fabricated-corruption"


def test_symlink_archive_is_refused_without_touching_target(tmp_path):
    target = tmp_path / "target"
    target.write_text("unchanged")
    path = tmp_path / "store" / "tasks.sqlite3"
    path.parent.mkdir()
    path.symlink_to(target)
    with pytest.raises(OSError):
        TaskArchive(path)
    assert target.read_text() == "unchanged"


def test_scoped_pages_remain_complete_without_duplicates(store):
    for index in range(7):
        store.create(f"t-{index}", "ctx", "peer", "agent", "tenant")
    first, offset, total = store.list(page_size=3, agent_slug="agent", with_total=True)
    second, offset = store.list(page_size=3, offset=offset, agent_slug="agent")
    third, offset = store.list(page_size=3, offset=offset, agent_slug="agent")
    assert total == 7 and offset == 0
    assert len({r["task_id"] for r in first + second + third}) == 7


def test_metadata_inventory_does_not_create_missing_archive(tmp_path):
    path = tmp_path / "absent" / "tasks.sqlite3"
    assert receipt_inventory(path)["archive_present"] is False
    assert not path.parent.exists()


def test_bound_acceptance_terminal_inventory_and_reads_are_noop(store):
    store.accept("task-one", "ctx", "peer", request_id="message-one", text="fabricated")
    store.complete("task-one", p.STATE_COMPLETED, "SYNTHETIC-DO-NOT-REPORT")
    before = store.path.read_bytes()
    report = receipt_inventory(store.path, ("task-one", "not-found"))
    assert report["requested_ids_present"] == 1 and report["integrity_ok"]
    assert report["request_bindings"] == 1 and report["result_reads"] == 0
    assert "SYNTHETIC" not in str(report)
    assert store.path.read_bytes() == before
