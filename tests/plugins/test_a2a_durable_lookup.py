"""Installed-compatible A2A acceptance and exact lookup, synthetic and no-send."""

import hashlib
import json
import sqlite3
import urllib.error
from unittest.mock import MagicMock

import pytest

from gateway.config import PlatformConfig
from plugins.platforms.a2a import protocol as p, tools
from plugins.platforms.a2a.adapter import A2AAdapter
from plugins.platforms.a2a.task_archive import TaskArchive, archive_path, lookup_alias


def adapter():
    return A2AAdapter(PlatformConfig(enabled=True))


def test_receiver_gettask_retains_exact_result_across_adapter_reconstruction():
    first = adapter()
    scope = first._scope_for_agent(None)
    first.tasks.create("synthetic-task", "synthetic-context", "peer", *scope)
    first.tasks.complete("synthetic-task", p.STATE_COMPLETED, "synthetic-result")
    second = adapter()
    result = second._rpc_tasks_get("read-only", {"id": "synthetic-task"})["result"]
    assert result["id"] == "synthetic-task" and result["status"]["state"] == p.STATE_COMPLETED
    assert p.extract_text(result["artifacts"][0]) == "synthetic-result"


def test_receiver_metadata_only_lookup_omits_all_payload_locations():
    instance = adapter()
    instance.tasks.create("t", "ctx", "peer", *instance._scope_for_agent(None))
    instance.tasks.complete("t", p.STATE_INPUT_REQUIRED, "SYNTHETIC-PRIVATE")
    result = instance._rpc_tasks_get("read", {"id": "t", "metadataOnly": True})["result"]
    assert "SYNTHETIC-PRIVATE" not in json.dumps(result)
    assert not ({"artifacts", "history"} & result.keys())
    assert "message" not in result["status"]


def test_restarted_acceptance_is_not_dispatched_and_missing_receipt_is_not_invented(monkeypatch):
    first = adapter()
    scope = first._scope_for_agent(None)
    first.tasks.accept("t", "ctx", "peer", *scope, request_id="m", text="synthetic")
    second = adapter()
    second.tasks.owner = "new-process"
    dispatch = MagicMock(side_effect=AssertionError("must not dispatch"))
    monkeypatch.setattr(second, "handle_message", dispatch)
    params = {"message": p.text_message(p.ROLE_USER, "synthetic", context_id="ctx", message_id="m")}
    receipt, pending = second._prepare_task(params, "peer")
    assert pending is None and receipt["id"] == "t"
    assert receipt["status"]["state"] == p.STATE_INPUT_REQUIRED
    assert "Do not resend" in p.extract_text(receipt["status"]["message"])
    dispatch.assert_not_called()
    assert second._rpc_tasks_get("read", {"id": "not-found"})["error"]["code"] == p.ERR_TASK_NOT_FOUND


def test_no_dispatch_when_acceptance_cannot_commit(monkeypatch):
    instance = adapter()
    monkeypatch.setattr(instance.tasks, "accept", MagicMock(side_effect=sqlite3.OperationalError("synthetic full disk")))
    dispatch = MagicMock(side_effect=AssertionError("must not dispatch"))
    monkeypatch.setattr(instance, "handle_message", dispatch)
    params = {"message": p.text_message(p.ROLE_USER, "synthetic", context_id="ctx", message_id="m")}
    with pytest.raises(sqlite3.OperationalError):
        instance._prepare_task(params, "peer")
    dispatch.assert_not_called()


def test_client_records_observed_audit_to_owner_binding_and_resolves_only_that_peer(monkeypatch):
    calls = []
    monkeypatch.setattr(tools, "_fetch_card", lambda *_args: None)
    monkeypatch.setattr(tools.time, "sleep", lambda *_args: None)
    monkeypatch.setattr(tools, "_resolve_peer", lambda _agent: {"url": "http://localhost:9999", "timeout": 5})
    def post(_url, body, *_args):
        calls.append(body)
        return p.jsonrpc_result(body["id"], p.build_task("owner-task", "ctx", p.STATE_COMPLETED, "synthetic-result"))
    monkeypatch.setattr(tools, "_http_post_json", post)
    tools._send_task_uncontrolled("watson", {"url": "http://localhost:9999", "timeout": 5}, "fabricated", "ctx")
    alias = calls[0]["id"]
    endpoint = hashlib.sha256(json.dumps(["http://localhost:9999", ""]).encode()).hexdigest()
    assert lookup_alias("watson", endpoint, alias) == ("owner-task", "ctx")
    assert lookup_alias("other", endpoint, alias) is None
    output = tools.a2a_task_status({"agent": "watson", "task_id": alias})
    assert "synthetic-result" in output
    assert calls[-1]["method"] == "GetTask" and calls[-1]["params"]["id"] == "owner-task"
    assert sum(c["method"] == "SendMessage" for c in calls) == 1


@pytest.mark.parametrize("actual_id,actual_context", [("different", "ctx"), ("task", "wrong")])
def test_status_rejects_mismatched_result_identity_without_send(monkeypatch, actual_id, actual_context):
    monkeypatch.setattr(tools, "_fetch_card", lambda *_args: None)
    monkeypatch.setattr(tools, "_resolve_peer", lambda _agent: {"url": "http://localhost:9999", "timeout": 5})
    def rpc(_base, _card, _headers, method, _params, _timeout):
        assert method == "GetTask"
        return p.build_task(actual_id, actual_context, p.STATE_COMPLETED, "must-not-return")
    monkeypatch.setattr(tools, "_task_rpc", rpc)
    output = tools.a2a_task_status({"agent": "watson", "task_id": "task", "context_id": "ctx"})
    assert "identity mismatch" in output and "must-not-return" not in output


def test_status_missing_alias_is_read_only_and_does_not_create_archive(monkeypatch):
    assert not archive_path().exists()
    monkeypatch.setattr(tools, "_fetch_card", lambda *_args: None)
    monkeypatch.setattr(tools, "_resolve_peer", lambda _agent: {"url": "http://localhost:9999", "timeout": 5})
    def missing(_base, _card, _headers, method, _params, _timeout):
        assert method == "GetTask"
        raise ValueError("task not found; owner must reconcile")
    monkeypatch.setattr(tools, "_task_rpc", missing)
    assert "owner must reconcile" in tools.a2a_task_status({"agent": "watson", "task_id": "unknown"})
    assert not archive_path().exists()


def test_live_lookup_rate_limit_hotfix_is_preserved_without_submission(monkeypatch):
    calls = []
    monkeypatch.setattr(tools, "_fetch_card", lambda *_args: None)
    monkeypatch.setattr(tools, "_resolve_peer", lambda _agent: {"url": "http://localhost:9999", "timeout": 5})
    monkeypatch.setattr(tools.time, "sleep", lambda seconds: calls.append(seconds))
    def rpc(_base, _card, _headers, method, _params, _timeout):
        assert method == "GetTask"
        if not calls:
            raise urllib.error.HTTPError("http://localhost:9999", 429, "rate limited", {}, None)
        return p.build_task("task", "ctx", p.STATE_COMPLETED, "synthetic-result")
    monkeypatch.setattr(tools, "_task_rpc", rpc)
    assert "synthetic-result" in tools.a2a_task_status({"agent": "watson", "task_id": "task"})
    assert calls == [1.0]
