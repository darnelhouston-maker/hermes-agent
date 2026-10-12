"""Compatibility regressions for older fleet A2A releases."""

from __future__ import annotations

import time
from unittest.mock import MagicMock

from gateway.config import PlatformConfig
from plugins.platforms.a2a import protocol, tools, work_items
from plugins.platforms.a2a.adapter import A2AAdapter


def _adapter() -> A2AAdapter:
    return A2AAdapter(PlatformConfig(enabled=True))


def _pending(adapter: A2AAdapter, task_id: str = "task-long") -> dict:
    context_id = "ctx-long"
    rec = adapter.tasks.create(task_id, context_id, "peer")
    future = adapter._add_pending(task_id, context_id)
    adapter.tasks.set_state(task_id, protocol.STATE_WORKING)
    return {
        "task_id": task_id,
        "context_id": context_id,
        "peer": "peer",
        "future": future,
        "created_iso": rec["created_iso"],
        "started": time.time(),
    }


def test_nonblocking_send_returns_durable_working_receipt(monkeypatch):
    adapter = _adapter()
    pending = _pending(adapter)
    monkeypatch.setattr(adapter, "_prepare_task", lambda *args, **kwargs: (None, pending))
    starter = MagicMock()
    monkeypatch.setattr(adapter, "_start_pending_finalizer", starter)
    response = adapter._rpc_message_send(
        "rpc-1", {"configuration": {"blocking": False}}, "peer"
    )
    assert response["result"]["id"] == "task-long"
    assert response["result"]["status"]["state"] == protocol.STATE_WORKING
    starter.assert_called_once_with(pending)


def test_detached_finalizer_commits_late_result():
    adapter = _adapter()
    pending = _pending(adapter, "task-late")
    adapter._start_pending_finalizer(pending)
    pending["future"].set_result((protocol.STATE_COMPLETED, "late result"))
    deadline = time.time() + 2
    while time.time() < deadline:
        record = adapter.tasks.get("task-late")
        if record and record["state"] == protocol.STATE_COMPLETED:
            break
        time.sleep(0.01)
    record = adapter.tasks.get("task-late")
    assert record["state"] == protocol.STATE_COMPLETED
    assert record["reply"] == "late result"


def test_client_submits_nonblocking_then_polls(monkeypatch):
    calls = []
    monkeypatch.setattr(tools, "_fetch_card", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(tools.time, "sleep", lambda _seconds: None)

    def fake_post(_url, body, _headers, _timeout):
        calls.append(body["method"])
        if body["method"] == "SendMessage":
            assert body["params"]["configuration"]["blocking"] is False
            return protocol.jsonrpc_result(
                body["id"],
                protocol.build_task("task-poll", "ctx-poll", protocol.STATE_WORKING),
            )
        return protocol.jsonrpc_result(
            body["id"],
            protocol.build_task(
                "task-poll", "ctx-poll", protocol.STATE_COMPLETED, "finished"
            ),
        )

    monkeypatch.setattr(tools, "_http_post_json", fake_post)
    result = tools._send_task_uncontrolled(
        "peer",
        {"url": "http://localhost:9999", "auth": {}, "timeout": 5},
        "synthetic task",
        "ctx-poll",
        request_id="logical-request",
    )
    assert result == ("finished", "ctx-poll", protocol.STATE_COMPLETED, "task-poll")
    assert calls == ["SendMessage", "GetTask"]


def test_stable_request_id_is_deterministic_and_nonsecret():
    first = tools._stable_message_id("logical-request")
    second = tools._stable_message_id("logical-request")
    assert first == second
    assert first != "logical-request"
    assert len(first) == 64


def test_active_work_item_rewording_returns_original_task():
    adapter = _adapter()
    scope = adapter._scope_for_agent(None)
    adapter.tasks.create("task-existing", "ctx-original", "peer", *scope)
    adapter.tasks.set_state("task-existing", protocol.STATE_WORKING)
    assert adapter._claim_active_work_item(
        "peer", scope, "Shared work item: item-1\n\nfirst wording", "task-existing"
    ) == ""
    task, pending = adapter._prepare_task(
        {
            "message": protocol.text_message(
                protocol.ROLE_USER,
                "Shared work item: item-1\n\nsecond wording",
                context_id="ctx-retry",
            )
        },
        "peer",
    )
    assert pending is None
    assert task["id"] == "task-existing"
    assert task["status"]["state"] == protocol.STATE_WORKING


def test_missing_work_item_is_created_once(monkeypatch):
    created = []
    attempts = {"count": 0}

    def fake_prefix(item_id):
        attempts["count"] += 1
        if attempts["count"] == 1:
            raise work_items.WorkItemError("missing", kind="not_found")
        return f"Shared work item: {item_id}\n\n"

    monkeypatch.setattr(tools.work_items, "reference_prefix", fake_prefix)
    monkeypatch.setattr(
        tools.work_items, "append", lambda params: created.append(params) or {"created": True}
    )
    prefix, warning = tools._work_item_reference_or_create("new-item")
    assert prefix == "Shared work item: new-item\n\n"
    assert warning == ""
    assert [row["work_item_id"] for row in created] == ["new-item"]


def test_working_result_is_explicitly_no_resend():
    output = tools._format_task_result(
        "peer", "", "ctx-1", protocol.STATE_WORKING, "task-1"
    )
    assert "ACCEPTED" in output
    assert "Do not resend" in output
    assert "a2a_task_status" in output
