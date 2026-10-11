"""Regression coverage for durable long-running A2A calls."""

from __future__ import annotations

import time
from concurrent.futures import Future
from threading import Event
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
        "round_number": 1,
        "future": future,
        "created_iso": rec["created_iso"],
        "started": time.time(),
    }


def test_nonblocking_send_returns_working_without_waiting(monkeypatch):
    adapter = _adapter()
    pending = _pending(adapter)
    monkeypatch.setattr(adapter, "_prepare_task", lambda *args, **kwargs: (None, pending))
    starter = MagicMock()
    monkeypatch.setattr(adapter, "_start_pending_finalizer", starter, raising=False)
    monkeypatch.setattr(
        adapter,
        "_await_reply",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(AssertionError("nonblocking send waited")),
    )

    response = adapter._rpc_message_send(
        "rpc-1",
        {"configuration": {"blocking": False}},
        "peer",
    )

    task = response["result"]
    assert task["id"] == "task-long"
    assert task["status"]["state"] == protocol.STATE_WORKING
    starter.assert_called_once_with(pending)


def test_nonblocking_finalizer_keeps_task_alive_after_http_return(monkeypatch):
    adapter = _adapter()
    pending = _pending(adapter, "task-late")
    assert hasattr(adapter, "_start_pending_finalizer")
    adapter._start_pending_finalizer(pending)

    pending["future"].set_result((protocol.STATE_COMPLETED, "late result"))
    deadline = time.time() + 2
    while time.time() < deadline:
        record = adapter.tasks.get("task-late")
        if record and record["state"] == protocol.STATE_COMPLETED:
            break
        time.sleep(0.01)

    record = adapter.tasks.get("task-late")
    assert record is not None
    assert record["state"] == protocol.STATE_COMPLETED
    assert record["reply"] == "late result"


def test_nonlocal_profile_nonblocking_returns_before_profile_finishes(monkeypatch):
    adapter = A2AAdapter(PlatformConfig(enabled=True, extra={
        "agents": {"dev": {"profile": "dev", "tenant": "dev", "local": False}}
    }))
    monkeypatch.setattr(
        "plugins.platforms.a2a.adapter.controller.reserve_round",
        lambda _peer, _context: 1,
    )
    release = Event()

    def forward(*_args):
        assert release.wait(timeout=2)
        return "profile result", protocol.STATE_COMPLETED

    monkeypatch.setattr(adapter, "_forward_to_profile", forward)
    params = {
        "tenant": "dev",
        "configuration": {"blocking": False},
        "message": protocol.text_message(
            protocol.ROLE_USER,
            "synthetic long profile task",
            context_id="ctx-profile",
        ),
    }

    response = adapter._rpc_message_send(
        "rpc-profile",
        params,
        "peer",
        agent=adapter._agents["dev"],
    )
    task_id = response["result"]["id"]
    assert response["result"]["status"]["state"] == protocol.STATE_WORKING
    release.set()

    deadline = time.time() + 2
    while time.time() < deadline:
        record = adapter.tasks.get(task_id, "dev", "dev")
        if record and record["state"] == protocol.STATE_COMPLETED:
            break
        time.sleep(0.01)
    assert adapter.tasks.get(task_id, "dev", "dev")["reply"] == "profile result"


def test_stable_request_id_dedupes_reworded_retry(monkeypatch):
    adapter = A2AAdapter(PlatformConfig(enabled=True, extra={
        "agents": {"dev": {"profile": "dev", "tenant": "dev", "local": False}}
    }))
    monkeypatch.setattr(
        "plugins.platforms.a2a.adapter.controller.reserve_round",
        lambda _peer, _context: 1,
    )
    calls: list[str] = []

    def forward(_agent, _peer, _context, text):
        calls.append(text)
        return "done", protocol.STATE_COMPLETED

    monkeypatch.setattr(adapter, "_forward_to_profile", forward)
    first_message = protocol.text_message(
        protocol.ROLE_USER,
        "first wording",
        context_id="ctx-idempotent",
        message_id="stable-request-id",
    )
    second_message = protocol.text_message(
        protocol.ROLE_USER,
        "reworded retry",
        context_id="ctx-idempotent",
        message_id="stable-request-id",
    )

    first = adapter._rpc_message_send(
        "one", {"tenant": "dev", "message": first_message}, "peer", agent=adapter._agents["dev"]
    )
    second = adapter._rpc_message_send(
        "two", {"tenant": "dev", "message": second_message}, "peer", agent=adapter._agents["dev"]
    )

    assert first["result"]["id"] == second["result"]["id"]
    assert len(calls) == 1


def test_client_submits_nonblocking_then_polls_to_terminal(monkeypatch):
    calls: list[str] = []

    monkeypatch.setattr(tools, "_fetch_card", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(tools.time, "sleep", lambda _seconds: None, raising=False)

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
            protocol.build_task("task-poll", "ctx-poll", protocol.STATE_COMPLETED, "finished"),
        )

    monkeypatch.setattr(tools, "_http_post_json", fake_post)
    reply, context_id, state, task_id = tools._send_task_uncontrolled(
        "peer",
        {"url": "http://localhost:9999", "auth": {}, "timeout": 5},
        "synthetic task",
        "ctx-poll",
    )

    assert (reply, context_id, state, task_id) == (
        "finished",
        "ctx-poll",
        protocol.STATE_COMPLETED,
        "task-poll",
    )
    assert calls == ["SendMessage", "GetTask"]


def test_active_exact_duplicate_returns_existing_working_task():
    adapter = _adapter()
    scope = adapter._scope_for_agent(None)
    adapter.tasks.create("task-existing", "ctx-existing", "peer", *scope)
    adapter.tasks.set_state("task-existing", protocol.STATE_WORKING)
    assert adapter._content_guard.claim(
        "peer", scope, "same synthetic request", "task-existing"
    ) == ""

    task, pending = adapter._prepare_task(
        {
            "message": protocol.text_message(
                protocol.ROLE_USER,
                "same synthetic request",
                context_id="ctx-existing",
            )
        },
        "peer",
    )

    assert pending is None
    assert task["id"] == "task-existing"
    assert task["status"]["state"] == protocol.STATE_WORKING


def test_active_work_item_dedupes_reworded_retry_across_contexts():
    adapter = _adapter()
    scope = adapter._scope_for_agent(None)
    adapter.tasks.create("task-work-item", "ctx-original", "peer", *scope)
    adapter.tasks.set_state("task-work-item", protocol.STATE_WORKING)
    assert adapter._claim_active_work_item(
        "peer",
        scope,
        "Shared work item: synthetic-item\n\nfirst wording",
        "task-work-item",
    ) == ""

    task, pending = adapter._prepare_task(
        {
            "message": protocol.text_message(
                protocol.ROLE_USER,
                "Shared work item: synthetic-item\n\nreworded retry",
                context_id="ctx-retry",
            )
        },
        "peer",
    )

    assert pending is None
    assert task["id"] == "task-work-item"
    assert task["status"]["state"] == protocol.STATE_WORKING


def test_missing_work_item_is_created_before_dispatch(monkeypatch):
    created: list[dict] = []
    attempts = {"count": 0}

    def fake_prefix(item_id):
        attempts["count"] += 1
        if attempts["count"] == 1:
            raise work_items.WorkItemError(
                f"shared work item '{item_id}' does not exist",
                kind="not_found",
            )
        return f"Shared work item: {item_id}\n\n"

    monkeypatch.setattr(tools.work_items, "reference_prefix", fake_prefix)
    monkeypatch.setattr(
        tools.work_items,
        "append",
        lambda params: created.append(params) or {"created": True},
    )
    monkeypatch.setattr(
        tools,
        "_resolve_peer",
        lambda _agent: {"url": "http://localhost:9999", "auth": {}, "timeout": 5},
    )
    monkeypatch.setattr(
        tools,
        "_send_task",
        lambda *_args, **_kwargs: (
            "ok",
            "ctx-created",
            protocol.STATE_COMPLETED,
            "task-created",
        ),
    )

    output = tools.a2a_call(
        {"agent": "peer", "message": "synthetic", "work_item_id": "new-item"}
    )

    assert "ok" in output
    assert created[0]["work_item_id"] == "new-item"
    assert created[0]["status"] == "open"
    assert "task task-created" in output


def test_missing_work_item_store_outage_does_not_drop_dispatch(monkeypatch):
    monkeypatch.setattr(
        tools.work_items,
        "reference_prefix",
        lambda item_id: (_ for _ in ()).throw(
            work_items.WorkItemError(
                f"shared work item '{item_id}' does not exist",
                kind="not_found",
            )
        ),
    )
    monkeypatch.setattr(
        tools.work_items,
        "append",
        lambda _params: (_ for _ in ()).throw(
            work_items.WorkItemError("shared work-item service is unavailable", kind="unavailable")
        ),
    )
    monkeypatch.setattr(
        tools,
        "_resolve_peer",
        lambda _agent: {"url": "http://localhost:9999", "auth": {}, "timeout": 5},
    )
    monkeypatch.setattr(
        tools,
        "_send_task",
        lambda *_args, **_kwargs: (
            "ok",
            "ctx-no-store",
            protocol.STATE_COMPLETED,
            "task-no-store",
        ),
    )

    output = tools.a2a_call(
        {"agent": "peer", "message": "synthetic", "work_item_id": "missing-item"}
    )

    assert "ok" in output
    assert "reference was omitted" in output
    assert "task task-no-store" in output


def test_working_receipt_says_do_not_resend(monkeypatch):
    monkeypatch.setattr(
        tools,
        "_resolve_peer",
        lambda _agent: {"url": "http://localhost:9999", "auth": {}, "timeout": 5},
    )
    monkeypatch.setattr(
        tools,
        "_send_task",
        lambda *_args, **_kwargs: (
            "",
            "ctx-running",
            protocol.STATE_WORKING,
            "task-running",
        ),
    )

    output = tools.a2a_call({"agent": "peer", "message": "synthetic"})

    assert "task task-running" in output
    assert "do not resend" in output.lower()
    assert "a2a_task_status" in output


def test_submit_timeout_is_delivery_unknown_and_never_says_retry(monkeypatch):
    monkeypatch.setattr(
        tools,
        "_resolve_peer",
        lambda _agent: {"url": "http://localhost:9999", "auth": {}, "timeout": 5},
    )
    monkeypatch.setattr(
        tools,
        "_send_task",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(
            tools.A2ASubmitTimeout("peer", "ctx-timeout")
        ),
    )

    output = tools.a2a_call({"agent": "peer", "message": "synthetic"})

    assert "delivery-unknown" in output
    assert "do not resend" in output.lower()
    assert "context ctx-timeout" in output


def test_task_status_fetches_terminal_result(monkeypatch):
    monkeypatch.setattr(
        tools,
        "_resolve_peer",
        lambda _agent: {"url": "http://localhost:9999", "auth": {}, "timeout": 5},
    )
    monkeypatch.setattr(tools, "_fetch_card", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(
        tools,
        "_http_post_json",
        lambda _url, body, _headers, _timeout: protocol.jsonrpc_result(
            body["id"],
            protocol.build_task(
                "task-status",
                "ctx-status",
                protocol.STATE_COMPLETED,
                "complete result",
            ),
        ),
    )

    output = tools.a2a_task_status({"agent": "peer", "task_id": "task-status"})

    assert "complete result" in output
    assert "task task-status" in output
    assert "completed" in output
