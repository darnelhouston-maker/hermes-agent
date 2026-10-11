from plugins.platforms.a2a.content_guard import RecentInboundGuard
from plugins.platforms.a2a import protocol
try:
    from plugins.platforms.a2a import controller
except ImportError:  # Older fleet adapters predate the conversation controller.
    controller = None
from plugins.platforms.a2a.adapter import A2AAdapter
from gateway.config import PlatformConfig


def test_content_guard_dedupes_across_context_independent_task_ids():
    now = [1000.0]
    guard = RecentInboundGuard(3600, time_fn=lambda: now[0])
    assert guard.claim("watson", ("", ""), "synthetic task", "task-1") == ""
    assert guard.claim("watson", ("", ""), "synthetic task", "task-2") == "task-1"


def test_content_guard_separates_peer_scope_and_content():
    guard = RecentInboundGuard(3600, time_fn=lambda: 1000.0)
    assert guard.claim("watson", ("", ""), "one", "task-1") == ""
    assert guard.claim("nova", ("", ""), "one", "task-2") == ""
    assert guard.claim("watson", ("dev", "tenant"), "one", "task-3") == ""
    assert guard.claim("watson", ("", ""), "two", "task-4") == ""


def test_content_guard_expires_after_window_and_can_be_disabled():
    now = [1000.0]
    guard = RecentInboundGuard(3600, time_fn=lambda: now[0])
    assert guard.claim("watson", ("", ""), "synthetic task", "task-1") == ""
    now[0] = 4600.0
    assert guard.claim("watson", ("", ""), "synthetic task", "task-2") == ""
    disabled = RecentInboundGuard(0, time_fn=lambda: now[0])
    assert disabled.claim("watson", ("", ""), "synthetic task", "task-3") == ""
    assert disabled.claim("watson", ("", ""), "synthetic task", "task-4") == ""


def test_adapter_returns_first_terminal_result_without_second_agent_turn(monkeypatch):
    if controller is not None:
        monkeypatch.setattr(controller, "reserve_round", lambda _peer, _context: 1)
    adapter = A2AAdapter(PlatformConfig(enabled=True, extra={
        "agents": {"dev": {"profile": "dev", "tenant": "dev", "local": False}}
    }))
    agent = adapter._agents["dev"]
    calls = []

    def forward(_agent, _peer, _context, _framed):
        calls.append(True)
        return "synthetic result", protocol.STATE_COMPLETED

    monkeypatch.setattr(adapter, "_forward_to_profile", forward)
    first, first_pending = adapter._prepare_task(
        {"message": protocol.text_message(protocol.ROLE_USER, "identical synthetic task", context_id="ctx-one")},
        "watson",
        agent=agent,
    )
    second, second_pending = adapter._prepare_task(
        {"message": protocol.text_message(protocol.ROLE_USER, "identical synthetic task", context_id="ctx-two")},
        "watson",
        agent=agent,
    )

    assert first_pending is second_pending is None
    assert first["id"] == second["id"]
    assert first["status"]["state"] == protocol.STATE_COMPLETED
    assert second["status"]["state"] == protocol.STATE_COMPLETED
    assert calls == [True]
