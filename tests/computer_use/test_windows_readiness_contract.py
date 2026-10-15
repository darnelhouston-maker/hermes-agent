"""Hermes-side readiness must agree with native cua-driver health."""

from __future__ import annotations

import asyncio
import json
import threading
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

import pytest


def test_pre_phase_import_failure_is_typed_and_cleanup_completes(monkeypatch):
    from tools.computer_use import cua_backend_session as session_mod

    session = session_mod._CuaDriverSession(session_mod._AsyncBridge())

    def fail_import():
        raise ImportError("pywintypes loader unavailable")

    monkeypatch.setattr(session_mod, "_load_mcp_client", fail_import)
    with pytest.raises(ImportError):
        asyncio.run(session._lifecycle_coro())

    assert session._startup_phase == "python-mcp-import"
    assert isinstance(session._setup_error, ImportError)
    assert session._ready_event.is_set()
    assert session._lifecycle_done_event.is_set()
    assert session._started is False


def test_transient_failed_start_recovers_on_next_attempt(monkeypatch):
    from tools.computer_use import cua_backend_session as session_mod

    bridge = session_mod._AsyncBridge()
    session = session_mod._CuaDriverSession(bridge)
    attempts = 0

    async def lifecycle(self):
        nonlocal attempts
        attempts += 1
        self._shutdown_event = asyncio.Event()
        try:
            if attempts == 1:
                self._startup_phase = "python-mcp-import"
                self._setup_error = ImportError("transient import failure")
                self._ready_event.set()
                return
            self._startup_phase = "ready"
            self._ready_event.set()
            await self._shutdown_event.wait()
        finally:
            self._started = False
            self._lifecycle_done_event.set()

    monkeypatch.setattr(session_mod._CuaDriverSession, "_lifecycle_coro", lifecycle)
    bridge.start()
    try:
        with pytest.raises(RuntimeError, match="python-mcp-import"):
            session.start()
        session.start()
        assert session._startup_phase == "ready"
        session.stop()
        assert attempts == 2
    finally:
        bridge.stop()


def test_timeout_never_reports_unknown_and_cleans_up(monkeypatch):
    from tools.computer_use import cua_backend_session as session_mod

    async def wedged(self):
        self._shutdown_event = asyncio.Event()
        self._startup_phase = "mcp-initialize"
        try:
            await self._shutdown_event.wait()
        finally:
            self._started = False
            self._lifecycle_done_event.set()

    bridge = session_mod._AsyncBridge()
    session = session_mod._CuaDriverSession(bridge)
    session._startup_timeout_seconds = 0.05
    monkeypatch.setattr(session_mod._CuaDriverSession, "_lifecycle_coro", wedged)
    bridge.start()
    try:
        with pytest.raises(RuntimeError) as exc_info:
            session.start()
        message = str(exc_info.value)
        assert "phase: mcp-initialize" in message
        assert "unknown" not in message
        assert session._lifecycle_done_event.wait(1)
        assert session._started is False
    finally:
        bridge.stop()


def test_native_health_cannot_hide_python_client_failure(monkeypatch):
    from tools.computer_use import doctor

    monkeypatch.setattr(doctor, "_load_mcp_client", None, raising=False)
    with patch(
        "tools.computer_use.cua_backend_session._load_mcp_client",
        side_effect=ImportError("pywintypes missing"),
    ):
        report = doctor._apply_hermes_readiness_contract(
            {"overall": "ok", "checks": []}, session_ready=True
        )

    assert report["hermes_driver_overall"] == "ok"
    assert report["hermes_overall"] == "failed"
    assert report["hermes_session_readiness"]["phase"] == "python-mcp-import"
    assert report["hermes_session_readiness"]["cleanup"] == "not-started"


def test_driver_and_delayed_tool_readiness_agree(monkeypatch):
    from tools.computer_use import doctor

    monkeypatch.setattr(doctor, "_python_mcp_client_check", lambda: {
        "name": "python_mcp_client", "status": "pass", "message": "ok",
        "data": {"phase": "python-mcp-import"},
    })
    monkeypatch.setattr(doctor, "probe_tool_session_readiness", lambda: {
        "ready": True, "phase": "ready", "elapsed_ms": 1359,
        "cleanup": "complete", "error_class": None, "reason": None,
    })

    report = doctor._apply_hermes_readiness_contract(
        {"overall": "ok", "checks": []}, session_ready=True
    )

    assert report["hermes_overall"] == "ok"
    assert report["hermes_session_readiness"]["elapsed_ms"] == 1359
    assert report["hermes_session_readiness"]["cleanup"] == "complete"


def test_canary_deduplicates_mismatch_and_emits_recovery(tmp_path, monkeypatch):
    from hermes_cli import computer_use_readiness_canary as canary

    events = []
    probe_results = iter([
        {"profile": "default", "ok": False, "driver_overall": "ok",
         "phase": "python-mcp-import", "elapsed_ms": 12, "cleanup": "complete",
         "error_class": "ImportError", "reason": "typed failure"},
        {"profile": "default", "ok": False, "driver_overall": "ok",
         "phase": "python-mcp-import", "elapsed_ms": 11, "cleanup": "complete",
         "error_class": "ImportError", "reason": "typed failure"},
        {"profile": "default", "ok": True, "driver_overall": "ok",
         "phase": "ready", "elapsed_ms": 9, "cleanup": "complete",
         "error_class": None, "reason": None},
    ])
    monkeypatch.setattr(canary, "_profiles", lambda root: [("default", root)])
    monkeypatch.setattr(canary, "_probe", lambda *args, **kwargs: next(probe_results))
    monkeypatch.setattr(canary, "_event_log", lambda kind, result: events.append(kind))

    first = canary.run(root=tmp_path, now=100)
    second = canary.run(root=tmp_path, now=101)
    third = canary.run(root=tmp_path, now=102)

    assert first["events"] == [{"kind": "mismatch", "profile": "default"}]
    assert second["events"] == []
    assert third["events"] == [{"kind": "recovered", "profile": "default"}]
    assert events == ["mismatch", "recovered"]
    assert json.loads((tmp_path / "state" / "computer_use_readiness_canary.json").read_text())["ok"]


@pytest.mark.platforms("windows")
def test_canary_registration_requires_stable_runner_and_launcher(tmp_path, monkeypatch):
    from hermes_cli import tools_config_cua as cua

    home = tmp_path / "hermes"
    launcher = home / "bin" / "hermes.exe"
    runner = home / "scripts" / "computer-use-readiness-canary-task.ps1"
    launcher.parent.mkdir(parents=True)
    runner.parent.mkdir(parents=True)
    launcher.write_bytes(b"MZ")
    runner.write_text("Write-Output ok\n", encoding="utf-8")
    xml = (
        "<Task xmlns='http://schemas.microsoft.com/windows/2004/02/mit/task'>"
        "<Actions><Exec><Command>powershell.exe</Command>"
        f"<Arguments>-File &quot;{runner}&quot; -HermesExe &quot;{launcher}&quot;</Arguments>"
        "</Exec></Actions></Task>"
    ).encode("utf-16")
    monkeypatch.setattr("hermes_constants.get_default_hermes_root", lambda: home)
    monkeypatch.setattr(cua.subprocess, "run", lambda *args, **kwargs: SimpleNamespace(
        returncode=0, stdout=xml, stderr=b""
    ))

    assert cua._computer_use_readiness_canary_registered_windows()


def test_canary_runner_records_public_launcher_failure_without_ui_payload():
    root = Path(__file__).resolve().parents[2]
    body = (root / "scripts" / "run-computer-use-readiness-canary.ps1").read_text(encoding="utf-8")

    assert "phase = \"public-launcher\"" in body
    assert "hermes_cli.computer_use_readiness_canary" in body
    assert "screenshot" not in body.lower()
