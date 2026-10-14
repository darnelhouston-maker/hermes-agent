"""Windows Computer Use readiness must reflect the real Hermes MCP handshake."""

from __future__ import annotations

import asyncio
import concurrent.futures
import json
import time
from types import SimpleNamespace
from unittest.mock import patch

import pytest


class _AsyncContext:
    def __init__(self, value):
        self.value = value

    async def __aenter__(self):
        return self.value

    async def __aexit__(self, *_args):
        return None


class _FakeClientSession:
    delay = 0.0

    def __init__(self, _read, _write):
        pass

    async def __aenter__(self):
        return self

    async def __aexit__(self, *_args):
        return None

    async def initialize(self):
        await asyncio.sleep(self.delay)

    async def list_tools(self):
        return SimpleNamespace(tools=[], capability_version="test")


def _healthy_loader(delay: float = 0.0):
    class Client(_FakeClientSession):
        pass

    Client.delay = delay
    return Client, (lambda **kwargs: kwargs), (lambda _params: _AsyncContext((object(), object())))


def _session(monkeypatch, loader):
    from tools.computer_use import cua_backend_session as session_mod

    monkeypatch.setattr(session_mod, "_load_mcp_client", loader)
    monkeypatch.setattr(session_mod._driver, "resolve_cua_driver_cmd", lambda: "cua-driver")
    monkeypatch.setattr(session_mod._driver, "_resolve_mcp_invocation", lambda _cmd: ("cua-driver", ["mcp"]))
    monkeypatch.setattr("tools.computer_use.cua_backend.cua_driver_child_env", lambda: {})
    bridge = session_mod._AsyncBridge()
    return session_mod._CuaDriverSession(bridge), bridge


def test_import_failure_is_typed_immediately_and_never_unknown(monkeypatch):
    def broken():
        raise ModuleNotFoundError("No module named 'pywintypes'")

    session, bridge = _session(monkeypatch, broken)
    began = time.monotonic()
    with pytest.raises(RuntimeError, match="phase python-mcp-import.*pywintypes"):
        session.start()
    assert time.monotonic() - began < 2
    assert session._startup_phase == "python-mcp-import"
    assert session._lifecycle_done_event.wait(1)
    bridge.stop()


def test_delayed_handshake_reaches_ready_and_cleans_up(monkeypatch):
    session, bridge = _session(monkeypatch, lambda: _healthy_loader(0.03))
    session._startup_timeout_seconds = 1
    session.start()
    assert session._startup_phase == "ready"
    session.stop()
    assert session._lifecycle_done_event.wait(1)
    bridge.stop()


def test_stuck_handshake_reports_last_typed_phase_and_cancels(monkeypatch):
    session, bridge = _session(monkeypatch, lambda: _healthy_loader(0.2))
    session._startup_timeout_seconds = 0.02
    with pytest.raises(RuntimeError, match=r"timeout 0.02s; stuck in phase: mcp-initialize"):
        session.start()
    assert session._startup_phase == "mcp-initialize"
    assert session._lifecycle_done_event.wait(1)
    bridge.stop()


def test_retry_after_failed_import_can_recover(monkeypatch):
    calls = 0

    def loader():
        nonlocal calls
        calls += 1
        if calls == 1:
            raise ModuleNotFoundError("No module named 'pywintypes'")
        return _healthy_loader()

    session, bridge = _session(monkeypatch, loader)
    with pytest.raises(RuntimeError, match="python-mcp-import"):
        session.start()
    session.start()
    assert session._startup_phase == "ready"
    session.stop()
    bridge.stop()


def test_concurrent_independent_session_probes_do_not_share_state(monkeypatch):
    from tools.computer_use import doctor

    monkeypatch.setattr(doctor, "probe_tool_session_readiness", lambda timeout=30.0: {
        "ready": True, "phase": "ready", "elapsed_ms": 5, "cleanup": "complete",
        "error_class": None, "reason": None})
    with concurrent.futures.ThreadPoolExecutor(max_workers=2) as pool:
        results = list(pool.map(lambda _: doctor.probe_tool_session_readiness(), range(2)))
    assert all(result["ready"] and result["cleanup"] == "complete" for result in results)


def test_doctor_healthy_driver_fails_closed_when_python_client_is_broken():
    from tools.computer_use import doctor

    report = {"schema_version": "1", "overall": "ok", "platform": "windows",
              "driver_version": "0.22.2", "checks": []}
    with patch("tools.computer_use.cua_backend_session._load_mcp_client",
               side_effect=ModuleNotFoundError("No module named 'pywintypes'")):
        augmented = doctor._apply_hermes_readiness_contract(report, session_ready=True)
    assert augmented["overall"] == "ok"
    assert augmented["hermes_driver_overall"] == "ok"
    assert augmented["hermes_overall"] == "failed"
    assert augmented["hermes_session_readiness"]["phase"] == "python-mcp-import"
    assert "unknown" not in json.dumps(augmented)


def test_windows_current_package_precedes_prunable_legacy_path(monkeypatch):
    from tools.computer_use import cua_backend_driver

    monkeypatch.setattr(cua_backend_driver.sys, "platform", "win32")
    monkeypatch.setattr(cua_backend_driver.os.path, "expanduser", lambda _value: r"C:\Users\tester")
    candidates = cua_backend_driver._candidate_cua_driver_commands()
    assert candidates[1].replace("/", "\\").endswith(r".cua-driver\packages\current\cua-driver.exe")
    assert "Programs" in candidates[2]


def test_canary_deduplicates_failure_and_emits_one_recovery(tmp_path, monkeypatch):
    from scripts import computer_use_readiness_canary as canary

    failed = {"profile": "default", "ok": False, "driver_overall": "ok", "phase": "mcp-initialize",
              "elapsed_ms": 30, "cleanup": "complete", "error_class": "RuntimeError", "reason": "synthetic"}
    monkeypatch.setattr(canary, "_profiles", lambda _root: [("default", tmp_path)])
    monkeypatch.setattr(canary, "_probe", lambda *_args, **_kwargs: dict(failed))
    monkeypatch.setattr(canary, "_event_log", lambda *_args: None)
    first = canary.run(root=tmp_path, now=100)
    second = canary.run(root=tmp_path, now=101)
    assert first["events"] == [{"kind": "mismatch", "profile": "default"}]
    assert second["events"] == []
    monkeypatch.setattr(canary, "_probe", lambda *_args, **_kwargs: {**failed, "ok": True,
                                                                    "phase": "ready", "reason": None})
    recovered = canary.run(root=tmp_path, now=102)
    assert recovered["events"] == [{"kind": "recovered", "profile": "default"}]


def test_registered_windows_task_with_missing_target_is_not_ready(monkeypatch):
    from hermes_cli import tools_config_cua

    xml = """<?xml version='1.0'?><Task xmlns='http://schemas.microsoft.com/windows/2004/02/mit/task'>
    <Actions><Exec><Command>C:\\pruned\\cua-driver.exe</Command></Exec></Actions></Task>"""
    monkeypatch.setattr(tools_config_cua.sys, "platform", "win32")
    monkeypatch.setattr(tools_config_cua, "_post_setup_no_window_flags", lambda **_kwargs: 0)
    monkeypatch.setattr(tools_config_cua.subprocess, "run", lambda *_args, **_kwargs:
                        SimpleNamespace(returncode=0, stdout=xml, stderr=""))
    monkeypatch.setattr(tools_config_cua.os.path, "isfile", lambda _path: False)
    status = tools_config_cua._cua_driver_autostart_status_windows(r"C:\\current\\cua-driver.exe")
    assert status == {"registered": True, "target": r"C:\pruned\cua-driver.exe",
                      "target_exists": False, "matches_driver": False}
    assert tools_config_cua._cua_driver_autostart_registered_windows() is False


def test_stale_windows_task_repair_disables_then_reenables(monkeypatch):
    from hermes_cli import tools_config_cua

    driver = r"C:\current\cua-driver.exe"
    captured = {}
    monkeypatch.setattr(tools_config_cua.sys, "platform", "win32")
    monkeypatch.setattr(tools_config_cua.shutil, "which", lambda name: driver if name == driver else "powershell.exe")
    monkeypatch.setattr(tools_config_cua.os.path, "isfile", lambda path: path == driver)
    monkeypatch.setattr(tools_config_cua, "_cua_driver_autostart_status_windows",
                        lambda _driver="": {"registered": True, "target_exists": False, "matches_driver": False})
    monkeypatch.setattr(tools_config_cua, "_post_setup_no_window_flags", lambda **_kwargs: 0)

    def run(command, **_kwargs):
        captured["command"] = command
        return SimpleNamespace(returncode=0, stdout="", stderr="")

    monkeypatch.setattr(tools_config_cua, "_run_text", run)
    assert tools_config_cua._repair_cua_driver_autostart_windows(driver, verbose=False) is True
    powershell = captured["command"][-1]
    assert "@('autostart','disable')" in powershell
    assert "@('autostart','enable')" in powershell


def test_canary_installer_uses_active_python_and_canonical_script(monkeypatch, tmp_path):
    from hermes_cli import tools_config_cua

    captured = {}
    monkeypatch.setattr(tools_config_cua.sys, "platform", "win32")
    monkeypatch.setattr(tools_config_cua.shutil, "which", lambda _name: "powershell.exe")
    monkeypatch.setattr(tools_config_cua, "_post_setup_no_window_flags", lambda **_kwargs: 0)
    monkeypatch.setattr("hermes_constants.get_hermes_home", lambda: tmp_path)

    def run(command, **_kwargs):
        captured["command"] = command
        return SimpleNamespace(returncode=0, stdout="", stderr="")

    monkeypatch.setattr(tools_config_cua, "_run_text", run)
    assert tools_config_cua._install_computer_use_readiness_canary_windows(verbose=False) is True
    assert captured["command"][0] == "powershell.exe"
    assert "-PythonExe" in captured["command"]
    assert "computer_use_readiness_canary.py" in " ".join(captured["command"])
