"""Desktop-context failures cannot be hidden by an otherwise healthy driver."""
import json
from unittest.mock import Mock

import pytest


def _blocked():
    return {"name": "interactive_desktop", "status": "fail",
            "message": "windows_interactive_session_required",
            "data": {"phase": "windows-interactive-desktop"}}


def test_doctor_fails_before_native_driver_or_ui_access(monkeypatch, capsys):
    from tools.computer_use import doctor
    monkeypatch.setattr(doctor, "native_desktop_check", _blocked)
    monkeypatch.setattr(doctor, "_python_mcp_client_check", lambda: {
        "name": "python_mcp_client", "status": "pass", "data": {}, "message": "ok",
    })
    native = Mock(side_effect=AssertionError("driver_must_not_start"))
    monkeypatch.setattr(doctor, "_drive_health_report", native)
    assert doctor.run_doctor(json_output=True, session_ready=True) == 1
    result = json.loads(capsys.readouterr().out)
    assert result["hermes_overall"] == "failed"
    assert result["hermes_session_readiness"]["phase"] == "windows-interactive-desktop"
    assert result["hermes_session_readiness"]["cleanup"] == "not-started"
    native.assert_not_called()


def test_backend_fails_before_acquisition_or_native_spawn(monkeypatch):
    from tools.computer_use import cua_backend, permissions
    monkeypatch.setattr(cua_backend, "sandbox_mcp_invocation", lambda: None)
    monkeypatch.setattr(permissions, "native_desktop_check", _blocked)
    backend = cua_backend.CuaDriverBackend.__new__(cua_backend.CuaDriverBackend)
    with pytest.raises(RuntimeError, match="windows_interactive_session_required"):
        backend.start()


def test_status_cannot_report_ready_from_driver_health_alone(monkeypatch):
    from tools.computer_use import permissions, cua_backend_driver
    from plugins import computer_use
    monkeypatch.setattr(computer_use, "configured_backend_name", lambda: "cua")
    monkeypatch.setattr(cua_backend_driver, "resolve_cua_driver_cmd", lambda _: "synthetic-driver")
    monkeypatch.setattr(permissions, "native_desktop_check", _blocked)
    native = Mock(side_effect=AssertionError("driver_must_not_start"))
    monkeypatch.setattr(permissions, "_doctor", native)
    status = permissions.computer_use_status()
    assert status["ready"] is False
    assert status["checks"][0]["name"] == "interactive_desktop"
    native.assert_not_called()


@pytest.mark.platforms("windows")
@pytest.mark.parametrize("error,expected", [(False, True), (True, False), (None, False)])
def test_interactive_readiness_exercises_non_capture_desktop_rpc(monkeypatch, error, expected):
    from tools.computer_use import doctor, cua_backend_session
    monkeypatch.setattr(doctor, "native_desktop_check", lambda: {
        "name": "interactive_desktop", "status": "pass", "message": "ok",
    })
    session = Mock(_startup_phase="ready")
    session.call_tool.return_value = {"isError": error}
    session._lifecycle_done_event.is_set.return_value = True
    monkeypatch.setattr(cua_backend_session, "_CuaDriverSession", lambda _: session)
    monkeypatch.setattr(cua_backend_session, "_AsyncBridge", Mock)
    result = doctor.probe_tool_session_readiness()
    assert result["ready"] is expected
    session.call_tool.assert_called_once_with("get_displays", {}, timeout=30.0)
    session.stop.assert_called_once()
