"""An interactive canary cannot vouch for a Session0 or foreign-home gateway."""
import json
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock, call

import pytest

from hermes_cli import computer_use_readiness_canary as canary


@pytest.fixture
def healthy_doctor(monkeypatch):
    payload = {"hermes_driver_overall": "ok", "hermes_identity": {"cli_version": "fixture"},
               "hermes_session_readiness": {"ready": True, "phase": "ready", "cleanup": "complete"}}
    monkeypatch.setattr(canary.subprocess, "run", Mock(return_value=SimpleNamespace(
        returncode=0, stdout=json.dumps(payload), stderr="")))


@pytest.mark.platforms("windows")
@pytest.mark.parametrize("case,current,owner,reason", [
    ("valid", 1, 1, None),
    ("session0", 1, 0, "gateway_noninteractive_session"),
    ("canary0", 0, 1, "gateway_noninteractive_session"),
    ("mismatch", 1, 2, "gateway_session_mismatch"),
    ("unknown", 1, None, "gateway_session_query_unavailable"),
    ("self_unknown", None, 1, "gateway_session_query_unavailable"),
    ("missing", 1, 1, "gateway_owner_unavailable"),
    ("foreign_home", 1, 1, "gateway_home_mismatch"),
    ("bad_pid", 1, 1, "gateway_pid_invalid"),
    ("query_error", 1, 1, "gateway_session_query_unavailable"),
])
def test_canary_checks_exact_named_home_gateway_metadata(
    tmp_path, monkeypatch, healthy_doctor, case, current, owner, reason,
):
    from gateway import control_socket
    from hermes_cli import gateway_windows_session
    from hermes_platform import host

    home = tmp_path / "profiles" / "worker"
    monkeypatch.setattr(gateway_windows_session, "_requires_local_desktop", lambda target: True)
    identity = {"pid": 7476, "hermes_home": str(home)}
    if case == "foreign_home":
        identity["hermes_home"] = str(tmp_path)
    if case == "bad_pid":
        identity["pid"] = True
    identify = Mock(return_value=None if case == "missing" else identity)
    if case == "query_error":
        identify.side_effect = OSError("unavailable")
    monkeypatch.setattr(control_socket, "identify_gateway", identify)
    sessions = Mock(side_effect=lambda pid=None: current if pid is None else owner)
    monkeypatch.setattr(host, "process_session_id", sessions)

    result = canary._probe("worker", home, timeout=3)
    identify.assert_called_once_with(home.resolve(), timeout=2.0)
    context = result["gateway_context"]
    assert result["ok"] is (reason is None)
    assert context["reason"] == reason
    assert context["status"] == ("pass" if reason is None else "not-checked" if case == "missing" else "fail")
    if case in {"valid", "session0", "canary0", "mismatch", "unknown", "self_unknown"}:
        assert sessions.call_args_list == [call(), call(7476)]
        assert context["gateway_pid"] == 7476
    else:
        sessions.assert_not_called()


@pytest.mark.platforms("windows")
@pytest.mark.parametrize("config", ["terminal: {backend: ssh}", "computer_use: {backend: remote}"])
def test_remote_headless_canary_does_not_require_local_gateway(
    tmp_path, monkeypatch, healthy_doctor, config,
):
    from gateway import control_socket

    home = tmp_path / "profiles" / "worker"
    home.mkdir(parents=True)
    monkeypatch.setattr(Path, "home", lambda: tmp_path)
    monkeypatch.setenv("HERMES_HOME", str(tmp_path / "launcher"))
    (home / "config.yaml").write_text(config, encoding="utf-8")
    identify = Mock(side_effect=AssertionError("nonlocal config must not query a local owner"))
    monkeypatch.setattr(control_socket, "identify_gateway", identify)
    result = canary._probe("worker", home, timeout=1)
    assert result["ok"] is True
    assert result["gateway_context"]["status"] == "not-applicable"
    identify.assert_not_called()


@pytest.mark.platforms("windows")
def test_unreadable_target_config_cannot_vouch_for_gateway(tmp_path, monkeypatch, healthy_doctor):
    from gateway import control_socket
    from hermes_cli import gateway_windows_session

    monkeypatch.setattr(gateway_windows_session, "_requires_local_desktop", Mock(side_effect=OSError("unreadable")))
    identify = Mock()
    monkeypatch.setattr(control_socket, "identify_gateway", identify)
    result = canary._probe("worker", tmp_path, timeout=1)
    assert result["ok"] is False
    assert result["gateway_context"]["reason"] == "gateway_config_unavailable"
    identify.assert_not_called()


@pytest.mark.parametrize("status,ready", [("fail", False), ("not-checked", False), ("pass", True),
                                          ("not-applicable", None)])
def test_probe_never_hides_failed_gateway_context(tmp_path, monkeypatch, healthy_doctor, status, ready):
    context = {"status": status, "ready": ready, "reason": "fixture"}
    monkeypatch.setattr(canary, "_gateway_desktop_context", lambda home, **kwargs: context)
    result = canary._probe("worker", tmp_path, timeout=1)
    assert result["ok"] is (ready is not False)
    assert result["gateway_context"] == context
    if ready is False:
        assert result["phase"] == "gateway-desktop-context"
        assert result["reason"] == "fixture"


def test_gateway_context_alert_is_deduplicated_then_recovers(tmp_path, monkeypatch, healthy_doctor):
    contexts = iter([{"status": "fail", "ready": False, "reason": "gateway_noninteractive_session"},
                     {"status": "fail", "ready": False, "reason": "gateway_noninteractive_session"},
                     {"status": "pass", "ready": True, "reason": None}])
    monkeypatch.setattr(canary, "_gateway_desktop_context", lambda home, **kwargs: next(contexts))
    monkeypatch.setattr(canary, "_profiles", lambda root: [("worker", root)])
    events = []
    monkeypatch.setattr(canary, "_event_log", lambda kind, result: events.append(kind))
    first = canary.run(root=tmp_path, now=100)
    second = canary.run(root=tmp_path, now=101)
    third = canary.run(root=tmp_path, now=102)
    assert [first["ok"], second["ok"], third["ok"]] == [False, False, True]
    assert second["events"] == []
    assert events == ["mismatch", "recovered"]
