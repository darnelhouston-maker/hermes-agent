"""Session0 handoffs preserve explicit homes and never bypass a failed task."""
from pathlib import Path
from unittest.mock import Mock

import pytest

from hermes_cli import gateway_windows_session as session
from hermes_cli.gateway_runtime_service import ExistingService, RuntimeStartError


@pytest.fixture
def target_home(tmp_path, monkeypatch):
    monkeypatch.setattr(Path, "home", lambda: tmp_path)
    launcher = tmp_path / "launcher"
    launcher.mkdir()
    monkeypatch.setenv("HERMES_HOME", str(launcher))
    home = tmp_path / "target" / "profiles" / "worker"
    home.mkdir(parents=True)
    return home


@pytest.mark.parametrize("config,expected", [
    ("{}", True),
    ("terminal: {backend: local}\ncomputer_use: {backend: cua}", True),
    ("terminal: {backend: ssh}", False),
    ("terminal: {backend: docker}", False),
    ("computer_use: {backend: remote}", False),
])
def test_target_effective_config_selects_only_local_cua(target_home, config, expected):
    (target_home / "config.yaml").write_text(config, encoding="utf-8")
    assert session._requires_local_desktop(target_home) is expected


@pytest.mark.parametrize("config", ["terminal: [", "terminal: []", "computer_use: {backend: 7}"])
def test_invalid_target_config_is_typed(target_home, config):
    (target_home / "config.yaml").write_text(config, encoding="utf-8")
    with pytest.raises(RuntimeStartError, match="windows_interactive_config_invalid"):
        session._requires_local_desktop(target_home)


@pytest.fixture
def boundaries(target_home, monkeypatch):
    from hermes_cli import gateway_windows

    service = ExistingService("windows", ("schtasks.exe", "/Run", "/TN", "target-task"))
    discover = Mock(return_value=service)
    start = Mock(return_value=9999)
    ready = Mock(return_value=[7476])
    config = Mock(return_value={})
    monkeypatch.setattr(session.host, "interactive_session", lambda: False)
    monkeypatch.setattr(session, "load_user_config_effective", config)
    monkeypatch.setattr(session, "discover_existing_gateway_service", discover)
    monkeypatch.setattr(session, "start_existing_gateway_service", start)
    monkeypatch.setattr(session, "remaining", lambda deadline: 8.0)
    monkeypatch.setattr(gateway_windows, "_wait_for_gateway_ready", ready)
    monkeypatch.setattr(gateway_windows, "_spawn_detached", Mock(side_effect=AssertionError("no direct spawn")))
    return service, discover, start, ready, config


@pytest.mark.platforms("windows")
@pytest.mark.parametrize("interactive,config,expected", [
    (True, {}, False),
    (False, {}, True),
    (False, {"terminal": {"backend": "ssh"}}, False),
    (False, {"computer_use": {"backend": "remote"}}, False),
])
def test_public_requirement_preserves_interactive_and_nonlocal_paths(
    target_home, boundaries, monkeypatch, interactive, config, expected,
):
    loader = boundaries[-1]
    loader.return_value = config
    monkeypatch.setattr(session.host, "interactive_session", lambda: interactive)
    assert session.requires_interactive_gateway(target_home) is expected
    if interactive:
        loader.assert_not_called()
    else:
        loader.assert_called_once_with(target_home.resolve() / "config.yaml", fail_closed=True)


@pytest.mark.platforms("windows")
def test_interactive_caller_retains_direct_path(target_home, boundaries, monkeypatch):
    monkeypatch.setattr(session.host, "interactive_session", lambda: True)
    assert session.request_interactive_gateway(target_home, 10.0) is None
    for boundary in boundaries[1:]:
        boundary.assert_not_called()


@pytest.mark.platforms("windows")
def test_session0_returns_owned_ready_pid_for_explicit_named_home(target_home, boundaries):
    service, discover, start, ready, config = boundaries
    assert session.request_interactive_gateway(target_home, 10.0) == 7476
    config.assert_called_once_with(target_home.resolve() / "config.yaml", fail_closed=True)
    discover.assert_called_once_with(target_home.resolve(), deadline=10.0)
    start.assert_called_once_with(service, deadline=10.0)
    ready.assert_called_once_with(home=target_home.resolve(), timeout_s=8.0)


@pytest.mark.platforms("windows")
@pytest.mark.parametrize("reason", [None, "service_identity_unverified", "service_account_mismatch"])
def test_session0_missing_or_invalid_task_never_falls_back(target_home, boundaries, reason):
    _, discover, start, ready, _ = boundaries
    if reason is None:
        discover.return_value = None
    else:
        discover.side_effect = RuntimeStartError(reason)
    with pytest.raises(RuntimeStartError, match=reason or "windows_interactive_service_required"):
        session.request_interactive_gateway(target_home, 10.0)
    start.assert_not_called()
    ready.assert_not_called()


@pytest.mark.platforms("windows")
@pytest.mark.parametrize("config", [
    {"terminal": {"backend": "ssh"}},
    {"terminal": {"backend": "docker"}},
    {"computer_use": {"backend": "remote"}},
])
def test_session0_remote_or_headless_config_is_unchanged(target_home, boundaries, config):
    _, discover, start, ready, loader = boundaries
    loader.return_value = config
    assert session.request_interactive_gateway(target_home, 10.0) is None
    for boundary in (discover, start, ready):
        boundary.assert_not_called()


@pytest.mark.platforms("windows")
@pytest.mark.parametrize("failure", ["config", "start", "readiness", "deadline", "wrong_backend"])
def test_session0_failures_do_not_report_a_launcher_pid(target_home, boundaries, failure):
    _, discover, start, ready, config = boundaries
    reason = "windows_interactive_gateway_not_ready"
    if failure == "config":
        config.side_effect = OSError("unreadable")
        reason = "windows_interactive_config_invalid"
    elif failure == "start":
        start.side_effect = RuntimeStartError("service_start_failed")
        reason = "service_start_failed"
    elif failure == "deadline":
        ready.side_effect = TimeoutError
    elif failure == "wrong_backend":
        discover.return_value = ExistingService("systemd", ("systemctl", "start", "unrelated"))
        reason = "service_identity_unverified"
    else:
        ready.return_value = []
    expected = TimeoutError if failure == "deadline" else RuntimeStartError
    with pytest.raises(expected, match=None if failure == "deadline" else reason):
        session.request_interactive_gateway(target_home, 10.0)
    if failure in {"config", "wrong_backend"}:
        start.assert_not_called()
    if failure in {"config", "start", "wrong_backend"}:
        ready.assert_not_called()
