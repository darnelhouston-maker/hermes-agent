"""Task owners come from the Windows token, not mutable SSH environment."""
from types import SimpleNamespace

import pytest


@pytest.mark.platforms("windows")
@pytest.mark.parametrize("value,expected", [("PC\\owner", "PC\\owner"), ("", None),
                                          ("owner", None), ("PC\\owner\n", None)])
def test_task_owner_ignores_environment(monkeypatch, value, expected):
    import ctypes
    from hermes_cli import gateway_windows_session as session
    monkeypatch.setenv("USERDOMAIN", "WORKGROUP")
    monkeypatch.setenv("USERNAME", "untrusted")
    class Query:
        def __call__(self, kind, buffer, size):
            assert kind == 2
            buffer.value = value
            return bool(value)
    monkeypatch.setattr(ctypes, "WinDLL", lambda *_a, **_k: SimpleNamespace(GetUserNameExW=Query()))
    assert session.current_task_user() == expected


def test_unknown_task_owner_preserves_existing_task(monkeypatch, tmp_path):
    from hermes_cli import gateway_windows
    monkeypatch.setattr(gateway_windows, "_resolve_task_user", lambda: None)
    def forbidden(*_args, **_kwargs):
        pytest.fail("unknown owner must not delete or create a task")
    monkeypatch.setattr(gateway_windows, "_exec_schtasks", forbidden)
    assert gateway_windows._install_scheduled_task("synthetic", tmp_path / "gateway.cmd") == (
        False, "windows_task_owner_unavailable")
