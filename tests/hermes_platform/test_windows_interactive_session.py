"""Win32 desktop readiness, including service and locked-session negatives."""
from unittest.mock import Mock

import pytest


@pytest.mark.platforms("windows")
@pytest.mark.parametrize("session,visible,desktop,expected", [
    (0, True, True, False), (1, False, True, False),
    (1, True, False, False), (1, True, True, True),
])
def test_interactive_session_requires_visible_accessible_desktop(
    monkeypatch, session, visible, desktop, expected,
):
    import ctypes
    from hermes_platform.host import facts

    kernel, user = Mock(), Mock()
    kernel.GetCurrentProcessId.return_value = 7
    def session_id(_pid, target):
        target._obj.value = session
        return 1
    def object_flags(_station, _index, target, _size, _length):
        target._obj.flags = int(visible)
        return 1
    kernel.ProcessIdToSessionId.side_effect = session_id
    user.GetProcessWindowStation.return_value = 64
    user.GetUserObjectInformationW.side_effect = object_flags
    user.OpenInputDesktop.return_value = 128 if desktop else 0
    monkeypatch.setattr(ctypes, "WinDLL", lambda name, **_: kernel if name == "kernel32" else user)
    assert facts._windows_interactive_session() is expected
    if session == 0:
        user.OpenInputDesktop.assert_not_called()
    elif visible and desktop:
        user.CloseDesktop.assert_called_once_with(128)


@pytest.mark.platforms("windows")
@pytest.mark.parametrize("pid,session,success,expected", [
    (None, 1, True, 1), (7476, 0, True, 0), (22000, 1, True, 1),
    (7476, 1, False, None),
])
def test_process_session_id_queries_only_requested_pid(monkeypatch, pid, session, success, expected):
    import ctypes
    from hermes_platform import host

    kernel = Mock()
    def query(actual_pid, target):
        assert actual_pid == (99 if pid is None else pid)
        target._obj.value = session
        return int(success)
    kernel.ProcessIdToSessionId.side_effect = query
    monkeypatch.setattr(ctypes, "WinDLL", lambda *args, **kwargs: kernel)
    monkeypatch.setattr("os.getpid", lambda: 99)
    assert host.process_session_id(pid) == expected


@pytest.mark.platforms("windows")
def test_process_session_id_api_unavailable_is_unknown(monkeypatch):
    import ctypes
    from hermes_platform import host

    monkeypatch.setattr(ctypes, "WinDLL", Mock(side_effect=OSError("unavailable")))
    assert host.process_session_id(7476) is None


@pytest.mark.parametrize("pid", [0, -1, True, "7476", 1.5, 0x100000000])
def test_process_session_id_rejects_invalid_pid_without_query(pid):
    from hermes_platform import host

    assert host.process_session_id(pid) is None
