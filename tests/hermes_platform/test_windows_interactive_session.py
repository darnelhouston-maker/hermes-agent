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
