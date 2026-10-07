"""Regression coverage for bounded cua-driver lifecycle recovery.

These tests use synthetic futures only.  They specifically guard the leak
class where an MCP lifecycle missed its ready/shutdown deadline and Hermes
dropped the future while the ``cua-driver mcp`` child kept running.
"""

import concurrent.futures
import os
import threading
from unittest.mock import MagicMock, patch

import pytest

from tools.computer_use.cua_backend import CuaDriverBackend, _CuaDriverSession


class _NeverReadyEvent:
    def set(self):
        return None

    def is_set(self):
        return False

    def clear(self):
        return None

    def wait(self, timeout=None):
        return False


class _TimedOutFuture:
    def __init__(self):
        self.cancel_calls = 0
        self.result_timeouts = []

    def result(self, timeout=None):
        self.result_timeouts.append(timeout)
        raise concurrent.futures.TimeoutError()

    def cancel(self):
        self.cancel_calls += 1
        return True


def _bare_session(future):
    session = _CuaDriverSession.__new__(_CuaDriverSession)
    session._lock = threading.Lock()
    session._started = True
    session._bridge = MagicMock()
    session._bridge._loop = MagicMock()
    session._shutdown_event = None
    session._lifecycle_future = future
    session._lifecycle_done_event = _NeverReadyEvent()
    session._owned_standard_runtime_socket = None
    return session


def test_shutdown_timeout_cancels_abandoned_lifecycle():
    future = _TimedOutFuture()
    session = _bare_session(future)

    session._stop_lifecycle_locked()

    assert future.cancel_calls == 1
    assert session._lifecycle_future is None


def test_startup_timeout_cancels_abandoned_lifecycle():
    future = _TimedOutFuture()
    session = _bare_session(None)
    session._started = False
    session._setup_error = None
    session._startup_phase = "mcp-initialize"

    with patch("tools.computer_use.cua_backend.threading.Event", _NeverReadyEvent), \
         patch("tools.computer_use.cua_backend.asyncio.run_coroutine_threadsafe", return_value=future), \
         patch.object(_CuaDriverSession, "_lifecycle_coro", lambda self: None):
        with pytest.raises(RuntimeError, match="stuck in phase: mcp-initialize"):
            session._start_lifecycle_locked()

    assert future.cancel_calls == 1
    assert session._lifecycle_future is None


def test_stop_reaps_lifecycle_even_after_started_flag_dropped():
    future = _TimedOutFuture()
    session = _bare_session(future)
    session._started = False
    session._stop_owned_standard_runtime_locked = MagicMock()

    session.stop()

    assert future.cancel_calls == 1
    session._stop_owned_standard_runtime_locked.assert_called_once_with()


def test_backend_end_session_has_a_short_teardown_deadline():
    backend = CuaDriverBackend.__new__(CuaDriverBackend)
    backend._session = MagicMock()
    backend._session._started = True
    backend._session_id = "synthetic-session"
    backend._bridge = MagicMock()
    backend._embedded_daemon = None

    backend.stop()

    backend._session.call_tool.assert_called_once_with(
        "end_session", {"session": backend._session_id}, timeout=3.0
    )


def test_forced_cleanup_terminates_only_the_tracked_owned_mcp_child():
    session = _bare_session(None)
    session._mcp_child_tokens = {321: 1234.5, 654: 5678.0}

    owned = MagicMock()
    owned.ppid.return_value = os.getpid()
    owned.create_time.return_value = 1234.5
    owned.cmdline.return_value = ["/opt/bin/cua-driver", "mcp", "--no-overlay"]

    unrelated = MagicMock()
    unrelated.ppid.return_value = 1
    unrelated.create_time.return_value = 5678.0
    unrelated.cmdline.return_value = ["/opt/bin/cua-driver", "mcp"]

    with patch("psutil.Process", side_effect=lambda pid: {321: owned, 654: unrelated}[pid]), \
         patch("psutil.wait_procs", side_effect=[([], [owned]), ([owned], [])]):
        session._terminate_tracked_mcp_children(reason="synthetic timeout")

    owned.terminate.assert_called_once_with()
    owned.kill.assert_called_once_with()
    unrelated.terminate.assert_not_called()
    assert session._mcp_child_tokens == {}
