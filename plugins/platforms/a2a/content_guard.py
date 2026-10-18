"""Content-addressed inbound A2A loop guard.

Only SHA-256 keys and task ids are retained; message text is never stored.
"""

from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path
import socket
import subprocess
import threading
import time
from collections import deque


def content_dedupe_window() -> float:
    try:
        return min(86400.0, max(0.0, float(os.getenv("A2A_CONTENT_DEDUPE_SECONDS", "3600"))))
    except (ValueError, TypeError):
        return 3600.0


def volume_alarm_settings() -> tuple[int, float]:
    try:
        threshold = min(10000, max(0, int(os.getenv("A2A_VOLUME_ALERT_THRESHOLD", "30"))))
    except (ValueError, TypeError):
        threshold = 30
    try:
        window = min(86400.0, max(60.0, float(os.getenv("A2A_VOLUME_ALERT_WINDOW_SECONDS", "3600"))))
    except (ValueError, TypeError):
        window = 3600.0
    return threshold, window


class RecentInboundGuard:
    """Process-local SHA-256 guard for identical peer tasks across contexts."""

    def __init__(self, window: float, time_fn=time.time, cap: int = 5000) -> None:
        self.window = window
        self.time_fn = time_fn
        self.cap = cap
        self._entries: dict[str, tuple[float, str]] = {}
        self._lock = threading.Lock()

    @staticmethod
    def key(peer: str, scope: tuple[str, str], text: str) -> str:
        material = "\0".join((peer, scope[0], scope[1], text)).encode("utf-8", errors="replace")
        return hashlib.sha256(b"hermes-a2a-content-v1\0" + material).hexdigest()

    def claim(self, peer: str, scope: tuple[str, str], text: str, task_id: str) -> str:
        if self.window <= 0:
            return ""

    def release_unaccepted(self, peer: str, scope: tuple[str, str], text: str, task_id: str) -> None:
        """Release only this attempt's process-local reservation after failed acceptance."""
        key = self.key(peer, scope, text)
        with self._lock:
            if self._entries.get(key, (None, None))[1] == task_id:
                self._entries.pop(key, None)
        now = self.time_fn()
        key = self.key(peer, scope, text)
        with self._lock:
            stale = [entry for entry, (seen, _) in self._entries.items() if now - seen >= self.window]
            for entry in stale:
                self._entries.pop(entry, None)
            prior = self._entries.get(key)
            if prior:
                return prior[1]
            self._entries[key] = (now, task_id)
            if len(self._entries) > self.cap:
                oldest = min(self._entries, key=lambda entry: self._entries[entry][0])
                self._entries.pop(oldest, None)
            return ""


class PeerVolumeAlarm:
    """Observe A2A volume without blocking or retaining message content."""

    def __init__(self, threshold: int, window: float, time_fn=time.time) -> None:
        self.threshold = threshold
        self.window = window
        self.time_fn = time_fn
        self._events: dict[tuple[str, tuple[str, str]], deque[float]] = {}
        self._last_alert: dict[tuple[str, tuple[str, str]], float] = {}
        self._lock = threading.Lock()

    def observe(self, peer: str, scope: tuple[str, str]) -> int:
        if self.threshold <= 0:
            return 0
        now = self.time_fn()
        key = (peer, scope)
        with self._lock:
            events = self._events.setdefault(key, deque())
            while events and now - events[0] >= self.window:
                events.popleft()
            events.append(now)
            last_alert = self._last_alert.get(key, float("-inf"))
            if len(events) > self.threshold and now - last_alert >= self.window:
                self._last_alert[key] = now
                return len(events)
        return 0


def _append_alarm_receipt(payload: dict) -> None:
    home = Path(os.getenv("HERMES_HOME", str(Path.home() / ".hermes")))
    path = home / "state" / "a2a_volume_alerts.jsonl"
    try:
        path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
        os.chmod(path.parent, 0o700)
        with path.open("a", encoding="utf-8") as handle:
            handle.write(json.dumps(payload, sort_keys=True, separators=(",", ":")) + "\n")
        os.chmod(path, 0o600)
    except OSError:
        pass


def _dispatch_volume_alarm(peer: str, scope: tuple[str, str], count: int) -> None:
    host = socket.gethostname().split(".", 1)[0]
    scope_hash = hashlib.sha256("\0".join(scope).encode("utf-8")).hexdigest()[:16]
    hour_bucket = int(time.time() // 3600)
    item = f"a2a-volume-{host}-{hashlib.sha256(peer.encode()).hexdigest()[:12]}-{hour_bucket}"
    report = (
        f"Work item: {item}. Metadata-only A2A volume alarm: host={host}; "
        f"sender_hash={hashlib.sha256(peer.encode()).hexdigest()[:16]}; "
        f"receiver_scope_hash={scope_hash}; observed_count={count}; window_seconds=3600. "
        "Investigate for a software loop without replaying tasks or reading message content."
    )
    receipt = {
        "event": "a2a_volume_alarm",
        "host": host,
        "sender_hash": hashlib.sha256(peer.encode()).hexdigest()[:16],
        "receiver_scope_hash": scope_hash,
        "count": count,
        "item": item,
        "delivered": False,
        "ts": int(time.time()),
    }
    configured_command = os.getenv("A2A_VOLUME_ALERT_COMMAND", "").strip()
    candidates = ([Path(configured_command)] if configured_command else []) + [
        Path("/root/.hermes/scripts/tell_mac2.py"),
        Path("/data/.hermes/scripts/tell_mac2.py"),
    ]
    for candidate in candidates:
        if not candidate.is_file():
            continue
        try:
            proc = subprocess.run(
                [str(candidate), "--", report],
                stdin=subprocess.DEVNULL,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
                timeout=180,
                check=False,
            )
            receipt["delivered"] = proc.returncode == 0
            receipt["route"] = "mac2"
            receipt["rc"] = proc.returncode
        except (OSError, subprocess.TimeoutExpired):
            receipt["route"] = "mac2_error"
        _append_alarm_receipt(receipt)
        return

    try:
        from plugins.platforms.a2a import tools as a2a_tools

        target = "mac2" if a2a_tools._resolve_peer("mac2") else "watson"
        target_peer = a2a_tools._resolve_peer(target)
        if target_peer:
            relay = report if target == "mac2" else report + " Relay this once to Mac 2 with tell_mac2.py."
            _, _, state, _task_id = a2a_tools._send_task_uncontrolled(
                target,
                target_peer,
                relay,
                "ctx-" + hashlib.sha256(item.encode()).hexdigest()[:16],
            )
            receipt["delivered"] = str(state).lower() in {"completed", "succeeded", "success"}
            receipt["route"] = target
    except Exception as exc:
        receipt["route"] = "unavailable"
        receipt["error_class"] = type(exc).__name__
    _append_alarm_receipt(receipt)


def start_volume_alarm(peer: str, scope: tuple[str, str], count: int) -> None:
    """Dispatch a metadata-only alarm asynchronously; never block the A2A turn."""
    threading.Thread(
        target=_dispatch_volume_alarm,
        args=(peer, scope, count),
        name="a2a-volume-alarm",
        daemon=True,
    ).start()
