"""Durable, shared conversation budget for Watson's A2A hub.

Both the outbound client and inbound platform adapter reserve from the same
peer/context counter.  Locks are held only while the state file is read and
atomically replaced; no lock is held during a network call or model turn.
"""

from __future__ import annotations

import hashlib
import hmac
import json
import os
import tempfile
import threading
import time
from pathlib import Path
from typing import Any


_CONTEXT_LOCKS: dict[str, threading.Lock] = {}
_CONTEXT_LOCKS_GUARD = threading.Lock()


def limits() -> tuple[int, int]:
    """Return the configured (turn cap, idle timeout seconds), tightly bounded."""
    raw: dict[str, Any] = {}
    try:
        from hermes_cli.config import load_config

        cfg = load_config() or {}
        raw = ((cfg.get("a2a") or {}).get("conversation") or {})
        if not isinstance(raw, dict):
            raw = {}
    except Exception:
        raw = {}
    try:
        cap = max(1, min(int(raw.get("max_round_trips", 6)), 6))
    except (TypeError, ValueError):
        cap = 6
    try:
        idle = max(60, min(int(raw.get("idle_timeout_seconds", 3600)), 86400))
    except (TypeError, ValueError):
        idle = 3600
    return cap, idle


def _controller_secret() -> str:
    try:
        from agent.secret_scope import get_secret

        secret = (get_secret("A2A_CONTROLLER_HMAC_KEY", "") or "").strip()
    except Exception as exc:
        raise RuntimeError("A2A conversation controller key is unavailable.") from exc
    if len(secret) < 32:
        raise RuntimeError("A2A conversation controller key is missing or invalid.")
    return secret


def _context_digest(context_id: str) -> str:
    raw = str(context_id or "").encode("utf-8", errors="replace")
    return hmac.new(
        _controller_secret().encode("utf-8"), raw, hashlib.sha256
    ).hexdigest()


def _controller_dir() -> Path:
    try:
        from hermes_constants import get_hermes_home

        home = Path(get_hermes_home())
    except Exception:
        home = Path(os.getenv("HERMES_HOME") or (Path.home() / ".hermes"))
    path = home / "a2a_controller"
    path.mkdir(mode=0o700, parents=True, exist_ok=True)
    try:
        path.chmod(0o700)
    except OSError:
        pass
    return path


def _context_lock(key: str) -> threading.Lock:
    with _CONTEXT_LOCKS_GUARD:
        return _CONTEXT_LOCKS.setdefault(key, threading.Lock())


def _atomic_json_write(path: Path, payload: dict) -> None:
    fd, temp_name = tempfile.mkstemp(prefix=".state.", dir=str(path.parent))
    try:
        os.fchmod(fd, 0o600)
        with os.fdopen(fd, "w", encoding="utf-8", newline="\n") as handle:
            json.dump(payload, handle, ensure_ascii=False, sort_keys=True)
            handle.write("\n")
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temp_name, path)
    finally:
        if os.path.exists(temp_name):
            os.unlink(temp_name)


def reserve_round(peer: str, context_id: str) -> int:
    """Atomically reserve and return one shared inbound-or-outbound round."""
    if not str(peer or "").strip() or not str(context_id or "").strip():
        raise ValueError("A2A peer and context are required for conversation control.")
    cap, idle_timeout = limits()
    digest = _context_digest(context_id)
    base = _controller_dir()
    state_path = base / f"{digest}.json"
    lock_path = base / f"{digest}.lock"
    peer_fingerprint = hashlib.sha256(
        str(peer).encode("utf-8", errors="replace")
    ).hexdigest()

    with _context_lock(digest):
        with lock_path.open("a+", encoding="utf-8") as lock_handle:
            try:
                import fcntl
            except ImportError as exc:
                raise RuntimeError("A2A controller process locking is unavailable.") from exc
            try:
                fcntl.flock(lock_handle.fileno(), fcntl.LOCK_EX)
            except OSError as exc:
                raise RuntimeError("A2A controller process lock could not be acquired.") from exc

            now = time.time()
            state: dict[str, Any] = {}
            if state_path.exists():
                try:
                    state = json.loads(state_path.read_text(encoding="utf-8"))
                except Exception as exc:
                    raise ValueError(
                        "A2A conversation controller state is unreadable; start a new context."
                    ) from exc
            if state and state.get("peer_fingerprint") != peer_fingerprint:
                raise ValueError(
                    "An A2A context cannot be reused with a different peer; start a new context."
                )
            last_activity = float(state.get("last_activity_at") or 0)
            if state and last_activity and now - last_activity > idle_timeout:
                raise ValueError(
                    f"A2A conversation expired after {idle_timeout} seconds idle; start a new context."
                )
            round_number = int(state.get("rounds") or 0) + 1
            if round_number > cap:
                raise ValueError(
                    f"A2A conversation reached its {cap}-round limit; start a new context."
                )

            _atomic_json_write(
                state_path,
                {
                    "peer_fingerprint": peer_fingerprint,
                    "rounds": round_number,
                    "created_at": float(state.get("created_at") or now),
                    "last_activity_at": now,
                },
            )
    return round_number
