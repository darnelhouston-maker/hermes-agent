#!/usr/bin/env python3
"""Compare cua-driver health with a real, no-action Hermes session handshake."""

from __future__ import annotations

import argparse
import json
import os
import socket
import subprocess
import sys
import tempfile
import time
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

REMINDER_SECONDS = 6 * 60 * 60


def _default_home() -> Path:
    if value := os.environ.get("HERMES_HOME"):
        return Path(value).expanduser()
    if os.name == "nt" and (local := os.environ.get("LOCALAPPDATA")):
        return Path(local) / "hermes"
    return Path.home() / ".hermes"


def _profiles(root: Path) -> list[tuple[str, Path]]:
    found = [("default", root)]
    profiles = root / "profiles"
    if profiles.is_dir():
        found.extend((path.name, path) for path in sorted(profiles.iterdir()) if path.is_dir())
    return found


def _probe(profile: str, home: Path, *, timeout: float) -> dict[str, Any]:
    env = dict(os.environ)
    env["HERMES_HOME"] = str(home)
    env["PYTHONUTF8"] = "1"
    env["CUA_DRIVER_RS_TELEMETRY_ENABLED"] = "0"
    command = [
        sys.executable,
        "-m",
        "hermes_cli.main",
        "computer-use",
        "doctor",
        "--session-ready",
        "--json",
    ]
    began = time.monotonic()
    try:
        result = subprocess.run(
            command,
            cwd=ROOT,
            env=env,
            capture_output=True,
            text=True,
            encoding="utf-8",
            errors="replace",
            timeout=timeout,
            stdin=subprocess.DEVNULL,
            check=False,
        )
        payload = json.loads(result.stdout)
        readiness = payload.get("hermes_session_readiness") or {}
        ok = (
            result.returncode == 0
            and payload.get("hermes_driver_overall") == "ok"
            and readiness.get("ready") is True
            and readiness.get("cleanup") == "complete"
        )
        return {
            "profile": profile,
            "ok": ok,
            "driver_overall": payload.get("hermes_driver_overall"),
            "phase": readiness.get("phase") or "not-reported",
            "elapsed_ms": readiness.get("elapsed_ms") or round((time.monotonic() - began) * 1000),
            "cleanup": readiness.get("cleanup") or "not-reported",
            "error_class": readiness.get("error_class"),
            "reason": readiness.get("reason"),
        }
    except subprocess.TimeoutExpired:
        return {
            "profile": profile,
            "ok": False,
            "driver_overall": "unknown",
            "phase": "canary-timeout",
            "elapsed_ms": round((time.monotonic() - began) * 1000),
            "cleanup": "subprocess-terminated",
            "error_class": "TimeoutExpired",
            "reason": "bounded readiness probe timed out",
        }
    except (OSError, ValueError, TypeError) as exc:
        return {
            "profile": profile,
            "ok": False,
            "driver_overall": "unknown",
            "phase": "canary-protocol",
            "elapsed_ms": round((time.monotonic() - began) * 1000),
            "cleanup": "complete",
            "error_class": type(exc).__name__,
            "reason": "readiness probe did not return valid metadata",
        }


def _atomic_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, temporary = tempfile.mkstemp(prefix=path.name + ".", dir=path.parent)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as handle:
            json.dump(payload, handle, indent=2, sort_keys=True)
            handle.write("\n")
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
    finally:
        try:
            os.unlink(temporary)
        except FileNotFoundError:
            pass


def _event_log(kind: str, result: dict[str, Any]) -> None:
    if os.name != "nt":
        return
    event_type = "INFORMATION" if kind == "recovered" else "ERROR"
    message = (
        f"Hermes computer-use readiness {kind}; host={socket.gethostname()}; "
        f"profile={result['profile']}; phase={result['phase']}; "
        f"driver={result['driver_overall']}; cleanup={result['cleanup']}; "
        f"error={result.get('error_class') or 'none'}"
    )
    try:
        subprocess.run(
            [
                "eventcreate",
                "/T",
                event_type,
                "/ID",
                "100",
                "/L",
                "APPLICATION",
                "/SO",
                "HermesComputerUse",
                "/D",
                message,
            ],
            timeout=10,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            check=False,
        )
    except (OSError, subprocess.SubprocessError):
        pass


def run(*, root: Path, timeout: float = 45.0, now: int | None = None) -> dict[str, Any]:
    now = int(time.time()) if now is None else now
    state_path = root / "state" / "computer_use_readiness_canary.json"
    try:
        previous = json.loads(state_path.read_text(encoding="utf-8"))
    except (OSError, ValueError, TypeError):
        previous = {}

    old_by_profile = {
        row.get("profile"): row
        for row in previous.get("results", [])
        if isinstance(row, dict)
    }
    results = [_probe(name, home, timeout=timeout) for name, home in _profiles(root)]
    events: list[dict[str, Any]] = []
    for result in results:
        old = old_by_profile.get(result["profile"], {})
        reminder_due = now - int(old.get("alerted_at") or 0) >= REMINDER_SECONDS
        if not result["ok"] and (old.get("ok") is not False or reminder_due):
            result["alerted_at"] = now
            events.append({"kind": "mismatch", "profile": result["profile"]})
            _event_log("mismatch", result)
        elif result["ok"] and old.get("ok") is False:
            events.append({"kind": "recovered", "profile": result["profile"]})
            _event_log("recovered", result)
        elif old.get("alerted_at"):
            result["alerted_at"] = old["alerted_at"]

    payload = {
        "schema_version": 1,
        "host": socket.gethostname(),
        "checked_at": now,
        "ok": all(result["ok"] for result in results),
        "results": results,
        "events": events,
    }
    _atomic_json(state_path, payload)
    return payload


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--hermes-home", type=Path, default=_default_home())
    parser.add_argument("--timeout", type=float, default=45.0)
    args = parser.parse_args()
    payload = run(root=args.hermes_home.expanduser(), timeout=args.timeout)
    print(json.dumps(payload, sort_keys=True))
    return 0 if payload["ok"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
