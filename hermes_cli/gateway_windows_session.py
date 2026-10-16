"""Non-installing interactive-task handoff for Windows gateway launchers."""
from __future__ import annotations

from pathlib import Path

from hermes_cli.config_effective import load_user_config_effective
from hermes_cli.gateway_runtime_service import (
    RuntimeStartError,
    discover_existing_gateway_service,
    remaining,
    start_existing_gateway_service,
)
from hermes_platform import host


def _requires_local_desktop(home: Path) -> bool:
    try:
        config = load_user_config_effective(home / "config.yaml", fail_closed=True)
        backends = []
        for section, default in (("terminal", "local"), ("computer_use", "cua")):
            settings = config.get(section, {})
            if not isinstance(settings, dict):
                raise ValueError("invalid backend settings")
            backend = settings.get("backend", default)
            if not isinstance(backend, str):
                raise ValueError("invalid backend name")
            backends.append(backend.strip() or default)
    except Exception as exc:
        raise RuntimeStartError("windows_interactive_config_invalid") from exc
    return backends == ["local", "cua"]


def requires_interactive_gateway(home: Path) -> bool:
    """Whether this Windows caller needs a task handoff for the target profile."""
    return not host.interactive_session() and _requires_local_desktop(Path(home).resolve())


def request_interactive_gateway(home: Path, deadline: float) -> int | None:
    """Return the task-started gateway PID, or None to retain the direct launch path.

    Callers own drain/absence checks. A missing or unready interactive task is
    never permission to spawn a local desktop gateway in Session0 instead.
    """
    if not requires_interactive_gateway(home):
        return None
    target = Path(home).resolve()
    service = discover_existing_gateway_service(target, deadline=deadline)
    if service is None:
        raise RuntimeStartError("windows_interactive_service_required")
    if service.backend != "windows":
        raise RuntimeStartError("service_identity_unverified")
    start_existing_gateway_service(service, deadline=deadline)
    from hermes_cli import gateway_windows

    pids = gateway_windows._wait_for_gateway_ready(home=target, timeout_s=remaining(deadline))
    if not pids:
        raise RuntimeStartError("windows_interactive_gateway_not_ready")
    return pids[0]
