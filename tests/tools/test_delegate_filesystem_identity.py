"""Delegated local commands keep the invoking parent's filesystem identity."""

from __future__ import annotations

import os
import shlex
import sys
from pathlib import Path

import pytest

from agent.delegation_context import (
    DELEGATED_CHILD_ENV_MARKER,
    DELEGATED_PARENT_GID_ENV,
    DELEGATED_PARENT_UID_ENV,
    delegated_child_context,
    delegated_child_parent_identity,
    delegated_child_subprocess_env,
)
from tools.environments import local
from tools.environments.local import LocalEnvironment


pytestmark = pytest.mark.skipif(os.name == "nt", reason="POSIX UID/GID contract")


def _assert_parent_can_publish_phase_report(root: Path) -> None:
    staged = root / ".hermes-tmp"
    published = root / "phase-report.synthetic"
    staged.write_text("synthetic\n", encoding="utf-8")
    staged.replace(published)
    assert published.read_text(encoding="utf-8") == "synthetic\n"


def _run_delegated(root: Path, command: str, *, timeout: float = 10) -> dict:
    with delegated_child_context(
        "synthetic-child", parent_uid=os.geteuid(), parent_gid=os.getegid()
    ):
        env = LocalEnvironment(cwd=str(root), timeout=timeout)
        return env.execute(command, timeout=timeout)


def test_parent_identity_is_context_local_and_exported_to_descendants() -> None:
    assert delegated_child_parent_identity() is None
    with delegated_child_context("child", parent_uid=1000, parent_gid=1001):
        assert delegated_child_parent_identity() == (1000, 1001)
        child_env = delegated_child_subprocess_env({})
        assert child_env is not None
        assert child_env[DELEGATED_CHILD_ENV_MARKER]
        assert child_env[DELEGATED_PARENT_UID_ENV] == "1000"
        assert child_env[DELEGATED_PARENT_GID_ENV] == "1001"
        assert delegated_child_parent_identity(child_env) == (1000, 1001)
    assert delegated_child_parent_identity() is None


def test_privileged_worker_is_dropped_to_parent_identity(monkeypatch: pytest.MonkeyPatch) -> None:
    env = {
        DELEGATED_CHILD_ENV_MARKER: "1",
        DELEGATED_PARENT_UID_ENV: "1000",
        DELEGATED_PARENT_GID_ENV: "1001",
    }
    monkeypatch.setattr(local.os, "geteuid", lambda: 0)
    monkeypatch.setattr(local.os, "getegid", lambda: 0)
    assert local._delegated_popen_identity_kwargs(env) == {
        "user": 1000,
        "group": 1001,
        "extra_groups": (),
    }


def test_unprivileged_identity_mismatch_fails_before_spawn(monkeypatch: pytest.MonkeyPatch) -> None:
    env = {
        DELEGATED_CHILD_ENV_MARKER: "1",
        DELEGATED_PARENT_UID_ENV: "1000",
        DELEGATED_PARENT_GID_ENV: "1000",
    }
    monkeypatch.setattr(local.os, "geteuid", lambda: 2000)
    monkeypatch.setattr(local.os, "getegid", lambda: 2000)
    with pytest.raises(PermissionError, match="does not match"):
        local._delegated_popen_identity_kwargs(env)


def test_success_nested_outputs_remain_parent_writable(tmp_path: Path) -> None:
    result = _run_delegated(tmp_path, "mkdir -p success/nested && : > success/nested/output")
    assert result["returncode"] == 0
    assert (tmp_path / "success/nested/output").stat().st_uid == os.geteuid()
    _assert_parent_can_publish_phase_report(tmp_path / "success")


def test_failure_keeps_created_outputs_parent_writable(tmp_path: Path) -> None:
    result = _run_delegated(tmp_path, "mkdir -p failed/nested && : > failed/nested/output; exit 7")
    assert result["returncode"] == 7
    assert (tmp_path / "failed/nested/output").stat().st_uid == os.geteuid()
    _assert_parent_can_publish_phase_report(tmp_path / "failed")


def test_timeout_keeps_preexisting_and_new_outputs_parent_writable(tmp_path: Path) -> None:
    preexisting = tmp_path / "timeout/preexisting"
    preexisting.mkdir(parents=True)
    command = "mkdir -p timeout/preexisting/child && : > timeout/preexisting/child/output; sleep 30"
    result = _run_delegated(tmp_path, command, timeout=0.2)
    assert result["returncode"] != 0
    assert (preexisting / "child/output").stat().st_uid == os.geteuid()
    _assert_parent_can_publish_phase_report(tmp_path / "timeout")


def test_child_subprocess_outputs_remain_parent_writable(tmp_path: Path) -> None:
    script = (
        "from pathlib import Path; "
        "p=Path('subprocess/nested'); p.mkdir(parents=True); "
        "(p/'output').write_text('synthetic', encoding='utf-8')"
    )
    command = f"{shlex.quote(sys.executable)} -c {shlex.quote(script)}"
    result = _run_delegated(tmp_path, command)
    assert result["returncode"] == 0
    assert (tmp_path / "subprocess/nested/output").stat().st_uid == os.geteuid()
    _assert_parent_can_publish_phase_report(tmp_path / "subprocess")
