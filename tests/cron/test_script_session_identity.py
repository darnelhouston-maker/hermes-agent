"""Scheduler-owned script identity must not depend on agent setup timing."""
import os
import subprocess
import sys
from concurrent.futures import ThreadPoolExecutor

import pytest


@pytest.mark.parametrize("suffix", [".py", ".sh"])
@pytest.mark.parametrize("parent_marker", [None, "0", "1"])
def test_scheduler_script_identity_survives_context_and_overlay(
    tmp_path, monkeypatch, suffix, parent_marker,
):
    from cron import scheduler_script as script
    from cron.scheduler_provider import _profile_cron_scope
    from gateway.session_context import _VAR_MAP

    marker = _VAR_MAP["HERMES_CRON_SESSION"]
    token = marker.set(parent_marker)
    monkeypatch.setenv("HERMES_CRON_SESSION", "ambient-not-authority")
    original = script._script_argv

    def conflicting_overlay(path):
        argv, overlay, error = original(path)
        return argv, {**overlay, "HERMES_CRON_SESSION": "0"}, error

    monkeypatch.setattr(script, "_script_argv", conflicting_overlay)
    try:
        for home in (tmp_path / "a", tmp_path / "b", tmp_path / "a"):
            scripts = home / "scripts"
            scripts.mkdir(parents=True, exist_ok=True)
            child = scripts / ("identity" + suffix)
            child.write_text(
                "import os; print(os.environ.get('HERMES_CRON_SESSION', 'missing'))\n"
                if suffix == ".py" else 'printf "%s\\n" "$HERMES_CRON_SESSION"\n'
            )
            with _profile_cron_scope(home):
                ok, output = script._run_job_script(child.name)
            assert (ok, output) == (True, "1")
            assert marker.get() == parent_marker
            assert os.environ["HERMES_CRON_SESSION"] == "ambient-not-authority"
    finally:
        marker.reset(token)


def test_scheduler_identity_is_thread_local_and_not_added_to_direct_launch(
    tmp_path, monkeypatch,
):
    import cron.scheduler as scheduler
    from cron import scheduler_script as script
    from gateway.session_context import _VAR_MAP
    from tools.environments.local import build_subprocess_env

    monkeypatch.setattr(scheduler, "_get_hermes_home", lambda: tmp_path)
    monkeypatch.delenv("HERMES_CRON_SESSION", raising=False)
    scripts = tmp_path / "scripts"
    scripts.mkdir()
    child = scripts / "identity.py"
    child.write_text("import os; print(os.environ.get('HERMES_CRON_SESSION', 'missing'))\n")
    marker = _VAR_MAP["HERMES_CRON_SESSION"]

    def owned_fire(index):
        prior = "non-cron-" + str(index)
        token = marker.set(prior)
        try:
            result = script._run_job_script(child.name)
            assert marker.get() == prior
            return result
        finally:
            marker.reset(token)

    with ThreadPoolExecutor(max_workers=2) as pool:
        assert list(pool.map(owned_fire, range(4))) == [(True, "1")] * 4
    token = marker.set(None)
    try:
        direct = subprocess.run(
            [sys.executable, str(child)], env=build_subprocess_env(),
            capture_output=True, text=True, timeout=15,
        )
        assert direct.returncode == 0
        assert direct.stdout.strip() != "1"
    finally:
        marker.reset(token)
