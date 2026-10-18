"""No-send contracts for recurring repeat exhaustion and isolated CLI resumption."""

import json
import os
from pathlib import Path
import subprocess
import sys
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone

import pytest

from cron import jobs
from cron.exhaustion import read_inventory


@pytest.fixture
def store(tmp_path, monkeypatch):
    home = tmp_path / "home"
    home.mkdir()
    (home / "scripts").mkdir()
    (home / "scripts" / "synthetic.py").write_text("raise RuntimeError('MUST NOT EXECUTE')\n")
    monkeypatch.setenv("HERMES_HOME", str(home))
    monkeypatch.setattr(jobs, "CRON_DIR", home / "cron")
    monkeypatch.setattr(jobs, "JOBS_FILE", home / "cron" / "jobs.json")
    monkeypatch.setattr(jobs, "OUTPUT_DIR", home / "cron" / "output")
    return home


def completed(*, schedule="every 6h", script=False):
    kw = {"script": "synthetic.py", "no_agent": True} if script else {}
    job = jobs.create_job("synthetic", schedule, repeat=2, **kw)
    jobs.mark_job_run(job["id"], success=True)
    jobs.mark_job_run(job["id"], success=True)
    return jobs.get_job(job["id"])


@pytest.mark.parametrize("schedule", ["every 6h", "0 */6 * * *"])
def test_extended_total_budget_plain_resume_preserves_completed_and_history(store, schedule):
    old = completed(schedule=schedule)
    jobs.update_job(old["id"], {"repeat": 4})
    new = jobs.resume_job(old["id"])
    assert new["repeat"] == {"times": 4, "completed": 2}
    assert new["last_run_at"] == old["last_run_at"]
    assert new["last_status"] == "ok"
    assert new["enabled"] and new["state"] == "scheduled"
    assert datetime.fromisoformat(new["next_run_at"]) > datetime.now(timezone.utc)
    assert jobs.get_due_jobs() == []


def test_remaining_runs_adds_to_lifetime_count_and_is_not_a_counter_reset(store):
    old = completed()
    new = jobs.resume_job(old["id"], remaining_runs=3)
    assert new["repeat"] == {"times": 5, "completed": 2}
    assert new["repeat_exhaustion"]["at"] == old["repeat_exhaustion"]["at"]
    assert new["repeat_exhaustion"]["resumed_at"]


def test_exhausted_resume_is_non_mutating_and_actionable(store):
    old = completed()
    before = jobs.JOBS_FILE.read_bytes()
    with pytest.raises(ValueError, match="--remaining-runs"):
        jobs.resume_job(old["id"])
    assert jobs.JOBS_FILE.read_bytes() == before


@pytest.mark.parametrize("remaining", [0, -1, True, False, 1.5, "3"])
def test_invalid_remaining_runs_do_not_mutate(store, remaining):
    old = completed()
    before = jobs.JOBS_FILE.read_bytes()
    with pytest.raises(ValueError, match="positive integer"):
        jobs.resume_job(old["id"], remaining_runs=remaining)
    assert jobs.JOBS_FILE.read_bytes() == before


@pytest.mark.parametrize("claim", ["run_claim", "fire_claim"])
def test_terminal_resume_preserves_live_claim(store, claim):
    old = completed()
    old[claim] = {"at": datetime.now(timezone.utc).isoformat(), "by": "synthetic-owner"}
    jobs.save_jobs([old])
    before = jobs.JOBS_FILE.read_bytes()
    with pytest.raises(ValueError, match="live .* claim"):
        jobs.resume_job(old["id"], remaining_runs=2)
    assert jobs.JOBS_FILE.read_bytes() == before


def test_active_replacement_watcher_blocks_terminal_resume(store):
    old = completed(script=True)
    replacement = jobs.create_job("synthetic", "every 6h", script="synthetic.py", no_agent=True)
    before = jobs.JOBS_FILE.read_bytes()
    with pytest.raises(ValueError, match="equivalent active"):
        jobs.resume_job(old["id"], remaining_runs=20)
    assert jobs.JOBS_FILE.read_bytes() == before
    assert jobs.get_job(replacement["id"])["enabled"]


def test_concurrent_resume_cannot_replenish_budget_twice(store):
    old = completed()
    def resume(_):
        try:
            return bool(jobs.resume_job(old["id"], remaining_runs=3))
        except ValueError:
            return False
    with ThreadPoolExecutor(max_workers=4) as pool:
        assert sum(pool.map(resume, range(4))) == 1
    assert jobs.get_job(old["id"])["repeat"] == {"times": 5, "completed": 2}


def test_paused_jobs_and_one_shot_rearm_restrictions_survive(store):
    paused = jobs.create_job("synthetic", "every 1h")
    jobs.pause_job(paused["id"], reason="intentional hold")
    with pytest.raises(ValueError, match="only to completed recurring"):
        jobs.resume_job(paused["id"], remaining_runs=2)
    assert jobs.get_job(paused["id"])["state"] == "paused"
    future = (datetime.now(timezone.utc) + timedelta(minutes=5)).isoformat()
    with pytest.raises(ValueError, match="one-shot-only"):
        jobs.rearm_oneshot(paused["id"], future)
    once = jobs.create_job("synthetic", "in 1h")
    jobs.mark_job_run(once["id"], success=True)
    with pytest.raises(ValueError, match="terminal"):
        jobs.resume_job(once["id"])
    assert jobs.rearm_oneshot(once["id"], future)["repeat"]["completed"] == 0


def test_exhaustion_marker_survives_reload_and_is_payload_free(store):
    old = completed(script=True)
    marker = jobs.get_job(old["id"])["repeat_exhaustion"]
    assert marker == {"code": "recurring_repeat_exhausted", "at": old["last_run_at"],
                      "completed": 2, "times": 2, "kind": "interval"}
    assert not any(k in marker for k in ("script", "prompt", "deliver", "name"))
    assert jobs.get_due_jobs() == []


def test_inventory_is_read_only_payload_free_and_preserves_paused(store):
    old = completed(script=True)
    replacement = jobs.create_job("synthetic", "every 6h", script="synthetic.py", no_agent=True)
    paused = jobs.create_job("synthetic", "every 1h")
    jobs.pause_job(paused["id"])
    before = jobs.JOBS_FILE.read_bytes()
    result = read_inventory(jobs.JOBS_FILE)
    assert result["jobs_scanned"] == 3 and result["monitoring_candidates"] == 1
    assert result["rows"][0]["active_equivalents"] == 1
    assert result["rows"][0]["business_state"] == "owner_evidence_required"
    assert result["mutations"] == result["executions"] == 0
    assert "synthetic.py" not in json.dumps(result)
    assert old["id"] not in json.dumps(result) and replacement["id"] not in json.dumps(result)
    assert jobs.JOBS_FILE.read_bytes() == before
    assert read_inventory(jobs.JOBS_FILE) == result


def cli(home, *args):
    prefix = json.loads(os.environ.get("HERMES_CRON_TEST_CLI", "null"))
    if prefix is None:
        prefix = [sys.executable, "-m", "hermes_cli.main"]
    env = dict(os.environ, HERMES_HOME=str(home), HOME=str(home),
               HERMES_NO_UPDATE_CHECK="1", NO_COLOR="1")
    return subprocess.run([*prefix, "cron", *args], cwd=Path(__file__).resolve().parents[2],
                          env=env, capture_output=True, text=True, timeout=45)


def test_entry_cli_repeat_edit_then_resume_no_execution(store):
    old = completed(script=True)
    result = cli(store, "edit", old["id"], "--repeat", "4")
    assert result.returncode == 0, result.stdout + result.stderr
    result = cli(store, "resume", old["id"])
    assert result.returncode == 0, result.stdout + result.stderr
    assert jobs.get_job(old["id"])["repeat"] == {"times": 4, "completed": 2}
    assert not any(p.is_file() for p in (store / "cron" / "output").rglob("*"))


def test_entry_cli_remaining_errors_and_inventory_no_execution(store):
    old = completed(script=True)
    before = jobs.JOBS_FILE.read_bytes()
    for flags in [[], ["--run-now"], ["--remaining-runs", "0"],
                  ["--remaining-runs", "2", "--run-now"]]:
        result = cli(store, "resume", old["id"], *flags)
        assert result.returncode != 0, result.stdout + result.stderr
        assert jobs.JOBS_FILE.read_bytes() == before
    result = cli(store, "exhausted")
    assert result.returncode == 0, result.stdout + result.stderr
    assert json.loads(result.stdout)["monitoring_candidates"] == 1
    assert jobs.JOBS_FILE.read_bytes() == before
    result = cli(store, "resume", old["id"], "--remaining-runs", "3")
    assert result.returncode == 0, result.stdout + result.stderr
    assert jobs.get_job(old["id"])["repeat"] == {"times": 5, "completed": 2}


def test_doctor_reports_successful_but_exhausted_monitor(store, monkeypatch, capsys):
    old = completed(script=True)
    from hermes_cli import cron
    monkeypatch.setattr(cron, "_script_health_issue", lambda _: None, raising=False)
    assert cron.cron_doctor() == 1
    assert "recurring_repeat_exhausted" in capsys.readouterr().out
    assert jobs.get_job(old["id"])["last_status"] == "ok"
