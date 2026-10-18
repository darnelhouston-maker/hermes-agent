"""Read-only, payload-free inventory of terminal recurring jobs; never revive jobs."""

import argparse
import hashlib
import json
from collections import Counter
from pathlib import Path


def project(jobs):
    if not isinstance(jobs, list) or any(not isinstance(j, dict) for j in jobs):
        raise ValueError("invalid job inventory")
    rows = []
    for job in jobs:
        schedule = job.get("schedule") or {}
        if schedule.get("kind") not in {"interval", "cron"} or job.get("state") != "completed":
            continue
        repeat = job.get("repeat") or {}
        times, completed = repeat.get("times"), repeat.get("completed", 0)
        valid = (type(completed) is int and completed >= 0
                 and (times is None or (type(times) is int and times > 0)))
        disposition = ("budget_exhausted_owner_review" if valid and times is not None and completed >= times
                       else "terminal_owner_review" if valid else "invalid_budget_owner_review")
        active_equivalent = sum(
            other.get("id") != job.get("id") and other.get("enabled", True)
            and other.get("state") not in {"completed", "paused"}
            and any(job.get(f) and job.get(f) == other.get(f)
                    for f in ("script", "monitor_script", "monitor_url"))
            for other in jobs
        )
        rows.append({
            "job_token": hashlib.sha256(str(job.get("id", "")).encode()).hexdigest()[:24],
            "kind": schedule["kind"], "monitoring_candidate": bool(
                job.get("no_agent") or job.get("monitor_script") or job.get("monitor_url")),
            "completed": completed if valid else None, "times": times if valid else None,
            "enabled": bool(job.get("enabled", True)), "disposition": disposition,
            "exhaustion_marker": isinstance(job.get("repeat_exhaustion"), dict),
            "active_equivalents": active_equivalent,
            "business_state": "owner_evidence_required", "action": "report_only_no_revival",
        })
    rows.sort(key=lambda row: row["job_token"])
    return {"schema": 1, "jobs_scanned": len(jobs), "terminal_recurring": len(rows),
            "monitoring_candidates": sum(r["monitoring_candidate"] for r in rows),
            "dispositions": dict(Counter(r["disposition"] for r in rows)),
            "mutations": 0, "executions": 0, "rows": rows}


def read_inventory(path):
    path = Path(path)
    if not path.exists():
        return project([])
    raw = path.read_bytes()
    data = json.loads(raw)
    result = project(data.get("jobs") if isinstance(data, dict) else data)
    result["source_sha256"] = hashlib.sha256(raw).hexdigest()
    return result


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--jobs-file", required=True)
    parser.add_argument("--check", action="store_true")
    args = parser.parse_args(argv)
    try:
        result = read_inventory(args.jobs_file)
    except (OSError, ValueError, TypeError):
        print(json.dumps({"error": "job_inventory_unavailable_or_invalid", "mutations": 0}))
        return 2
    print(json.dumps(result, sort_keys=True))
    return int(args.check and result["monitoring_candidates"] > 0)


if __name__ == "__main__":
    raise SystemExit(main())
