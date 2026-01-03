from datetime import datetime, timezone
from sqlalchemy import text
from db import get_engine

def _iso_to_dt(ts):
    if not ts:
        return None
    return datetime.fromisoformat(ts.replace("Z", "+00:00"))

def insert_reporting_metrics(owner, repo, since_ts, commits, pulls, contributors_count, branches_count, run_at=None):
    if run_at is None:
        run_at = datetime.now(timezone.utc)

    since_utc = None
    if since_ts:
        since_utc = since_ts.replace(tzinfo=timezone.utc) if since_ts.tzinfo is None else since_ts.astimezone(timezone.utc)

    new_commits = len(commits)
    prs_opened = prs_closed = prs_merged = 0

    for pr in pulls:
        ca = _iso_to_dt(pr.get("created_at"))
        cla = _iso_to_dt(pr.get("closed_at"))
        ma = _iso_to_dt(pr.get("merged_at"))

        if since_utc:
            if ca and ca >= since_utc: prs_opened += 1
            if cla and cla >= since_utc: prs_closed += 1
            if ma and ma >= since_utc: prs_merged += 1
        else:
            # أول run: baseline من اللي جبناه
            if ca: prs_opened += 1
            if cla: prs_closed += 1
            if ma: prs_merged += 1

    engine = get_engine()
    with engine.begin() as conn:
        conn.execute(
            text("""
                INSERT INTO reporting_repo_metrics
                    (repo_owner, repo_name, run_at, since, new_commits, prs_opened, prs_closed, prs_merged,contributors_count, branches_count)
                
                VALUES
                    (:o, :r, :run_at, :since, :nc, :po, :pc, :pm, :cc, :bc)
            """),
            {
                "o": owner, "r": repo,
                "run_at": run_at.replace(tzinfo=None),  # لو جدولك TIMESTAMP بدون timezone
                "since": since_ts,
                "nc": new_commits,
                "po": prs_opened,
                "pc": prs_closed,
                "pm": prs_merged,
                "cc": contributors_count,
                "bc": branches_count,
            }
        )

    return {
        "run_at": run_at,
        "since": since_ts,
        "new_commits": new_commits,
        "prs_opened": prs_opened,
        "prs_closed": prs_closed,
        "prs_merged": prs_merged,
        "contributors_count": contributors_count,
        "branches_count": branches_count,
    }
