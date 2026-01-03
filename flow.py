from prefect import flow
from datetime import datetime, timezone

from state import get_last_run_at, update_last_run_at
from extract import extract_contributors, extract_branches, extract_commits, extract_pulls
from stage_raw_load import (
    load_stage_raw_contributors, load_stage_raw_branches, load_stage_raw_commits, load_stage_raw_pulls
)
from normalized_transform import (
    upsert_users_from_contributors, upsert_repo_users, upsert_branches, upsert_commits, upsert_pulls
)
from reporting_metrics import insert_reporting_metrics

@flow(log_prints=True)
def github_etl(repo_owner: str, repo_name: str):
    # A) watermark
    since_ts = get_last_run_at(repo_owner, repo_name)
    print(f"[STATE] since = {since_ts}")

    # B) extract
    contributors = extract_contributors(repo_owner, repo_name)
    branches = extract_branches(repo_owner, repo_name)
    commits = extract_commits(repo_owner, repo_name, since_ts)
    pulls = extract_pulls(repo_owner, repo_name, since_ts)

    print(f"[EXTRACT] contributors={len(contributors)}, branches={len(branches)}, commits={len(commits)}, pulls={len(pulls)}")

    # C) stage_raw
    load_stage_raw_contributors(repo_owner, repo_name, contributors)
    load_stage_raw_branches(repo_owner, repo_name, branches)
    load_stage_raw_commits(repo_owner, repo_name, commits)
    load_stage_raw_pulls(repo_owner, repo_name, pulls)
    print("[STAGE_RAW] loaded")

    # D) normalized
    upsert_users_from_contributors(contributors)
    upsert_repo_users(repo_owner, repo_name, contributors)
    upsert_branches(repo_owner, repo_name, branches)
    upsert_commits(repo_owner, repo_name, commits)
    upsert_pulls(repo_owner, repo_name, pulls)
    print("[NORMALIZED] upserted")

    # E) reporting
    run_at = datetime.now(timezone.utc)
    metrics = insert_reporting_metrics(
        repo_owner, repo_name,
        since_ts, commits, pulls,
        contributors_count=len(contributors),
        branches_count=len(branches),
        run_at=run_at
    )
    print(f"[REPORTING] {metrics}")

    # F) update state
    update_last_run_at(repo_owner, repo_name, run_at.replace(tzinfo=None))
    print("[STATE] updated")

    return metrics
