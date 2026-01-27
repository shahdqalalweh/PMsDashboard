from prefect import flow
from extract import extract_repository, extract_branches, extract_commits, extract_pulls
from etl_load import (
    ensure_schema,
    get_since_ts,
    upsert_repository,
    upsert_branches,
    upsert_commits,
    upsert_pulls,
)

@flow(log_prints=True)
def github_etl(owner: str, repo: str):
    ensure_schema()

    repo_meta = extract_repository(owner, repo)
    repo_id = upsert_repository(repo_meta, owner, repo)

    since_ts = get_since_ts(repo_id)
    print(f"[WATERMARK] since = {since_ts}")

    branches = extract_branches(owner, repo)
    commits = extract_commits(owner, repo, since_ts)
    pulls = extract_pulls(owner, repo, since_ts)

    upsert_branches(repo_id, branches)
    upsert_commits(repo_id, commits)
    upsert_pulls(repo_id, pulls)

    return {
        "repo_id": repo_id,
        "branches": len(branches),
        "commits": len(commits),
        "pulls": len(pulls),
    }
