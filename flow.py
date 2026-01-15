from __future__ import annotations

from prefect import flow

from extract import (
    extract_repository,
    extract_contributors,
    extract_branches,
    extract_commits,
    extract_branch_commits,
    extract_pulls,
)

from etl_load import (
    ensure_schema,
    get_since_ts,
    upsert_repository,
    upsert_github_users,
    upsert_branches,
    upsert_commits,
    upsert_branch_commits,
    upsert_pull_requests,
)


@flow(log_prints=True)
def github_etl(repo_owner: str, repo_name: str):
    # 0) make sure the target schema exists
    ensure_schema()

    # 1) repo metadata -> repositories
    repo_meta = extract_repository.submit(repo_owner, repo_name).result()
    repo_id = upsert_repository(repo_meta, repo_owner, repo_name)

    # 2) watermark (derived from commits table only)
    since_ts = get_since_ts(repo_id)
    print(f"[WATERMARK] repo_id={repo_id} since={since_ts}")

    # 3) extract (tasks)
    contributors_f = extract_contributors.submit(repo_owner, repo_name)
    branches_f = extract_branches.submit(repo_owner, repo_name)
    commits_f = extract_commits.submit(repo_owner, repo_name, since_ts)
    pulls_f = extract_pulls.submit(repo_owner, repo_name, since_ts)

    contributors = contributors_f.result()
    branches = branches_f.result()
    commits = commits_f.result()
    pulls = pulls_f.result()

    # map commits to branches (best-effort) — run branch fetches in parallel
    branch_futures = []
    for b in branches:
        bn = b.get("name")
        if not bn:
            continue
        branch_futures.append((bn, extract_branch_commits.submit(repo_owner, repo_name, bn, since_ts=since_ts)))

    branch_commit_pairs = []
    branch_commits_all = []
    for bn, fut in branch_futures:
        items = fut.result()
        for it in items:
            sha = it.get("sha")
            if sha:
                branch_commit_pairs.append((bn, sha))
                branch_commits_all.append(it)

    print(
        f"[EXTRACT] contributors={len(contributors)}, branches={len(branches)}, commits={len(commits)}, "
        f"branch_commits={len(branch_commit_pairs)}, pulls={len(pulls)}"
    )

    # 4) parse + load (directly to the final schema)
    # users from: contributors + commit authors + PR authors
    user_rows = []

    for c in contributors:
        if c.get("id") and c.get("login"):
            user_rows.append((int(c["id"]), c["login"]))

    for c in (commits + branch_commits_all):
        a = c.get("author") or {}
        if a.get("id") and a.get("login"):
            user_rows.append((int(a["id"]), a["login"]))

    for pr in pulls:
        u = pr.get("user") or {}
        if u.get("id") and u.get("login"):
            user_rows.append((int(u["id"]), u["login"]))

    # de-dup (keep last login per id)
    user_rows = list({uid: login for uid, login in user_rows}.items())
    upsert_github_users(user_rows)

    upsert_branches(repo_id, branches)
    upsert_commits(repo_id, commits + branch_commits_all)
    upsert_branch_commits(repo_id, branch_commit_pairs)
    upsert_pull_requests(repo_id, pulls)

    return {
        "repo_id": repo_id,
        "contributors": len(contributors),
        "branches": len(branches),
        "commits": len(commits),
        "branch_commit_edges": len(branch_commit_pairs),
        "pulls": len(pulls),
        "since": since_ts,
    }
