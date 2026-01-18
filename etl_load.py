from __future__ import annotations

from datetime import datetime, timezone
from typing import Iterable, Optional, Dict, Any, List, Tuple

from sqlalchemy import text

from db import get_engine


def ensure_schema() -> None:
    """Create the target tables if they don't exist.

    Note: schema.sql usually contains multiple statements, so we split by ';'
    and execute statement-by-statement.
    """
    engine = get_engine()
    schema_path = __import__("pathlib").Path(__file__).with_name("schema.sql")
    ddl = schema_path.read_text(encoding="utf-8")

    statements = [s.strip() for s in ddl.split(";") if s.strip()]

    with engine.begin() as conn:
        for stmt in statements:
            conn.execute(text(stmt))


def get_since_ts(repo_id: int) -> Optional[datetime]:
    """Watermark derived only from target tables (no extra state layer).

    We use the max committed_at in commits for that repo.
    """
    engine = get_engine()
    with engine.begin() as conn:
        row = conn.execute(
            text("SELECT MAX(committed_at) AS max_ts FROM commits WHERE repo_id = :rid"),
            {"rid": repo_id},
        ).mappings().first()
    return row["max_ts"] if row and row["max_ts"] else None


def upsert_repository(repo: Dict[str, Any], owner_org: str, name: str) -> int:
    """Insert/update repository row. Returns repo_id."""
    repo_id = int(repo["id"])
    default_branch = repo.get("default_branch") or "main"

    engine = get_engine()
    with engine.begin() as conn:
        conn.execute(
            text(
                """
                INSERT INTO repositories
                    (repo_id, owner_org, name, default_branch_name, default_branch_repo_id, default_branch_branch_name)
                VALUES
                    (:repo_id, :owner_org, :name, :default_branch_name, :default_branch_repo_id, :default_branch_branch_name)
                ON CONFLICT (repo_id) DO UPDATE SET
                    owner_org = EXCLUDED.owner_org,
                    name = EXCLUDED.name,
                    default_branch_name = EXCLUDED.default_branch_name,
                    default_branch_repo_id = EXCLUDED.default_branch_repo_id,
                    default_branch_branch_name = EXCLUDED.default_branch_branch_name
                """
            ),
            {
                "repo_id": repo_id,
                "owner_org": owner_org,
                "name": name,
                "default_branch_name": default_branch,
                # helper link (best effort)
                "default_branch_repo_id": repo_id,
                "default_branch_branch_name": default_branch,
            },
        )
    return repo_id


def upsert_repositories_min(rows: Iterable[Tuple[int, Optional[str], Optional[str]]]) -> None:
    """
    Upsert minimal repo rows by (repo_id, owner_org, name).
    Useful for head/base repos referenced by PRs (forks).
    """
    payload = []
    for rid, owner, nm in rows:
        if not rid:
            continue
        payload.append(
            {
                "repo_id": int(rid),
                "owner_org": owner or "unknown",
                "name": nm or f"repo_{rid}",
                "default_branch_name": "main",
                "default_branch_repo_id": int(rid),
                "default_branch_branch_name": "main",
            }
        )

    if not payload:
        return

    engine = get_engine()
    with engine.begin() as conn:
        conn.execute(
            text(
                """
                INSERT INTO repositories
                    (repo_id, owner_org, name, default_branch_name, default_branch_repo_id, default_branch_branch_name)
                VALUES
                    (:repo_id, :owner_org, :name, :default_branch_name, :default_branch_repo_id, :default_branch_branch_name)
                ON CONFLICT (repo_id) DO UPDATE SET
                    owner_org = EXCLUDED.owner_org,
                    name = EXCLUDED.name
                """
            ),
            payload,
        )


def upsert_github_users(users: Iterable[Tuple[int, str]]) -> None:
    """Upsert github users from (id, login)."""
    rows = [{"id": int(uid), "login": login} for uid, login in users if uid and login]
    if not rows:
        return

    engine = get_engine()
    with engine.begin() as conn:
        conn.execute(
            text(
                """
                INSERT INTO github_users (github_user_id, github_login)
                VALUES (:id, :login)
                ON CONFLICT (github_user_id) DO UPDATE SET
                    github_login = EXCLUDED.github_login
                """
            ),
            rows,
        )


def upsert_branches(repo_id: int, branches: List[Dict[str, Any]]) -> None:
    now_utc = datetime.now(timezone.utc)
    rows = [
        {"repo_id": repo_id, "name": b.get("name"), "first_seen_at": now_utc}
        for b in branches
        if b.get("name")
    ]
    if not rows:
        return

    engine = get_engine()
    with engine.begin() as conn:
        # Don't overwrite first_seen_at once it's there.
        conn.execute(
            text(
                """
                INSERT INTO branches (repo_id, name, first_seen_at)
                VALUES (:repo_id, :name, :first_seen_at)
                ON CONFLICT (repo_id, name) DO NOTHING
                """
            ),
            rows,
        )


def _parse_commit_author(commit_obj: Dict[str, Any]) -> Optional[int]:
    # Two possible locations:
    # - commit_obj["author"]["id"] when GitHub can link it to a user
    # - commit_obj["commit"]["author"] contains name/email but no id
    a = commit_obj.get("author")
    if isinstance(a, dict) and a.get("id"):
        return int(a["id"])
    return None


def _parse_commit_time(commit_obj: Dict[str, Any]) -> datetime:
    s = (
        (commit_obj.get("commit") or {}).get("committer", {}).get("date")
        or (commit_obj.get("commit") or {}).get("author", {}).get("date")
    )
    if not s:
        # fallback to now (should be rare)
        return datetime.now(timezone.utc)
    return datetime.fromisoformat(s.replace("Z", "+00:00"))


def upsert_commits(repo_id: int, commits: List[Dict[str, Any]]) -> None:
    rows = []
    for c in commits:
        sha = c.get("sha")
        if not sha:
            continue
        rows.append(
            {
                "repo_id": repo_id,
                "sha": sha,
                "committed_at": _parse_commit_time(c),
                "author_github_user_id": _parse_commit_author(c),
            }
        )

    if not rows:
        return

    engine = get_engine()
    with engine.begin() as conn:
        conn.execute(
            text(
                """
                INSERT INTO commits (repo_id, sha, committed_at, author_github_user_id)
                VALUES (:repo_id, :sha, :committed_at, :author_github_user_id)
                ON CONFLICT (repo_id, sha) DO UPDATE SET
                    committed_at = GREATEST(commits.committed_at, EXCLUDED.committed_at),
                    author_github_user_id = COALESCE(EXCLUDED.author_github_user_id, commits.author_github_user_id)
                """
            ),
            rows,
        )


def upsert_branch_commits(repo_id: int, mappings: List[Tuple[str, str]]) -> None:
    rows = [{"repo_id": repo_id, "branch_name": br, "sha": sha} for br, sha in mappings if br and sha]
    if not rows:
        return

    engine = get_engine()
    with engine.begin() as conn:
        conn.execute(
            text(
                """
                INSERT INTO branch_commits (repo_id, branch_name, sha)
                VALUES (:repo_id, :branch_name, :sha)
                ON CONFLICT (repo_id, branch_name, sha) DO NOTHING
                """
            ),
            rows,
        )


def _parse_pr_time(s: Optional[str]) -> Optional[datetime]:
    if not s:
        return None
    return datetime.fromisoformat(s.replace("Z", "+00:00"))


def upsert_pull_requests(repo_id: int, prs: List[Dict[str, Any]]) -> None:
    rows = []
    needed_repos = []  # (repo_id, owner_org, name)

    for pr in prs:
        number = pr.get("number")
        pr_id = pr.get("id")
        if number is None or pr_id is None:
            continue

        created = _parse_pr_time(pr.get("created_at"))
        if not created:
            continue

        user = pr.get("user") or {}
        head = pr.get("head") or {}
        base = pr.get("base") or {}
        head_repo = head.get("repo") or {}
        base_repo = base.get("repo") or {}

        head_repo_id = int(head_repo["id"]) if head_repo.get("id") else repo_id
        base_repo_id = int(base_repo["id"]) if base_repo.get("id") else repo_id

        # collect minimal info for FK safety (if FK is enforced)
        if head_repo_id:
            needed_repos.append((head_repo_id, (head_repo.get("owner") or {}).get("login"), head_repo.get("name")))
        if base_repo_id:
            needed_repos.append((base_repo_id, (base_repo.get("owner") or {}).get("login"), base_repo.get("name")))

        rows.append(
            {
                "repo_id": repo_id,
                "pr_number": int(number),
                "pr_id": int(pr_id),
                "author_github_user_id": int(user["id"]) if user.get("id") else None,
                "state": pr.get("state") or "unknown",
                "created_at": created,
                "merged_at": _parse_pr_time(pr.get("merged_at")),
                "head_repo_id": head_repo_id,
                "head_ref": head.get("ref"),
                "base_repo_id": base_repo_id,
                "base_ref": base.get("ref"),
            }
        )

    if not rows:
        return

    # make sure referenced repos exist (best effort)
    # de-dup by id
    dedup = {}
    for rid, owner, name in needed_repos:
        dedup[int(rid)] = (int(rid), owner, name)
    upsert_repositories_min(dedup.values())

    engine = get_engine()
    with engine.begin() as conn:
        conn.execute(
            text(
                """
                INSERT INTO pull_requests
                    (repo_id, pr_number, pr_id, author_github_user_id, state, created_at, merged_at,
                    head_repo_id, head_ref, base_repo_id, base_ref)
                VALUES
                    (:repo_id, :pr_number, :pr_id, :author_github_user_id, :state, :created_at, :merged_at,
                    :head_repo_id, :head_ref, :base_repo_id, :base_ref)
                ON CONFLICT (repo_id, pr_number) DO UPDATE SET
                    pr_id = EXCLUDED.pr_id,
                    author_github_user_id = COALESCE(EXCLUDED.author_github_user_id, pull_requests.author_github_user_id),
                    state = EXCLUDED.state,
                    created_at = EXCLUDED.created_at,
                    merged_at = EXCLUDED.merged_at,
                    head_repo_id = EXCLUDED.head_repo_id,
                    head_ref = EXCLUDED.head_ref,
                    base_repo_id = EXCLUDED.base_repo_id,
                    base_ref = EXCLUDED.base_ref
                """
            ),
            rows,
        )
