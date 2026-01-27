from datetime import datetime
from typing import Optional
from sqlalchemy import text
from db import get_engine


def ensure_schema():
    engine = get_engine()
    with open("schema.sql", "r", encoding="utf-8") as f:
        ddl = f.read()

    with engine.begin() as conn:
        for stmt in ddl.split(";"):
            if stmt.strip():
                conn.execute(text(stmt))


def get_since_ts(repo_id: int) -> Optional[datetime]:
    engine = get_engine()
    with engine.begin() as conn:
        row = conn.execute(
            text("SELECT MAX(committed_at) AS ts FROM commits WHERE repo_id = :rid"),
            {"rid": repo_id}
        ).mappings().first()
    return row["ts"] if row and row["ts"] else None


def upsert_repository(repo_meta, owner, name):
    repo_id = repo_meta["id"]

    engine = get_engine()
    with engine.begin() as conn:
        conn.execute(
            text("""
                INSERT INTO repo (repo_id, owner, name)
                VALUES (:repo_id, :owner, :name)
                ON CONFLICT (repo_id) DO UPDATE SET
                    owner = EXCLUDED.owner,
                    name = EXCLUDED.name
            """),
            {"repo_id": repo_id, "owner": owner, "name": name}
        )

    return repo_id


def upsert_branches(repo_id, branches):
    rows = []
    for b in branches:
        rows.append({
            "repo_id": repo_id,
            "name": b["name"],
            "sha": b["commit"]["sha"],
            "is_main": b["name"] == "main"
        })

    engine = get_engine()
    with engine.begin() as conn:
        conn.execute(
            text("""
                INSERT INTO branches (repo_id, name, sha, is_main)
                VALUES (:repo_id, :name, :sha, :is_main)
                ON CONFLICT (repo_id, name) DO UPDATE SET
                    sha = EXCLUDED.sha,
                    is_main = EXCLUDED.is_main
            """),
            rows
        )


def upsert_commits(repo_id, commits):
    rows = []
    for c in commits:
        rows.append({
            "repo_id": repo_id,
            "sha": c["sha"],
            "committed_at": c["commit"]["author"]["date"]
        })

    if not rows:
        return

    engine = get_engine()
    with engine.begin() as conn:
        conn.execute(
            text("""
                INSERT INTO commits (repo_id, sha, committed_at)
                VALUES (:repo_id, :sha, :committed_at)
                ON CONFLICT (repo_id, sha) DO NOTHING
            """),
            rows
        )


def upsert_pulls(repo_id, pulls):
    rows = []
    for pr in pulls:
        rows.append({
            "pull_id": pr["id"],
            "repo_id": repo_id,
            "number": pr["number"],
            "state": pr["state"],
            "merged": pr["merged_at"] is not None,
            "created_at": pr["created_at"],
            "merged_at": pr["merged_at"],
            "updated_at": pr["updated_at"]
        })

    if not rows:
        return

    engine = get_engine()
    with engine.begin() as conn:
        conn.execute(
            text("""
                INSERT INTO pulls
                (pull_id, repo_id, number, state, merged, created_at, merged_at, updated_at)
                VALUES
                (:pull_id, :repo_id, :number, :state, :merged, :created_at, :merged_at, :updated_at)
                ON CONFLICT (pull_id) DO UPDATE SET
                    state = EXCLUDED.state,
                    merged = EXCLUDED.merged,
                    merged_at = EXCLUDED.merged_at,
                    updated_at = EXCLUDED.updated_at
            """),
            rows
        )
