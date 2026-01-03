# stage_raw_load.py
from sqlalchemy import text, bindparam
from sqlalchemy.dialects.postgresql import JSONB
from db import get_engine

# نجهّز statements ثابتة (أسرع وأنظف)
STMT_CONTRIB = text("""
    INSERT INTO stage_raw_contributors (repo_owner, repo_name, user_id, payload)
    VALUES (:o, :r, :id, :payload)
    ON CONFLICT (repo_owner, repo_name, user_id)
    DO UPDATE SET payload = EXCLUDED.payload
""").bindparams(bindparam("payload", type_=JSONB))

STMT_BRANCH = text("""
    INSERT INTO stage_raw_branches (repo_owner, repo_name, branch_name, payload)
    VALUES (:o, :r, :n, :payload)
    ON CONFLICT (repo_owner, repo_name, branch_name)
    DO UPDATE SET payload = EXCLUDED.payload
""").bindparams(bindparam("payload", type_=JSONB))

STMT_COMMIT = text("""
    INSERT INTO stage_raw_commits (repo_owner, repo_name, sha, payload)
    VALUES (:o, :r, :sha, :payload)
    ON CONFLICT (repo_owner, repo_name, sha)
    DO UPDATE SET payload = EXCLUDED.payload
""").bindparams(bindparam("payload", type_=JSONB))

STMT_PULL = text("""
    INSERT INTO stage_raw_pulls (repo_owner, repo_name, pr_id, payload)
    VALUES (:o, :r, :id, :payload)
    ON CONFLICT (repo_owner, repo_name, pr_id)
    DO UPDATE SET payload = EXCLUDED.payload
""").bindparams(bindparam("payload", type_=JSONB))


def load_stage_raw_contributors(owner, repo, contributors):
    engine = get_engine()
    with engine.begin() as conn:
        for u in contributors:
            user_id = u.get("id")
            if user_id is None:
                continue
            conn.execute(STMT_CONTRIB, {"o": owner, "r": repo, "id": user_id, "payload": u})


def load_stage_raw_branches(owner, repo, branches):
    engine = get_engine()
    with engine.begin() as conn:
        for b in branches:
            name = b.get("name")
            if not name:
                continue
            conn.execute(STMT_BRANCH, {"o": owner, "r": repo, "n": name, "payload": b})


def load_stage_raw_commits(owner, repo, commits):
    engine = get_engine()
    with engine.begin() as conn:
        for c in commits:
            sha = c.get("sha")
            if not sha:
                continue
            conn.execute(STMT_COMMIT, {"o": owner, "r": repo, "sha": sha, "payload": c})


def load_stage_raw_pulls(owner, repo, pulls):
    engine = get_engine()
    with engine.begin() as conn:
        for pr in pulls:
            pr_id = pr.get("id")
            if pr_id is None:
                continue
            conn.execute(STMT_PULL, {"o": owner, "r": repo, "id": pr_id, "payload": pr})
