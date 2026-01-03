from sqlalchemy import text
from datetime import datetime
from db import get_engine

def _parse_iso(ts):
    if not ts:
        return None
    return datetime.fromisoformat(ts.replace("Z", "+00:00"))

def upsert_users_from_contributors(contributors):
    engine = get_engine()
    with engine.begin() as conn:
        for u in contributors:
            user_id = u.get("id")
            if user_id is None:
                continue
            conn.execute(
                text("""
                    INSERT INTO normalized_users (user_id, login, avatar_url, html_url, type)
                    VALUES (:id, :login, :avatar, :html, :type)
                    ON CONFLICT (user_id)
                    DO UPDATE SET
                        login = EXCLUDED.login,
                        avatar_url = EXCLUDED.avatar_url,
                        html_url = EXCLUDED.html_url,
                        type = EXCLUDED.type
                """),
                {
                    "id": user_id,
                    "login": u.get("login"),
                    "avatar": u.get("avatar_url"),
                    "html": u.get("html_url"),
                    "type": u.get("type"),
                }
            )

def upsert_repo_users(owner, repo, contributors):
    engine = get_engine()
    with engine.begin() as conn:
        for u in contributors:
            user_id = u.get("id")
            if user_id is None:
                continue
            conn.execute(
                text("""
                    INSERT INTO normalized_repo_users (repo_owner, repo_name, user_id, contributions)
                    VALUES (:o, :r, :id, :c)
                    ON CONFLICT (repo_owner, repo_name, user_id)
                    DO UPDATE SET contributions = EXCLUDED.contributions
                """),
                {"o": owner, "r": repo, "id": user_id, "c": u.get("contributions")}
            )

def upsert_branches(owner, repo, branches):
    engine = get_engine()
    with engine.begin() as conn:
        for b in branches:
            name = b.get("name")
            if not name:
                continue
            head_sha = None
            if b.get("commit") and isinstance(b["commit"], dict):
                head_sha = b["commit"].get("sha")
            conn.execute(
                text("""
                    INSERT INTO normalized_branches (repo_owner, repo_name, branch_name, head_sha)
                    VALUES (:o, :r, :n, :sha)
                    ON CONFLICT (repo_owner, repo_name, branch_name)
                    DO UPDATE SET head_sha = EXCLUDED.head_sha
                """),
                {"o": owner, "r": repo, "n": name, "sha": head_sha}
            )

def upsert_commits(owner, repo, commits):
    engine = get_engine()
    with engine.begin() as conn:
        for c in commits:
            sha = c.get("sha")
            if not sha:
                continue

            # author قد يكون null
            author_user_id = None
            author_login = None
            if c.get("author") and isinstance(c["author"], dict):
                author_user_id = c["author"].get("id")
                author_login = c["author"].get("login")

                # upsert user من هون كمان (اختياري لكنه مفيد)
                conn.execute(
                    text("""
                        INSERT INTO normalized_users (user_id, login, avatar_url, html_url, type)
                        VALUES (:id, :login, :avatar, :html, :type)
                        ON CONFLICT (user_id)
                        DO UPDATE SET login = EXCLUDED.login
                    """),
                    {
                        "id": author_user_id,
                        "login": author_login,
                        "avatar": c["author"].get("avatar_url"),
                        "html": c["author"].get("html_url"),
                        "type": c["author"].get("type"),
                    }
                )

            commit_obj = c.get("commit") or {}
            message = commit_obj.get("message")
            commit_date = _parse_iso((commit_obj.get("author") or {}).get("date"))

            conn.execute(
                text("""
                    INSERT INTO normalized_commits
                        (repo_owner, repo_name, sha, author_user_id, author_login, commit_date, message)
                    VALUES
                        (:o, :r, :sha, :uid, :login, :d, :m)
                    ON CONFLICT (repo_owner, repo_name, sha)
                    DO UPDATE SET
                        author_user_id = EXCLUDED.author_user_id,
                        author_login = EXCLUDED.author_login,
                        commit_date = EXCLUDED.commit_date,
                        message = EXCLUDED.message
                """),
                {"o": owner, "r": repo, "sha": sha, "uid": author_user_id, "login": author_login, "d": commit_date, "m": message}
            )

def upsert_pulls(owner, repo, pulls):
    engine = get_engine()
    with engine.begin() as conn:
        for pr in pulls:
            pr_id = pr.get("id")
            if pr_id is None:
                continue

            user_id = None
            user_login = None
            if pr.get("user") and isinstance(pr["user"], dict):
                user_id = pr["user"].get("id")
                user_login = pr["user"].get("login")

                # upsert user
                if user_id is not None:
                    conn.execute(
                        text("""
                            INSERT INTO normalized_users (user_id, login, avatar_url, html_url, type)
                            VALUES (:id, :login, :avatar, :html, :type)
                            ON CONFLICT (user_id)
                            DO UPDATE SET login = EXCLUDED.login
                        """),
                        {
                            "id": user_id,
                            "login": user_login,
                            "avatar": pr["user"].get("avatar_url"),
                            "html": pr["user"].get("html_url"),
                            "type": pr["user"].get("type"),
                        }
                    )

            conn.execute(
                text("""
                    INSERT INTO normalized_pull_requests
                        (repo_owner, repo_name, pr_id, number, state, title, user_id, user_login,created_at, updated_at, closed_at, merged_at)
                    
                    VALUES
                        (:o, :r, :id, :num, :st, :t, :uid, :ulogin, :ca, :ua, :cla, :ma)
                    ON CONFLICT (repo_owner, repo_name, pr_id)
                    DO UPDATE SET
                        number = EXCLUDED.number,
                        state = EXCLUDED.state,
                        title = EXCLUDED.title,
                        user_id = EXCLUDED.user_id,
                        user_login = EXCLUDED.user_login,
                        created_at = EXCLUDED.created_at,
                        updated_at = EXCLUDED.updated_at,
                        closed_at = EXCLUDED.closed_at,
                        merged_at = EXCLUDED.merged_at
                """),
                {
                    "o": owner, "r": repo, "id": pr_id,
                    "num": pr.get("number"),
                    "st": pr.get("state"),
                    "t": pr.get("title"),
                    "uid": user_id,
                    "ulogin": user_login,
                    "ca": _parse_iso(pr.get("created_at")),
                    "ua": _parse_iso(pr.get("updated_at")),
                    "cla": _parse_iso(pr.get("closed_at")),
                    "ma": _parse_iso(pr.get("merged_at")),
                }
            )
