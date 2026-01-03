from sqlalchemy import text
from db import get_engine


#watermark

def get_last_run_at(owner: str, repo: str):
    engine = get_engine()
    with engine.begin() as conn:
        row = conn.execute(
            text("""
                SELECT last_run_at
                FROM etl_state
                WHERE repo_owner = :o AND repo_name = :r
            """),
            {"o": owner, "r": repo}
        ).fetchone()
# اول مرة بشغل فيها الفلو  ما بكون فيه row => none
        if row is None:
            conn.execute(
                text("""
                    INSERT INTO etl_state (repo_owner, repo_name, last_run_at)
                    VALUES (:o, :r, NULL)
                    ON CONFLICT (repo_owner, repo_name) DO NOTHING
                """),
                {"o": owner, "r": repo}
            )
            return None

        return row[0]

def update_last_run_at(owner: str, repo: str, new_ts):
    engine = get_engine()
    with engine.begin() as conn:
        conn.execute(
            text("""
                INSERT INTO etl_state (repo_owner, repo_name, last_run_at)
                VALUES (:o, :r, :t)
                ON CONFLICT (repo_owner, repo_name)
                DO UPDATE SET last_run_at = EXCLUDED.last_run_at
            """),
            {"o": owner, "r": repo, "t": new_ts}
        )
