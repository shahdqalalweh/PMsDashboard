import os
from sqlalchemy import create_engine, text

DATABASE_URL = os.getenv("DATABASE_URL")
if not DATABASE_URL:
    raise Exception("DATABASE_URL is not set")

engine = create_engine(DATABASE_URL)

def load_metrics(devs, branches, commits, pulls):
    with engine.begin() as conn:
        conn.execute(text("""
            INSERT INTO github_metrics
            (developers, branches, commits, pulls)
            VALUES (:d, :b, :c, :p)
        """), {
            "d": devs,
            "b": branches,
            "c": commits,
            "p": pulls
        })
