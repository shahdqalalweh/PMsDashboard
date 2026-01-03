import os
from sqlalchemy import create_engine

def get_engine():
    url = os.getenv("DATABASE_URL")
    if not url:
        raise Exception("DATABASE_URL is not set in environment variables")
    return create_engine(url, pool_pre_ping=True)
