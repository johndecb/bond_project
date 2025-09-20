# database/db.py
import os, psycopg2
from contextlib import contextmanager

def connect(database_url=None):
    if database_url is None:
        database_url = os.getenv("DATABASE_URL")
        if not database_url:
            raise RuntimeError("DATABASE_URL not set")
    return psycopg2.connect(database_url)

@contextmanager
def get_conn(database_url=None):
    conn = connect(database_url)
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()
