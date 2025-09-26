# database/db.py
import os
import psycopg2
from psycopg2.extras import RealDictCursor
from contextlib import contextmanager

# Expect DATABASE_URL to be a full Postgres DSN, e.g.
# postgresql://username:password@hostname:5432/databasename
DATABASE_URL = os.getenv("DATABASE_URL")
if not DATABASE_URL:
    raise RuntimeError("DATABASE_URL environment variable is not set")

def connect(database_url: str = DATABASE_URL):
    """Return a Postgres connection using psycopg2."""
    return psycopg2.connect(database_url)

@contextmanager
def get_conn(database_url: str = DATABASE_URL):
    conn = connect(database_url)
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()

def get_cursor(conn):
    """Always use RealDictCursor so rows come back as dicts."""
    return conn.cursor(cursor_factory=RealDictCursor)
