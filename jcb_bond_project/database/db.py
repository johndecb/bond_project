# database/db.py
import os, psycopg2
from contextlib import contextmanager

def connect(database_url: str):
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
