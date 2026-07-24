"""Connexion PostgreSQL pour l'API FastAPI."""
import os
import psycopg2
import psycopg2.extras

PG_HOST = os.environ.get("POSTGRES_HOST", "postgres")
PG_PORT = os.environ.get("POSTGRES_PORT", "5432")
PG_DB = os.environ.get("POSTGRES_DB", "churn_db")
PG_USER = os.environ.get("POSTGRES_USER", "churn_user")
PG_PASSWORD = os.environ["POSTGRES_PASSWORD"]


def get_conn():
    return psycopg2.connect(host=PG_HOST, port=PG_PORT, dbname=PG_DB,
                             user=PG_USER, password=PG_PASSWORD)


def fetch_all(query: str, params: tuple = ()):
    with get_conn() as conn, conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(query, params)
        return cur.fetchall()


def fetch_one(query: str, params: tuple = ()):
    rows = fetch_all(query, params)
    return rows[0] if rows else None
