import os
from pathlib import Path

try:
    import psycopg2  # type: ignore
except ImportError:  # pragma: no cover
    psycopg2 = None

try:
    import pg8000  # type: ignore
except ImportError:  # pragma: no cover
    pg8000 = None


def _db_connection():
    database_url = os.environ.get("DATABASE_URL")
    if database_url:
        if psycopg2 is not None:
            return psycopg2.connect(database_url)
        if pg8000 is not None:
            return pg8000.connect(database_url)
        raise RuntimeError("Missing database client (psycopg2 or pg8000).")

    host = os.environ.get("DB_HOST")
    user = os.environ.get("DB_USER")
    password = os.environ.get("DB_PASSWORD")
    db_name = os.environ.get("DB_NAME")
    port = int(os.environ.get("DB_PORT", "5432"))
    if not all([host, user, password, db_name]):
        raise RuntimeError("Missing DB_* environment variables.")
    if psycopg2 is not None:
        return psycopg2.connect(host=host, user=user, password=password, port=port, dbname=db_name)
    if pg8000 is not None:
        return pg8000.connect(host=host, user=user, password=password, port=port, database=db_name)
    raise RuntimeError("Missing database client (psycopg2 or pg8000).")


def _ensure_migrations_table(cursor):
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS anyapi_app.schema_migrations (
            version TEXT PRIMARY KEY,
            applied_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        )
        """
    )


def _applied_versions(cursor):
    cursor.execute("SELECT version FROM anyapi_app.schema_migrations")
    return {row[0] for row in cursor.fetchall()}


def _apply_migration(cursor, version: str, sql: str):
    cursor.execute(sql)
    cursor.execute(
        "INSERT INTO anyapi_app.schema_migrations (version) VALUES (%s)",
        (version,),
    )


def main():
    migrations_dir = Path(__file__).parent
    migration_files = sorted(migrations_dir.glob("*.sql"))
    if not migration_files:
        print("No migrations found.")
        return

    conn = _db_connection()
    conn.autocommit = False
    try:
        with conn.cursor() as cursor:
            _ensure_migrations_table(cursor)
            applied = _applied_versions(cursor)
            for migration in migration_files:
                version = migration.stem
                if version in applied:
                    continue
                sql = migration.read_text()
                _apply_migration(cursor, version, sql)
        conn.commit()
        print("Migrations applied.")
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


if __name__ == "__main__":
    main()
