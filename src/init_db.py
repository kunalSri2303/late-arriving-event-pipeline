import sqlite3
from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parent.parent
DB_PATH = ROOT_DIR / "pipeline.db"
SCHEMA_PATH = ROOT_DIR / "sql" / "schema.sql"


def initialize_database(db_path=None):
    target_path = Path(db_path) if db_path else DB_PATH
    conn = sqlite3.connect(target_path)
    try:
        schema = SCHEMA_PATH.read_text(encoding="utf-8")
        conn.executescript(schema)
    finally:
        conn.close()

    print(f"Database initialized successfully: {target_path}")
    return target_path


if __name__ == "__main__":
    initialize_database()
