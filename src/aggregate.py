import sqlite3
import sys
from datetime import datetime
from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parent.parent
DB_PATH = ROOT_DIR / "pipeline.db"


def update_daily_aggregates(db_path=None, affected_dates=None) -> list:
    target_db = Path(db_path) if db_path else DB_PATH
    now = datetime.now().isoformat(timespec="seconds")

    conn = sqlite3.connect(target_db)
    try:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS daily_aggregates (
                event_date TEXT PRIMARY KEY,
                total_orders INTEGER NOT NULL DEFAULT 0,
                total_amount REAL NOT NULL DEFAULT 0,
                last_updated TEXT NOT NULL
            )
            """
        )

        query = """
            SELECT
                event_date,
                COUNT(*) AS total_orders,
                ROUND(SUM(amount), 2) AS total_amount
            FROM raw_events
        """
        params = []

        if affected_dates:
            dates_list = list(affected_dates)
            placeholders = ",".join("?" for _ in dates_list)
            query += f" WHERE event_date IN ({placeholders})"
            params.extend(dates_list)

        query += " GROUP BY event_date ORDER BY event_date"

        daily_results = conn.execute(query, params).fetchall()

        conn.execute("BEGIN")
        try:
            for event_date, total_orders, total_amount in daily_results:
                conn.execute(
                    """
                    INSERT INTO daily_aggregates (
                        event_date,
                        total_orders,
                        total_amount,
                        last_updated
                    )
                    VALUES (?, ?, ?, ?)
                    ON CONFLICT(event_date) DO UPDATE SET
                        total_orders = excluded.total_orders,
                        total_amount = excluded.total_amount,
                        last_updated = excluded.last_updated
                    """,
                    (event_date, total_orders, round(total_amount, 2), now),
                )
            conn.commit()
        except Exception:
            conn.rollback()
            raise
    finally:
        conn.close()

    print("--- Aggregation Summary ---")
    for event_date, total_orders, total_amount in daily_results:
        print(
            f"{event_date}: "
            f"orders={total_orders}, amount={total_amount:.2f}"
        )

    return daily_results


if __name__ == "__main__":
    db_file = sys.argv[1] if len(sys.argv) > 1 else None
    update_daily_aggregates(db_path=db_file)
