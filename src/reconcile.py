import sqlite3
import sys
from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parent.parent
DB_PATH = ROOT_DIR / "pipeline.db"


def reconcile_pipeline(db_path=None) -> dict:
    target_db = Path(db_path) if db_path else DB_PATH

    conn = sqlite3.connect(target_db)
    try:
        tables = [
            r[0]
            for r in conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table'"
            ).fetchall()
        ]
        if "raw_events" not in tables or "daily_aggregates" not in tables:
            missing = [t for t in ["raw_events", "daily_aggregates"] if t not in tables]
            print(f"ERROR: Missing database tables: {missing}")
            return {
                "reconciled": False,
                "discrepancies": [f"Missing required database tables: {missing}"],
                "raw_metrics": {},
                "agg_metrics": {},
            }

        raw_metrics = dict()
        for row in conn.execute(
            """
            SELECT
                event_date,
                COUNT(*) AS cnt,
                ROUND(SUM(amount), 2) AS amt
            FROM raw_events
            GROUP BY event_date
            """
        ).fetchall():
            raw_metrics[row[0]] = {"orders": row[1], "amount": round(row[2], 2)}

        agg_metrics = dict()
        for row in conn.execute(
            """
            SELECT event_date, total_orders, total_amount
            FROM daily_aggregates
            """
        ).fetchall():
            agg_metrics[row[0]] = {"orders": row[1], "amount": round(row[2], 2)}
    finally:
        conn.close()

    all_dates = sorted(set(raw_metrics.keys()) | set(agg_metrics.keys()))
    discrepancies = []

    for d in all_dates:
        raw = raw_metrics.get(d)
        agg = agg_metrics.get(d)

        if not raw:
            discrepancies.append(f"Date {d} exists in daily_aggregates but not in raw_events")
        elif not agg:
            discrepancies.append(f"Date {d} exists in raw_events but missing in daily_aggregates")
        else:
            if raw["orders"] != agg["orders"] or abs(raw["amount"] - agg["amount"]) > 1e-4:
                discrepancies.append(
                    f"Mismatch for date {d}: raw(orders={raw['orders']}, amount={raw['amount']}) "
                    f"vs agg(orders={agg['orders']}, amount={agg['amount']})"
                )

    is_reconciled = len(discrepancies) == 0

    print("--- Reconciliation Report ---")
    if is_reconciled:
        print("SUCCESS: daily_aggregates perfectly match raw_events calculations!")
    else:
        print("WARNING: Reconciliation discrepancies found:")
        for disc in discrepancies:
            print(f"  - {disc}")

    return {
        "reconciled": is_reconciled,
        "discrepancies": discrepancies,
        "raw_metrics": raw_metrics,
        "agg_metrics": agg_metrics,
    }


if __name__ == "__main__":
    db_file = sys.argv[1] if len(sys.argv) > 1 else None
    result = reconcile_pipeline(db_path=db_file)
    if not result.get("reconciled", False):
        sys.exit(1)

