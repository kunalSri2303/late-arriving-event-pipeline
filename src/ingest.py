import csv
import math
import sqlite3
import sys
from datetime import datetime
from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parent.parent
DB_PATH = ROOT_DIR / "pipeline.db"
CSV_PATH = ROOT_DIR / "data" / "sample_events.csv"

REQUIRED_COLUMNS = {
    "event_id",
    "event_time",
    "ingestion_time",
    "amount",
}


def parse_timestamp(value: str) -> datetime:
    if not value or not isinstance(value, str):
        raise ValueError("Timestamp string is missing or invalid")
    cleaned = value.strip()
    timestamp = datetime.fromisoformat(cleaned)
    if timestamp.tzinfo is not None:
        timestamp = timestamp.astimezone().replace(tzinfo=None)
    return timestamp


def ingest_events(
    csv_path=None,
    db_path=None,
    late_threshold_seconds=None,
) -> dict:
    target_csv = Path(csv_path) if csv_path else CSV_PATH
    target_db = Path(db_path) if db_path else DB_PATH

    read_count = 0
    inserted_count = 0
    duplicate_count = 0
    invalid_count = 0
    late_count = 0
    conflicting_duplicate_count = 0

    rejected_records = []
    late_records = []
    affected_dates = set()

    if not target_csv.exists():
        raise FileNotFoundError(f"Input CSV file not found: {target_csv}")

    conn = sqlite3.connect(target_db)
    try:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS raw_events (
                event_id TEXT PRIMARY KEY,
                event_time TEXT NOT NULL,
                ingestion_time TEXT NOT NULL,
                amount REAL NOT NULL CHECK (amount >= 0),
                event_date TEXT NOT NULL
            )
            """
        )

        with target_csv.open("r", newline="", encoding="utf-8") as file:
            reader = csv.DictReader(file)

            if not reader.fieldnames or not REQUIRED_COLUMNS.issubset(
                set(reader.fieldnames)
            ):
                missing = REQUIRED_COLUMNS - set(reader.fieldnames or [])
                raise ValueError(f"CSV missing required columns: {missing}")

            for row_idx, row in enumerate(reader, start=1):
                read_count += 1
                try:
                    event_id = row.get("event_id", "").strip()
                    if not event_id:
                        raise ValueError("Event ID is required and cannot be empty")

                    raw_event_time = row.get("event_time", "")
                    raw_ingestion_time = row.get("ingestion_time", "")
                    raw_amount = row.get("amount", "")

                    event_time = parse_timestamp(raw_event_time)
                    ingestion_time = parse_timestamp(raw_ingestion_time)

                    amount = float(raw_amount)
                    if not math.isfinite(amount) or amount < 0:
                        raise ValueError(f"Amount must be finite and non-negative, got: {raw_amount}")

                    event_date = event_time.date().isoformat()
                    ingestion_date = ingestion_time.date().isoformat()
                    event_time_str = event_time.isoformat()
                    ingestion_time_str = ingestion_time.isoformat()

                    existing = conn.execute(
                        "SELECT event_time, ingestion_time, amount FROM raw_events WHERE event_id = ?",
                        (event_id,),
                    ).fetchone()

                    if existing:
                        existing_event_time, existing_ingestion_time, existing_amount = existing
                        if (
                            existing_event_time != event_time_str
                            or abs(existing_amount - amount) > 1e-6
                        ):
                            conflicting_duplicate_count += 1
                            duplicate_count += 1
                            rejected_records.append({
                                "row": row_idx,
                                "event_id": event_id,
                                "reason": f"Conflicting payload for existing event_id {event_id}. Existing amount={existing_amount}, new amount={amount}"
                            })
                        else:
                            duplicate_count += 1
                        continue

                    time_diff = (ingestion_time - event_time).total_seconds()
                    
                    if late_threshold_seconds is not None:
                        is_late = time_diff > late_threshold_seconds
                    else:
                        is_late = event_date < ingestion_date

                    latest_db_event = conn.execute(
                        "SELECT MAX(event_time) FROM raw_events"
                    ).fetchone()[0]
                    if latest_db_event and event_time_str < latest_db_event:
                        is_late = True

                    conn.execute(
                        """
                        INSERT INTO raw_events
                        (event_id, event_time, ingestion_time, amount, event_date)
                        VALUES (?, ?, ?, ?, ?)
                        """,
                        (
                            event_id,
                            event_time_str,
                            ingestion_time_str,
                            amount,
                            event_date,
                        ),
                    )

                    inserted_count += 1
                    affected_dates.add(event_date)

                    if is_late:
                        late_count += 1
                        late_records.append({
                            "event_id": event_id,
                            "event_time": event_time_str,
                            "ingestion_time": ingestion_time_str,
                            "event_date": event_date,
                        })

                except Exception as exc:
                    invalid_count += 1
                    rejected_records.append({
                        "row": row_idx,
                        "row_data": row,
                        "reason": str(exc),
                    })

        conn.commit()
    finally:
        conn.close()

    summary = {
        "read": read_count,
        "inserted": inserted_count,
        "duplicate": duplicate_count,
        "conflicting_duplicates": conflicting_duplicate_count,
        "rejected": invalid_count,
        "invalid": invalid_count,
        "late": late_count,
        "affected_dates": sorted(list(affected_dates)),
        "rejected_details": rejected_records,
        "late_details": late_records,
    }

    print("--- Ingestion Summary ---")
    print(f"Read:                  {read_count}")
    print(f"Inserted:              {inserted_count}")
    print(f"Duplicates:            {duplicate_count}")
    print(f"Conflicting Duplicates:{conflicting_duplicate_count}")
    print(f"Rejected / Invalid:    {invalid_count}")
    print(f"Late-Arriving Events:  {late_count}")
    if rejected_records:
        print("\nRejected Records Details:")
        for r in rejected_records:
            print(f"  Row {r.get('row')}: {r.get('reason')}")
    if late_records:
        print("\nLate Events Details:")
        for l in late_records:
            print(f"  Event {l.get('event_id')} (event_time: {l.get('event_time')}, ingested: {l.get('ingestion_time')})")

    return summary


if __name__ == "__main__":
    csv_file = sys.argv[1] if len(sys.argv) > 1 else None
    db_file = sys.argv[2] if len(sys.argv) > 2 else None
    ingest_events(csv_path=csv_file, db_path=db_file)
