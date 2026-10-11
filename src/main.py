import argparse
import sys
from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parent.parent
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from src.init_db import initialize_database
from src.ingest import ingest_events
from src.aggregate import update_daily_aggregates
from src.reconcile import reconcile_pipeline
from src.ai_summary import generate_ai_run_summary


def run_pipeline(csv_path=None, db_path=None, init_db=False) -> dict:
    target_db = Path(db_path) if db_path else ROOT_DIR / "pipeline.db"
    target_csv = Path(csv_path) if csv_path else ROOT_DIR / "data" / "sample_events.csv"

    print("==================================================")
    print("      LATE-ARRIVING EVENT PIPELINE RUNNER        ")
    print("==================================================")

    pipeline_success = True
    failure_stage = None
    failure_reason = None

    # Step 1: Database Check / Explicit Initialization
    if init_db or not target_db.exists():
        print(f"\n[Step 1/4] Initializing Database Schema: {target_db}")
        try:
            initialize_database(db_path=target_db)
        except Exception as exc:
            print(f"\n[ERROR] Database initialization failed: {exc}")
            pipeline_success = False
            failure_stage = "Initialization"
            failure_reason = str(exc)
    else:
        print(f"\n[Step 1/4] Database exists ({target_db.name}). Preserving existing data.")

    # Step 2: CSV Ingestion
    ingest_summary = {}
    if pipeline_success:
        print(f"\n[Step 2/4] Ingesting CSV Data: {target_csv.name}")
        try:
            ingest_summary = ingest_events(csv_path=target_csv, db_path=target_db)
        except Exception as exc:
            print(f"\n[ERROR] Ingestion failed: {exc}")
            pipeline_success = False
            failure_stage = "Ingestion"
            failure_reason = str(exc)
    else:
        print("\n[Step 2/4] Ingestion: SKIPPED (prior stage failed)")

    # Step 3: Daily Aggregates Recalculation
    agg_results = []
    aggregation_status = "SKIPPED"
    if pipeline_success:
        print("\n[Step 3/4] Updating Daily Aggregates...")
        try:
            # We can recalculate affected dates or all dates; defaulting to all dates ensures
            # complete consistency across the historical database.
            agg_results = update_daily_aggregates(db_path=target_db)
            aggregation_status = "SUCCESS"
        except Exception as exc:
            print(f"\n[ERROR] Aggregation failed: {exc}")
            pipeline_success = False
            failure_stage = "Aggregation"
            failure_reason = str(exc)
            aggregation_status = "FAILED"
    else:
        print("\n[Step 3/4] Aggregation: SKIPPED (prior stage failed)")

    # Step 4: Data Reconciliation
    reconciliation = {"reconciled": False, "discrepancies": []}
    reconciliation_status = "SKIPPED"
    if pipeline_success:
        print("\n[Step 4/4] Running Data Reconciliation Check...")
        try:
            reconciliation = reconcile_pipeline(db_path=target_db)
            if not reconciliation.get("reconciled", False):
                pipeline_success = False
                failure_stage = "Reconciliation"
                failure_reason = "Discrepancies found between raw_events and daily_aggregates"
                reconciliation_status = "FAILED"
            else:
                reconciliation_status = "SUCCESS"
        except Exception as exc:
            print(f"\n[ERROR] Reconciliation check encountered an error: {exc}")
            pipeline_success = False
            failure_stage = "Reconciliation"
            failure_reason = str(exc)
            reconciliation_status = "FAILED"
    else:
        print("\n[Step 4/4] Reconciliation: SKIPPED (prior stage failed)")

    overall_status = "SUCCESS" if pipeline_success else "FAILED"

    # Operational summary generation
    summary_text = generate_ai_run_summary(
        ingest_summary=ingest_summary,
        agg_results=agg_results,
        reconciliation=reconciliation,
        overall_status=overall_status,
        aggregation_status=aggregation_status,
        reconciliation_status=reconciliation_status,
        failure_stage=failure_stage,
        failure_reason=failure_reason,
    )

    print("\n" + "=" * 50)
    print(summary_text)
    print("=" * 50 + "\n")

    return {
        "success": pipeline_success,
        "status": overall_status,
        "failure_stage": failure_stage,
        "failure_reason": failure_reason,
        "ingest": ingest_summary,
        "aggregation_status": aggregation_status,
        "aggregate": agg_results,
        "reconciliation_status": reconciliation_status,
        "reconciliation": reconciliation,
        "summary_text": summary_text,
    }


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run Late-Arriving Event Pipeline")
    parser.add_argument("--csv", help="Path to input CSV file", default=None)
    parser.add_argument("--db", help="Path to SQLite database", default=None)
    parser.add_argument("--init", help="Force database schema initialization", action="store_true")
    args = parser.parse_args()

    result = run_pipeline(csv_path=args.csv, db_path=args.db, init_db=args.init)
    if not result.get("success", False):
        sys.exit(1)
    sys.exit(0)
