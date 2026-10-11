import csv
import os
import sqlite3
import sys
import tempfile
from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parent.parent
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from src.init_db import initialize_database
from src.ingest import ingest_events
from src.aggregate import update_daily_aggregates
from src.reconcile import reconcile_pipeline


def print_header(title: str):
    print("\n" + "=" * 70)
    print(f" {title.upper()} ")
    print("=" * 70)


def print_table(title: str, rows: list):
    print(f"\n--- {title} ---")
    print(f"{'Event Date':<15} | {'Orders':<10} | {'Total Amount':<15}")
    print("-" * 46)
    for r in rows:
        print(f"{r[0]:<15} | {r[1]:<10} | ${r[2]:<14.2f}")
    if not rows:
        print(" (No records)")


def run_demonstration():
    with tempfile.TemporaryDirectory() as temp_dir:
        demo_db = Path(temp_dir) / "demo_pipeline.db"
        normal_csv = Path(temp_dir) / "normal_events.csv"
        late_csv = Path(temp_dir) / "late_events.csv"
        conflict_csv = Path(temp_dir) / "conflicting_events.csv"

        print_header("Bytespoke DAI-040: Late-Arriving Event Pipeline Demonstration")
        print(f"Working in isolated temporary database: {demo_db.name}")

        # Initialize schema
        initialize_database(db_path=demo_db)

        # -------------------------------------------------------------
        # STEP 1: Process Normal Events (Before Late Event Arrives)
        # -------------------------------------------------------------
        print_header("Step 1: Normal Event Processing (Before Late Event Arrives)")
        with open(normal_csv, "w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow(["event_id", "event_time", "ingestion_time", "amount"])
            writer.writerow(["evt_001", "2026-10-10T09:00:00", "2026-10-10T09:05:00", "500.0"])
            writer.writerow(["evt_002", "2026-10-10T10:30:00", "2026-10-10T10:35:00", "600.0"])
            writer.writerow(["evt_003", "2026-10-11T08:00:00", "2026-10-11T08:05:00", "1000.0"])

        ingest_summary_1 = ingest_events(csv_path=normal_csv, db_path=demo_db)
        agg_results_1 = update_daily_aggregates(db_path=demo_db)

        print_table("Initial Daily Aggregates (Pre-Correction)", agg_results_1)

        conn = sqlite3.connect(demo_db)
        oct10_initial = conn.execute(
            "SELECT total_orders, total_amount FROM daily_aggregates WHERE event_date = '2026-10-10'"
        ).fetchone()
        oct11_initial = conn.execute(
            "SELECT total_orders, total_amount FROM daily_aggregates WHERE event_date = '2026-10-11'"
        ).fetchone()
        conn.close()

        init_orders_10, init_amt_10 = oct10_initial[0], oct10_initial[1]
        print(f"\nBaseline for 2026-10-10 recorded: Orders={init_orders_10}, Total Revenue=${init_amt_10:.2f}")

        # -------------------------------------------------------------
        # STEP 2: Ingest Late-Arriving Event
        # -------------------------------------------------------------
        print_header("Step 2: Ingest Late-Arriving Event (evt_004)")
        print("Event Details:")
        print("  • ID:             evt_004")
        print("  • event_time:     2026-10-10T14:00:00  (Occurred Oct 10)")
        print("  • ingestion_time: 2026-10-11T09:00:00  (Arrived Oct 11 - NEXT DAY)")
        print("  • amount:         $500.00")

        with open(late_csv, "w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow(["event_id", "event_time", "ingestion_time", "amount"])
            writer.writerow(["evt_004", "2026-10-10T14:00:00", "2026-10-11T09:00:00", "500.0"])

        ingest_summary_2 = ingest_events(csv_path=late_csv, db_path=demo_db)
        print(f"Late event detected: {ingest_summary_2['late']} event(s)")

        # Verify event_date assignment in raw_events
        conn = sqlite3.connect(demo_db)
        evt4_row = conn.execute(
            "SELECT event_id, event_time, ingestion_time, amount, event_date FROM raw_events WHERE event_id = 'evt_004'"
        ).fetchone()
        conn.close()

        print(f"Stored in raw_events: id={evt4_row[0]}, event_date={evt4_row[4]} (strictly from event_time, NOT ingestion_time)")

        # Recalculate historical date
        print("\nRecalculating affected historical dates...")
        agg_results_2 = update_daily_aggregates(db_path=demo_db)
        print_table("Corrected Daily Aggregates (Post-Correction)", agg_results_2)

        conn = sqlite3.connect(demo_db)
        oct10_corrected = conn.execute(
            "SELECT total_orders, total_amount FROM daily_aggregates WHERE event_date = '2026-10-10'"
        ).fetchone()
        oct11_corrected = conn.execute(
            "SELECT total_orders, total_amount FROM daily_aggregates WHERE event_date = '2026-10-11'"
        ).fetchone()
        conn.close()

        corr_orders_10, corr_amt_10 = oct10_corrected[0], oct10_corrected[1]
        order_delta = corr_orders_10 - init_orders_10
        revenue_delta = corr_amt_10 - init_amt_10

        print("\n--- Correction Quantified for 2026-10-10 ---")
        print(f"• Pre-Correction:   {init_orders_10} orders, ${init_amt_10:.2f}")
        print(f"• Post-Correction:  {corr_orders_10} orders, ${corr_amt_10:.2f}")
        print(f"• Quantified Delta: +{order_delta} order, +${revenue_delta:.2f} revenue")
        print(f"• 2026-10-11 Date:  {oct11_corrected[0]} orders, ${oct11_corrected[1]:.2f} (Remained unchanged!)")

        # Reconciliation check
        print("\nVerifying data integrity with reconciliation...")
        rec_result = reconcile_pipeline(db_path=demo_db)
        assert rec_result["reconciled"], "Reconciliation failed after correction!"

        # -------------------------------------------------------------
        # STEP 3: Verify Idempotency & Duplicate Handling
        # -------------------------------------------------------------
        print_header("Step 3: Idempotency & Duplicate Handling Verification")

        print("\n[Test 3.1] Repeated Ingestion of the Same Events:")
        repeat_summary = ingest_events(csv_path=late_csv, db_path=demo_db)
        print(f"  • Inserted: {repeat_summary['inserted']} (Expected 0)")
        print(f"  • Duplicates: {repeat_summary['duplicate']} (Expected 1)")
        assert repeat_summary["inserted"] == 0
        assert repeat_summary["duplicate"] == 1

        print("\n[Test 3.2] Conflicting Duplicate ID Payload:")
        with open(conflict_csv, "w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow(["event_id", "event_time", "ingestion_time", "amount"])
            writer.writerow(["evt_001", "2026-10-10T09:00:00", "2026-10-10T09:05:00", "9999.0"])

        conflict_summary = ingest_events(csv_path=conflict_csv, db_path=demo_db)
        conn = sqlite3.connect(demo_db)
        evt1_amt = conn.execute("SELECT amount FROM raw_events WHERE event_id = 'evt_001'").fetchone()[0]
        conn.close()
        print(f"  • Conflicting duplicate detected and rejected: {conflict_summary['conflicting_duplicates']}")
        print(f"  • Ground truth in raw_events preserved: ${evt1_amt:.2f} (NOT overwritten to $9999.00)")
        assert evt1_amt == 500.0

        print("\n[Test 3.3] Repeated Aggregation Idempotency:")
        repeat_agg = update_daily_aggregates(db_path=demo_db)
        conn = sqlite3.connect(demo_db)
        final_10 = conn.execute("SELECT total_orders, total_amount FROM daily_aggregates WHERE event_date = '2026-10-10'").fetchone()
        final_11 = conn.execute("SELECT total_orders, total_amount FROM daily_aggregates WHERE event_date = '2026-10-11'").fetchone()
        conn.close()
        print(f"  • 2026-10-10 totals: {final_10[0]} orders, ${final_10[1]:.2f}")
        print(f"  • 2026-10-11 totals: {final_11[0]} orders, ${final_11[1]:.2f}")
        assert final_10[0] == 3 and final_10[1] == 1600.0
        assert final_11[0] == 1 and final_11[1] == 1000.0

        print("\n[Test 3.4] Deliberate Discrepancy Detection in Reconciliation:")
        conn = sqlite3.connect(demo_db)
        conn.execute("UPDATE daily_aggregates SET total_amount = 7777.0 WHERE event_date = '2026-10-10'")
        conn.commit()
        conn.close()

        rec_tampered = reconcile_pipeline(db_path=demo_db)
        print(f"  • Tampered reconciliation result: reconciled={rec_tampered['reconciled']}")
        assert not rec_tampered["reconciled"], "Reconciliation should have detected discrepancy!"

        print("\n[Test 3.5] Recovery from Discrepancy via Re-aggregation:")
        update_daily_aggregates(db_path=demo_db)
        rec_recovered = reconcile_pipeline(db_path=demo_db)
        print(f"  • Re-aggregation recovered state: reconciled={rec_recovered['reconciled']}")
        assert rec_recovered["reconciled"]

        print_header("DEMONSTRATION COMPLETED SUCCESSFULLY")


if __name__ == "__main__":
    run_demonstration()
