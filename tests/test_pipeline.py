import csv
import os
import sqlite3
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

# Add src to path
ROOT_DIR = Path(__file__).resolve().parent.parent
import sys
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from src.init_db import initialize_database
from src.ingest import ingest_events
from src.aggregate import update_daily_aggregates
from src.reconcile import reconcile_pipeline
from src.main import run_pipeline


class TestLateArrivingEventPipeline(unittest.TestCase):

    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.db_path = Path(self.temp_dir.name) / "test_pipeline.db"
        self.csv_path = Path(self.temp_dir.name) / "test_events.csv"
        initialize_database(db_path=self.db_path)

    def tearDown(self):
        self.temp_dir.cleanup()

    def _write_csv(self, rows):
        with open(self.csv_path, "w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow(["event_id", "event_time", "ingestion_time", "amount"])
            for row in rows:
                writer.writerow(row)

    def test_database_initialization(self):
        conn = sqlite3.connect(self.db_path)
        try:
            tables = [r[0] for r in conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()]
            self.assertIn("raw_events", tables)
            self.assertIn("daily_aggregates", tables)
        finally:
            conn.close()

    def test_valid_event_ingestion(self):
        rows = [
            ["evt_101", "2026-10-10T09:00:00", "2026-10-10T09:05:00", "500.0"],
            ["evt_102", "2026-10-10T10:00:00", "2026-10-10T10:05:00", "300.0"],
        ]
        self._write_csv(rows)
        summary = ingest_events(csv_path=self.csv_path, db_path=self.db_path)

        self.assertEqual(summary["read"], 2)
        self.assertEqual(summary["inserted"], 2)
        self.assertEqual(summary["duplicate"], 0)
        self.assertEqual(summary["invalid"], 0)
        self.assertEqual(summary["affected_dates"], ["2026-10-10"])

        conn = sqlite3.connect(self.db_path)
        try:
            count = conn.execute("SELECT COUNT(*) FROM raw_events").fetchone()[0]
            self.assertEqual(count, 2)
        finally:
            conn.close()

    def test_duplicate_event_rejection(self):
        rows = [
            ["evt_101", "2026-10-10T09:00:00", "2026-10-10T09:05:00", "500.0"],
            ["evt_101", "2026-10-10T09:00:00", "2026-10-10T09:05:00", "500.0"],
        ]
        self._write_csv(rows)
        summary = ingest_events(csv_path=self.csv_path, db_path=self.db_path)

        self.assertEqual(summary["read"], 2)
        self.assertEqual(summary["inserted"], 1)
        self.assertEqual(summary["duplicate"], 1)

    def test_duplicate_handling_across_repeated_executions(self):
        rows = [["evt_101", "2026-10-10T09:00:00", "2026-10-10T09:05:00", "500.0"]]
        self._write_csv(rows)

        summary1 = ingest_events(csv_path=self.csv_path, db_path=self.db_path)
        self.assertEqual(summary1["inserted"], 1)

        summary2 = ingest_events(csv_path=self.csv_path, db_path=self.db_path)
        self.assertEqual(summary2["inserted"], 0)
        self.assertEqual(summary2["duplicate"], 1)

    def test_conflicting_duplicate_event_ids(self):
        rows1 = [["evt_101", "2026-10-10T09:00:00", "2026-10-10T09:05:00", "500.0"]]
        self._write_csv(rows1)
        ingest_events(csv_path=self.csv_path, db_path=self.db_path)

        rows2 = [["evt_101", "2026-10-10T09:00:00", "2026-10-10T09:05:00", "999.0"]]
        self._write_csv(rows2)
        summary2 = ingest_events(csv_path=self.csv_path, db_path=self.db_path)

        self.assertEqual(summary2["conflicting_duplicates"], 1)
        conn = sqlite3.connect(self.db_path)
        try:
            amt = conn.execute("SELECT amount FROM raw_events WHERE event_id = 'evt_101'").fetchone()[0]
            self.assertEqual(amt, 500.0)
        finally:
            conn.close()

    def test_invalid_or_missing_event_fields(self):
        rows = [
            ["", "2026-10-10T09:00:00", "2026-10-10T09:05:00", "500.0"],
            ["evt_201", "2026-10-10T09:00:00", "2026-10-10T09:05:00", "-50.0"],
            ["evt_202", "2026-10-10T09:00:00", "2026-10-10T09:05:00", "not_a_number"],
        ]
        self._write_csv(rows)
        summary = ingest_events(csv_path=self.csv_path, db_path=self.db_path)

        self.assertEqual(summary["read"], 3)
        self.assertEqual(summary["inserted"], 0)
        self.assertEqual(summary["invalid"], 3)

    def test_malformed_timestamps(self):
        rows = [
            ["evt_301", "invalid-date-string", "2026-10-10T09:05:00", "500.0"],
        ]
        self._write_csv(rows)
        summary = ingest_events(csv_path=self.csv_path, db_path=self.db_path)

        self.assertEqual(summary["inserted"], 0)
        self.assertEqual(summary["invalid"], 1)

    def test_late_arriving_events_and_historical_correction(self):
        rows_day1 = [
            ["evt_003", "2026-10-11T08:00:00", "2026-10-11T08:05:00", "1000.0"]
        ]
        self._write_csv(rows_day1)
        ingest_events(csv_path=self.csv_path, db_path=self.db_path)
        update_daily_aggregates(db_path=self.db_path)

        conn = sqlite3.connect(self.db_path)
        try:
            oct10 = conn.execute("SELECT total_orders, total_amount FROM daily_aggregates WHERE event_date = '2026-10-10'").fetchone()
            self.assertIsNone(oct10)
        finally:
            conn.close()

        rows_day2 = [
            ["evt_004", "2026-10-10T14:00:00", "2026-10-11T09:00:00", "500.0"]
        ]
        self._write_csv(rows_day2)
        summary = ingest_events(csv_path=self.csv_path, db_path=self.db_path)
        self.assertEqual(summary["late"], 1)

        update_daily_aggregates(db_path=self.db_path)

        conn = sqlite3.connect(self.db_path)
        try:
            oct10 = conn.execute("SELECT total_orders, total_amount FROM daily_aggregates WHERE event_date = '2026-10-10'").fetchone()
            self.assertIsNotNone(oct10)
            self.assertEqual(oct10[0], 1)
            self.assertEqual(oct10[1], 500.0)
        finally:
            conn.close()

    def test_sample_csv_expected_baseline_and_revenue(self):
        sample_csv = ROOT_DIR / "data" / "sample_events.csv"
        results = run_pipeline(csv_path=sample_csv, db_path=self.db_path)

        ingest = results["ingest"]
        self.assertEqual(ingest["read"], 5)
        self.assertEqual(ingest["inserted"], 4)
        self.assertEqual(ingest["duplicate"], 1)
        self.assertEqual(ingest["invalid"], 0)
        self.assertEqual(ingest["late"], 1)

        reconcile = results["reconciliation"]
        self.assertTrue(reconcile["reconciled"])

        conn = sqlite3.connect(self.db_path)
        try:
            oct10 = conn.execute("SELECT total_orders, total_amount FROM daily_aggregates WHERE event_date = '2026-10-10'").fetchone()
            oct11 = conn.execute("SELECT total_orders, total_amount FROM daily_aggregates WHERE event_date = '2026-10-11'").fetchone()

            self.assertEqual(oct10[0], 3)
            self.assertEqual(oct10[1], 1600.0)

            self.assertEqual(oct11[0], 1)
            self.assertEqual(oct11[1], 1000.0)
        finally:
            conn.close()

    def test_idempotence(self):
        sample_csv = ROOT_DIR / "data" / "sample_events.csv"
        run_pipeline(csv_path=sample_csv, db_path=self.db_path)
        run_pipeline(csv_path=sample_csv, db_path=self.db_path)

        conn = sqlite3.connect(self.db_path)
        try:
            oct10 = conn.execute("SELECT total_orders, total_amount FROM daily_aggregates WHERE event_date = '2026-10-10'").fetchone()
            self.assertEqual(oct10[0], 3)
            self.assertEqual(oct10[1], 1600.0)
        finally:
            conn.close()

    def test_reconciliation_detects_mismatch(self):
        rows = [["evt_101", "2026-10-10T09:00:00", "2026-10-10T09:05:00", "500.0"]]
        self._write_csv(rows)
        run_pipeline(csv_path=self.csv_path, db_path=self.db_path)

        conn = sqlite3.connect(self.db_path)
        try:
            conn.execute("UPDATE daily_aggregates SET total_amount = 99999.0 WHERE event_date = '2026-10-10'")
            conn.commit()
        finally:
            conn.close()

        rec = reconcile_pipeline(db_path=self.db_path)
        self.assertFalse(rec["reconciled"])
        self.assertEqual(len(rec["discrepancies"]), 1)

    # --- DAY 4 ORCHESTRATION & RELIABILITY TESTS ---

    def test_successful_end_to_end_execution(self):
        sample_csv = ROOT_DIR / "data" / "sample_events.csv"
        result = run_pipeline(csv_path=sample_csv, db_path=self.db_path)

        self.assertTrue(result["success"])
        self.assertEqual(result["status"], "SUCCESS")
        self.assertIsNone(result["failure_stage"])
        self.assertEqual(result["aggregation_status"], "SUCCESS")
        self.assertEqual(result["reconciliation_status"], "SUCCESS")

    def test_ingestion_failure_stage_gating(self):
        missing_csv = Path(self.temp_dir.name) / "does_not_exist.csv"
        result = run_pipeline(csv_path=missing_csv, db_path=self.db_path)

        self.assertFalse(result["success"])
        self.assertEqual(result["status"], "FAILED")
        self.assertEqual(result["failure_stage"], "Ingestion")
        self.assertEqual(result["aggregation_status"], "SKIPPED")
        self.assertEqual(result["reconciliation_status"], "SKIPPED")

    def test_aggregation_failure_stage_gating(self):
        sample_csv = ROOT_DIR / "data" / "sample_events.csv"
        with patch("src.main.update_daily_aggregates", side_effect=RuntimeError("Simulated Database Aggregation Error")):
            result = run_pipeline(csv_path=sample_csv, db_path=self.db_path)

        self.assertFalse(result["success"])
        self.assertEqual(result["status"], "FAILED")
        self.assertEqual(result["failure_stage"], "Aggregation")
        self.assertEqual(result["aggregation_status"], "FAILED")
        self.assertEqual(result["reconciliation_status"], "SKIPPED")

    def test_reconciliation_failure_reporting(self):
        sample_csv = ROOT_DIR / "data" / "sample_events.csv"
        fake_rec = {"reconciled": False, "discrepancies": ["Simulated total mismatch"]}
        with patch("src.main.reconcile_pipeline", return_value=fake_rec):
            result = run_pipeline(csv_path=sample_csv, db_path=self.db_path)

        self.assertFalse(result["success"])
        self.assertEqual(result["status"], "FAILED")
        self.assertEqual(result["failure_stage"], "Reconciliation")
        self.assertEqual(result["reconciliation_status"], "FAILED")

    def test_operational_run_summary_metrics(self):
        sample_csv = ROOT_DIR / "data" / "sample_events.csv"
        result = run_pipeline(csv_path=sample_csv, db_path=self.db_path)
        summary_text = result["summary_text"]

        self.assertIn("=== Operational Pipeline Run Summary ===", summary_text)
        self.assertIn("• Overall Status:         SUCCESS", summary_text)
        self.assertIn("• Input Records Read:     5", summary_text)
        self.assertIn("• New Events Inserted:    4", summary_text)
        self.assertIn("• Duplicate Events:       1", summary_text)
        self.assertIn("• Conflicting Duplicates: 0", summary_text)
        self.assertIn("• Invalid / Rejected:     0", summary_text)
        self.assertIn("• Late-Arriving Events:   1", summary_text)
        self.assertIn("• Affected Event Dates:   2026-10-10, 2026-10-11", summary_text)
        self.assertIn("• Aggregation Status:     SUCCESS", summary_text)
        self.assertIn("• Reconciliation Status:  SUCCESS", summary_text)

    def test_safe_retry_after_partial_failure(self):
        rows = [
            ["evt_501", "2026-10-10T12:00:00", "2026-10-10T12:05:00", "400.0"],
            ["evt_502", "2026-10-10T13:00:00", "2026-10-10T13:05:00", "600.0"],
        ]
        self._write_csv(rows)

        # Ingestion succeeds, but aggregation simulated to fail on attempt 1
        with patch("src.main.update_daily_aggregates", side_effect=RuntimeError("Simulated Failure")):
            result1 = run_pipeline(csv_path=self.csv_path, db_path=self.db_path)
        self.assertFalse(result1["success"])

        # Raw events are safely preserved in raw_events
        conn = sqlite3.connect(self.db_path)
        try:
            cnt = conn.execute("SELECT COUNT(*) FROM raw_events").fetchone()[0]
            self.assertEqual(cnt, 2)
        finally:
            conn.close()

        # Attempt 2: Retry pipeline without mocking error
        result2 = run_pipeline(csv_path=self.csv_path, db_path=self.db_path)
        self.assertTrue(result2["success"])
        self.assertEqual(result2["status"], "SUCCESS")

        # Verify no double counting occurred
        conn = sqlite3.connect(self.db_path)
        try:
            oct10 = conn.execute("SELECT total_orders, total_amount FROM daily_aggregates WHERE event_date = '2026-10-10'").fetchone()
            self.assertEqual(oct10[0], 2)
            self.assertEqual(oct10[1], 1000.0)
        finally:
            conn.close()

    def test_preservation_of_existing_data_during_normal_execution(self):
        sample_csv = ROOT_DIR / "data" / "sample_events.csv"
        # Initial run
        run_pipeline(csv_path=sample_csv, db_path=self.db_path)

        # Record state
        conn = sqlite3.connect(self.db_path)
        try:
            raw_cnt = conn.execute("SELECT COUNT(*) FROM raw_events").fetchone()[0]
            agg_rows = conn.execute("SELECT event_date, total_orders, total_amount FROM daily_aggregates ORDER BY event_date").fetchall()
        finally:
            conn.close()

        self.assertEqual(raw_cnt, 4)

        # Normal re-run without --init flag
        run_pipeline(csv_path=sample_csv, db_path=self.db_path, init_db=False)

        # Verify state was preserved and not wiped
        conn = sqlite3.connect(self.db_path)
        try:
            raw_cnt_after = conn.execute("SELECT COUNT(*) FROM raw_events").fetchone()[0]
            agg_rows_after = conn.execute("SELECT event_date, total_orders, total_amount FROM daily_aggregates ORDER BY event_date").fetchall()
        finally:
            conn.close()

        self.assertEqual(raw_cnt, raw_cnt_after)
        self.assertEqual(agg_rows, agg_rows_after)


if __name__ == "__main__":
    unittest.main()
