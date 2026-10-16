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
from src.ai_summary import generate_ai_run_summary, AISummarizerInterface


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

    # --- PHASE D EXTENDED VERIFICATION TESTS ---

    def test_out_of_order_event_processing(self):
        """Verify that records arriving out of chronological order are assigned correctly."""
        # Record 1 has event_time on 2026-10-11
        # Record 2 has event_time on 2026-10-10 (arrives chronologically after Day 2 record)
        rows = [
            ["evt_901", "2026-10-11T10:00:00", "2026-10-11T10:05:00", "700.0"],
            ["evt_902", "2026-10-10T12:00:00", "2026-10-11T10:10:00", "300.0"],
        ]
        self._write_csv(rows)
        summary = ingest_events(csv_path=self.csv_path, db_path=self.db_path)

        # evt_902 should be flagged late because event_time is prior to max committed event_time
        self.assertEqual(summary["inserted"], 2)
        self.assertEqual(summary["late"], 1)

        # Aggregation should partition records strictly by their event_date
        update_daily_aggregates(db_path=self.db_path)

        conn = sqlite3.connect(self.db_path)
        try:
            oct10 = conn.execute(
                "SELECT total_orders, total_amount FROM daily_aggregates WHERE event_date = '2026-10-10'"
            ).fetchone()
            oct11 = conn.execute(
                "SELECT total_orders, total_amount FROM daily_aggregates WHERE event_date = '2026-10-11'"
            ).fetchone()

            self.assertEqual(oct10[0], 1)
            self.assertEqual(oct10[1], 300.0)
            self.assertEqual(oct11[0], 1)
            self.assertEqual(oct11[1], 700.0)
        finally:
            conn.close()

    def test_late_arriving_event_watermark_threshold(self):
        """Verify watermark threshold correctly identifies late arrival on the same date."""
        # Event occurred at 10:00 but arrived at 12:00 (lag = 7200 seconds)
        rows = [
            ["evt_903", "2026-10-10T10:00:00", "2026-10-10T12:00:00", "400.0"]
        ]
        self._write_csv(rows)
        # Threshold = 3600 seconds (1 hour); 7200 > 3600 -> late
        summary = ingest_events(
            csv_path=self.csv_path,
            db_path=self.db_path,
            late_threshold_seconds=3600,
        )
        self.assertEqual(summary["late"], 1)

    def test_before_after_aggregate_delta_verification(self):
        """Demonstrate and verify initial aggregate, late event arrival, and numerical delta."""
        # Initial baseline batch (Oct 10: 2 orders $1100, Oct 11: 1 order $1000)
        initial_rows = [
            ["evt_001", "2026-10-10T09:00:00", "2026-10-10T09:05:00", "500.0"],
            ["evt_002", "2026-10-10T10:30:00", "2026-10-10T10:35:00", "600.0"],
            ["evt_003", "2026-10-11T08:00:00", "2026-10-11T08:05:00", "1000.0"],
        ]
        self._write_csv(initial_rows)
        ingest_events(csv_path=self.csv_path, db_path=self.db_path)
        update_daily_aggregates(db_path=self.db_path)

        conn = sqlite3.connect(self.db_path)
        try:
            oct10_before = conn.execute(
                "SELECT total_orders, total_amount FROM daily_aggregates WHERE event_date = '2026-10-10'"
            ).fetchone()
            oct11_before = conn.execute(
                "SELECT total_orders, total_amount FROM daily_aggregates WHERE event_date = '2026-10-11'"
            ).fetchone()
        finally:
            conn.close()

        self.assertEqual(oct10_before[0], 2)
        self.assertEqual(oct10_before[1], 1100.0)
        self.assertEqual(oct11_before[0], 1)
        self.assertEqual(oct11_before[1], 1000.0)

        # Now late event arrives on Oct 11 for Oct 10
        late_rows = [
            ["evt_004", "2026-10-10T14:00:00", "2026-10-11T09:00:00", "500.0"]
        ]
        self._write_csv(late_rows)
        summary = ingest_events(csv_path=self.csv_path, db_path=self.db_path)
        self.assertEqual(summary["late"], 1)

        update_daily_aggregates(db_path=self.db_path)

        conn = sqlite3.connect(self.db_path)
        try:
            oct10_after = conn.execute(
                "SELECT total_orders, total_amount FROM daily_aggregates WHERE event_date = '2026-10-10'"
            ).fetchone()
            oct11_after = conn.execute(
                "SELECT total_orders, total_amount FROM daily_aggregates WHERE event_date = '2026-10-11'"
            ).fetchone()
        finally:
            conn.close()

        # Quantified delta verification
        delta_orders = oct10_after[0] - oct10_before[0]
        delta_amount = oct10_after[1] - oct10_before[1]

        self.assertEqual(delta_orders, 1)
        self.assertEqual(delta_amount, 500.0)
        self.assertEqual(oct10_after[0], 3)
        self.assertEqual(oct10_after[1], 1600.0)
        # Verify Oct 11 remained completely unaffected
        self.assertEqual(oct11_after[0], oct11_before[0])
        self.assertEqual(oct11_after[1], oct11_before[1])

    def test_reconciliation_success(self):
        """Verify that accurate data passes reconciliation check."""
        sample_csv = ROOT_DIR / "data" / "sample_events.csv"
        run_pipeline(csv_path=sample_csv, db_path=self.db_path)

        result = reconcile_pipeline(db_path=self.db_path)
        self.assertTrue(result["reconciled"])
        self.assertEqual(len(result["discrepancies"]), 0)

    def test_reconciliation_missing_tables(self):
        """Verify reconciliation handles missing tables safely without crashing."""
        empty_db = Path(self.temp_dir.name) / "uninitialized.db"
        result = reconcile_pipeline(db_path=empty_db)
        self.assertFalse(result["reconciled"])
        self.assertTrue(any("Missing required database tables" in d for d in result["discrepancies"]))

    def test_ai_summary_isolated_interface_default_fallback(self):
        """Verify operational summary works reliably without external AI services or credentials."""
        sample_csv = ROOT_DIR / "data" / "sample_events.csv"
        result = run_pipeline(csv_path=sample_csv, db_path=self.db_path)
        summary = result["summary_text"]

        self.assertIn("=== Operational Pipeline Run Summary ===", summary)
        self.assertIn("• Overall Status:         SUCCESS", summary)
        self.assertNotIn("• AI Operational Commentary:", summary)

    def test_ai_summary_with_mock_llm_provider(self):
        """Verify AI interface enriches output without modifying database records."""
        mock_summarizer = AISummarizerInterface()
        mock_summarizer.is_configured = lambda: True
        mock_summarizer.generate_commentary = lambda ctx: "All batches reconciled successfully with zero errors."

        summary = generate_ai_run_summary(
            ingest_summary={"read": 5, "inserted": 4, "duplicate": 1, "conflicting_duplicates": 0, "invalid": 0, "late": 1, "affected_dates": ["2026-10-10"]},
            agg_results=[["2026-10-10", 3, 1600.0]],
            reconciliation={"reconciled": True},
            ai_interface=mock_summarizer,
        )

        self.assertIn("• AI Operational Commentary:", summary)
        self.assertIn("All batches reconciled successfully with zero errors.", summary)

    def test_ai_summary_graceful_error_handling(self):
        """Verify that when AI service encounters an error, it falls back cleanly to deterministic summary."""
        mock_summarizer = AISummarizerInterface()
        mock_summarizer.is_configured = lambda: True
        # Simulate network error in generate_commentary
        mock_summarizer.generate_commentary = lambda ctx: None

        summary = generate_ai_run_summary(
            ingest_summary={"read": 1, "inserted": 1, "duplicate": 0, "conflicting_duplicates": 0, "invalid": 0, "late": 0, "affected_dates": ["2026-10-10"]},
            agg_results=[["2026-10-10", 1, 500.0]],
            reconciliation={"reconciled": True},
            ai_interface=mock_summarizer,
        )

        self.assertIn("=== Operational Pipeline Run Summary ===", summary)
        self.assertNotIn("• AI Operational Commentary:", summary)

    def test_database_isolation_and_transaction_safety(self):
        """Verify transaction rollback during aggregation prevents partial database state."""
        rows = [["evt_701", "2026-10-10T10:00:00", "2026-10-10T10:05:00", "250.0"]]
        self._write_csv(rows)
        ingest_events(csv_path=self.csv_path, db_path=self.db_path)

        # Create a temporary trigger on daily_aggregates that aborts insert to simulate failure
        conn = sqlite3.connect(self.db_path)
        conn.execute(
            "CREATE TRIGGER fail_agg BEFORE INSERT ON daily_aggregates "
            "BEGIN SELECT RAISE(ABORT, 'Simulated DB Aggregation Failure'); END;"
        )
        conn.close()

        with self.assertRaises(sqlite3.DatabaseError):
            update_daily_aggregates(db_path=self.db_path)

        # Verify raw_events remains intact and daily_aggregates was rolled back
        conn = sqlite3.connect(self.db_path)
        try:
            raw_cnt = conn.execute("SELECT COUNT(*) FROM raw_events").fetchone()[0]
            agg_cnt = conn.execute("SELECT COUNT(*) FROM daily_aggregates").fetchone()[0]
            self.assertEqual(raw_cnt, 1)
            self.assertEqual(agg_cnt, 0)
        finally:
            conn.close()


if __name__ == "__main__":
    unittest.main()
