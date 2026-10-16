# Late-Arriving Event Pipeline (Bytespoke DAI-040)

A robust, production-grade Python and SQL data engineering pipeline designed to handle event streams arriving out of chronological order, enforce strict deduplication and idempotency, dynamically correct historical daily aggregates, and verify data integrity through automated mathematical reconciliation.

---

## 1. Problem Statement & Architecture

### The Problem
In distributed, event-driven architectures, events frequently arrive out of chronological order due to mobile device offline buffering, network latency, upstream batch processing, or system retries. A naive ingestion pipeline that groups records by arrival time (`ingestion_time`) distorts historical reporting, corrupts daily financial metrics, and creates audit inconsistencies.

### The Solution
This pipeline separates **event time** (when the business transaction occurred) from **ingestion time** (when the system observed the record). Business metrics are strictly calculated using `event_date` derived from `event_time`. When late records arrive for earlier dates, the pipeline routes them to the appropriate historical date, triggers an atomic recalculation of the affected date's daily aggregates, and verifies total integrity via independent mathematical reconciliation.

```text
[ Incoming CSV Event Stream ]
             │
             ▼
   [ 1. Ingestion & Validation ]
       ├── Validate timestamps (ISO-8601), IDs, and amounts (>= 0)
       ├── Rejection log for malformed records
       ├── Deduplication check on event_id:
       │    ├── Identical payload  ──> Duplicate counter (safely skipped)
       │    └── Conflicting payload ──> Conflicting duplicate flagged & rejected (truth preserved)
       └── Late-event detection:
            ├── event_date < ingestion_date, OR
            ├── event_time < max(committed event_time), OR
            └── time_diff > late_threshold_seconds watermark
             │
             ▼
   [ 2. SQLite: raw_events ]  <── Immutable Append-Only Source of Truth
             │
             ▼
   [ 3. Daily Aggregations ]
       ├── Group by event_date (orders count, total revenue sum)
       └── Atomic transaction (with conn:) upsert into daily_aggregates
             │
             ▼
   [ 4. Data Reconciliation ]
       ├── Independent SUM(amount) and COUNT(*) audit from raw_events
       ├── Compares against daily_aggregates table
       └── Validation gate: exits with code 1 if any discrepancy is detected
             │
             ▼
   [ 5. Operational & AI Summary ]
       ├── Authoritative deterministic operational metrics
       └── Optional AI narrative commentary (local Ollama / cloud LLM with graceful fallback)
```

---

## 2. Directory Structure

```text
late-arriving-event-pipeline/
├── README.md                  # Comprehensive documentation, run guide & audit evidence
├── requirements.txt           # Minimal dependency specification (standard library runtime + dev pytest)
├── pyproject.toml             # Project build metadata and pytest configuration
├── pipeline.db                # SQLite database (auto-generated, git-ignored)
├── data/
│   └── sample_events.csv      # Sample event stream dataset with duplicates and late events
├── docs/
│   ├── architecture.md        # Detailed system architecture, data flow diagram & semantics
│   ├── design_notes.md        # One-page architecture design & decision rationale note
│   └── demo_video_script.md   # Complete 5–8 minute demonstration script & speaking notes
├── sql/
│   └── schema.sql             # DDL table schemas, primary keys, checks & indexes
├── src/
│   ├── __init__.py
│   ├── init_db.py             # Database schema initialization
│   ├── ingest.py              # Validation, deduplication, conflict safety & late-event ingestion
│   ├── aggregate.py           # Daily metric calculation & atomic upserts
│   ├── reconcile.py           # Independent mathematical verification & validation gate
│   ├── ai_summary.py          # Operational run metrics summary & isolated AI interface
│   ├── demo.py                # Standalone demonstration of historical correction & edge cases
│   └── main.py                # End-to-end pipeline orchestrator & CLI entry point
└── tests/
    └── test_pipeline.py       # Comprehensive automated test suite (27 passing tests)
```

---

## 3. Technology Stack & Prerequisites

- **Language**: Python 3.10+ (tested on Python 3.13.1)
- **Database Engine**: SQLite 3 (ACID-compliant, embedded, zero-dependency)
- **Core Dependencies**: **Zero third-party packages required** for the core pipeline. Relies exclusively on Python Standard Library modules:
  - `sqlite3` — Database connection, indexing, and transactional execution
  - `csv` — Streaming CSV reading and parsing
  - `datetime` — ISO-8601 parsing and event-time date derivation
  - `pathlib` — Cross-platform filesystem path resolution
  - `argparse` — Command-line interface orchestration
  - `urllib.request` / `json` — Isolated HTTP client for optional AI summaries
  - `unittest` — Built-in testing and test fixtures
- **Development / Optional Dependencies**:
  - `pytest>=8.0.0` — Supported alternative test runner

---

## 4. Setup Instructions

### Option A: Standard Setup (No Virtual Environment Needed)
Because the core pipeline and test suite use only Python standard library modules, you can run all commands directly:
```bash
python3 src/main.py
```

### Option B: Clean Virtual Environment Setup (Recommended)
```bash
# 1. Create a virtual environment
python3 -m venv .venv

# 2. Activate virtual environment
# On macOS / Linux:
source .venv/bin/activate

# 3. Install optional developer dependencies (pytest)
pip install -r requirements.txt
```

---

## 5. Execution Commands

### 1. Run Complete Pipeline End-to-End
Runs database verification, CSV ingestion, aggregate recalculation, reconciliation, and operational summary:
```bash
python3 src/main.py
```

### 2. Run with Custom Input Data or Target Database
```bash
python3 src/main.py --csv data/sample_events.csv --db custom_pipeline.db
```

### 3. Explicit Database Initialization
Initializes database tables and indexes from `sql/schema.sql`:
```bash
python3 src/init_db.py
# Or via the orchestrator:
python3 src/main.py --init
```
*(Note: Normal pipeline execution automatically creates missing tables and indexes while preserving all existing data).*

### 4. Modular Stage Execution
Each pipeline component can be executed independently:

```bash
# Ingestion only:
python3 src/ingest.py data/sample_events.csv pipeline.db

# Daily Aggregations only:
python3 src/aggregate.py pipeline.db

# Reconciliation check only:
python3 src/reconcile.py pipeline.db
# (Or using main.py flag):
python3 src/main.py --reconcile
```

### 5. Run Historical Correction Demonstration
Executes the interactive demonstration showing baseline aggregation, late-event arrival, quantified before/after deltas, duplicate rejection, conflicting-payload protection, and reconciliation recovery in an isolated temporary database:
```bash
python3 src/demo.py
# Or using main.py flag:
python3 src/main.py --demo
```

### 6. Run Automated Test Suite
Run the 27 comprehensive automated tests using Python's built-in `unittest` runner or `pytest`:
```bash
# Using Python Standard Library unittest:
python3 -m unittest discover -s tests -p "test_*.py" -v

# Using pytest:
pytest -v
```

---

## 6. Key Design Policies

### Late-Event Handling Policy
An incoming record is identified as **late-arriving** if any of the following conditions are met:
1. **Calendar Lag**: `event_date < ingestion_date` (the event took place on an earlier calendar day).
2. **Out-of-Order Sequence**: `event_time < max(event_time)` committed to `raw_events` (an event with an earlier timestamp arrived after a later event).
3. **Watermark Exceeded**: `(ingestion_time - event_time) > late_threshold_seconds` (arrival latency exceeded a configured threshold, e.g. 1 hour).

**Handling Mechanics**:
- Late events are **never discarded** and **never added to the current day's revenue**.
- The record is persisted in `raw_events` with its true `event_date`.
- The pipeline recalculates the daily totals for the affected historical `event_date` directly from `raw_events` and executes an atomic `UPSERT` on `daily_aggregates`.
- Unaffected historical dates remain unchanged.

### Idempotency & Conflicting Duplicate Policy
- **Primary Key Constraint**: `raw_events.event_id` enforces uniqueness at the database level.
- **Identical Duplicates**: If a record with an existing `event_id` arrives with the exact same payload (same timestamp and amount), it is counted as a duplicate and safely skipped.
- **Conflicting Payload Protection**: If a record reuses an existing `event_id` but provides a conflicting amount or timestamp, it is flagged as a `conflicting_duplicate`, logged with row details, and rejected. The existing ground-truth record in `raw_events` is **never overwritten**.
- **Repeated Ingestion**: Reprocessing the exact same file 10 times results in 0 new rows inserted and leaves `daily_aggregates` completely stable.

### Failure Handling & Stage Gating
The pipeline implements strict fail-fast stage gating:
1. **Initialization** ➔ 2. **Ingestion** ➔ 3. **Aggregation** ➔ 4. **Reconciliation**
- If an upstream stage encounters an error (e.g. missing CSV or disk failure), all subsequent dependent stages are set to `SKIPPED`.
- The CLI process exits with non-zero exit code `1`.
- Aggregations are wrapped in atomic transactions (`with conn:`). In the event of an error, SQLite rolls back changes completely.
- Because `raw_events` is append-only with primary key constraints, safe retries never result in duplicate writes or double-counted revenue.

---

## 7. Isolated AI Interface Configuration

The pipeline isolates AI functionality behind a clean interface in `src/ai_summary.py`:
- **Default Behavior (No Setup Required)**: The pipeline produces an authoritative, structured deterministic operational summary. No API keys, no internet connection, and no third-party packages are needed.
- **Optional AI Commentary**: You can enable natural language operational commentary by configuring environment variables:
  ```bash
  # Enable the AI enrichment hook:
  export AI_SUMMARY_ENABLED=1

  # Option 1: Free / Local Model (e.g., Ollama or vLLM)
  export LOCAL_LLM_URL="http://localhost:11434/api/generate"
  export LLM_MODEL_NAME="llama3"

  # Option 2: OpenAI or Compatible Cloud Endpoint
  export LLM_ENDPOINT_URL="https://api.openai.com/v1/chat/completions"
  export LLM_API_KEY="your-api-key"
  ```
- **Safety Guarantees**:
  - AI calls use standard library `urllib.request` with a 5-second timeout.
  - If the model is offline, unreachable, or returns an error, the pipeline prints a non-blocking diagnostic and falls back seamlessly to the deterministic summary.
  - **AI output is purely narrative and is NEVER permitted to alter raw database records or financial calculations.**

---

## 8. Verified Execution Outputs

### Actual Output from `python3 src/main.py`:
```text
==================================================
      LATE-ARRIVING EVENT PIPELINE RUNNER        
==================================================

[Step 1/4] Database exists (pipeline.db). Preserving existing data.

[Step 2/4] Ingesting CSV Data: sample_events.csv
--- Ingestion Summary ---
Read:                  5
Inserted:              4
Duplicates:            1
Conflicting Duplicates:0
Rejected / Invalid:    0
Late-Arriving Events:  1

Late Events Details:
  Event evt_004 (event_time: 2026-10-10T14:00:00, ingested: 2026-10-11T09:00:00)

[Step 3/4] Updating Daily Aggregates...
--- Aggregation Summary ---
2026-10-10: orders=3, amount=1600.00
2026-10-11: orders=1, amount=1000.00

[Step 4/4] Running Data Reconciliation Check...
--- Reconciliation Report ---
SUCCESS: daily_aggregates perfectly match raw_events calculations!

==================================================
=== Operational Pipeline Run Summary ===
• Overall Status:         SUCCESS
• Input Records Read:     5
• New Events Inserted:    4
• Duplicate Events:       1
• Conflicting Duplicates: 0
• Invalid / Rejected:     0
• Late-Arriving Events:   1
• Affected Event Dates:   2026-10-10, 2026-10-11
• Aggregation Status:     SUCCESS
• Reconciliation Status:  SUCCESS
• Daily Business Totals:
   - 2026-10-10: 3 orders, $1600.00 total revenue
   - 2026-10-11: 1 orders, $1000.00 total revenue
==================================================
```

### Actual Output from `python3 src/demo.py`:
```text
======================================================================
 BYTESPOKE DAI-040: LATE-ARRIVING EVENT PIPELINE DEMONSTRATION
======================================================================
Working in isolated temporary database: demo_pipeline.db
Database initialized successfully: /tmp/.../demo_pipeline.db

======================================================================
 STEP 1: NORMAL EVENT PROCESSING (BEFORE LATE EVENT ARRIVES)
======================================================================
Baseline for 2026-10-10 recorded: Orders=2, Total Revenue=$1100.00

======================================================================
 STEP 2: INGEST LATE-ARRIVING EVENT (EVT_004)
======================================================================
Event Details:
  • ID:             evt_004
  • event_time:     2026-10-10T14:00:00  (Occurred Oct 10)
  • ingestion_time: 2026-10-11T09:00:00  (Arrived Oct 11 - NEXT DAY)
  • amount:         $500.00

Late event detected: 1 event(s)
Stored in raw_events: id=evt_004, event_date=2026-10-10 (strictly from event_time, NOT ingestion_time)

Recalculating affected historical dates...

--- Correction Quantified for 2026-10-10 ---
• Pre-Correction:   2 orders, $1100.00
• Post-Correction:  3 orders, $1600.00
• Quantified Delta: +1 order, +$500.00 revenue
• 2026-10-11 Date:  1 orders, $1000.00 (Remained unchanged!)

Verifying data integrity with reconciliation...
--- Reconciliation Report ---
SUCCESS: daily_aggregates perfectly match raw_events calculations!

======================================================================
 STEP 3: IDEMPOTENCY & DUPLICATE HANDLING VERIFICATION
======================================================================
[Test 3.1] Repeated Ingestion: Inserted 0, Duplicates 1 (Passed)
[Test 3.2] Conflicting Payload: evt_001 with amount $9999 rejected; ground truth preserved at $500.00 (Passed)
[Test 3.3] Repeated Aggregation: Oct 10 ($1600.00) and Oct 11 ($1000.00) unchanged (Passed)
[Test 3.4] Discrepancy Detection: Deliberate aggregate tampering detected by reconciliation (Passed)
[Test 3.5] Recovery from Discrepancy: Re-aggregation restores 100% mathematical integrity (Passed)

======================================================================
 DEMONSTRATION COMPLETED SUCCESSFULLY
======================================================================
```

---

## 9. Automated Testing Summary

The test suite in `tests/test_pipeline.py` contains **27 automated tests** covering all operational scenarios:
1. `test_database_initialization` — Verifies DDL creation of tables and indexes.
2. `test_valid_event_ingestion` — Verifies ingestion of compliant events into `raw_events`.
3. `test_duplicate_event_rejection` — Verifies duplicate event IDs are skipped.
4. `test_duplicate_handling_across_repeated_executions` — Verifies idempotency across multiple runs.
5. `test_conflicting_duplicate_event_ids` — Verifies conflicting payload rejection and data preservation.
6. `test_invalid_or_missing_event_fields` — Verifies rejection of missing IDs, negative amounts, and non-numeric values.
7. `test_malformed_timestamps` — Verifies rejection of non-ISO-8601 timestamps.
8. `test_late_arriving_events_and_historical_correction` — Verifies late event identification and historical date recalculation.
9. `test_out_of_order_event_processing` — Verifies correct date partitioning when Day 2 arrives before Day 1.
10. `test_late_arriving_event_watermark_threshold` — Verifies watermark threshold detection on same-day events.
11. `test_before_after_aggregate_delta_verification` — Verifies pre-correction, post-correction, and numerical deltas (+1 order, +$500.00).
12. `test_sample_csv_expected_baseline_and_revenue` — Verifies exact metrics from `data/sample_events.csv`.
13. `test_idempotence` — Verifies running the pipeline multiple times does not inflate metrics.
14. `test_reconciliation_success` — Verifies clean data passes reconciliation with 0 discrepancies.
15. `test_reconciliation_detects_mismatch` — Verifies tampered aggregate triggers reconciliation failure.
16. `test_reconciliation_failure_reporting` — Verifies pipeline halts with stage failure on reconciliation mismatch.
17. `test_reconciliation_missing_tables` — Verifies safe handling of uninitialized databases without crashes.
18. `test_successful_end_to_end_execution` — Verifies end-to-end pipeline run from CSV to reconciliation.
19. `test_ingestion_failure_stage_gating` — Verifies missing CSV safely halts downstream stages.
20. `test_aggregation_failure_stage_gating` — Verifies aggregation error skips reconciliation.
21. `test_safe_retry_after_partial_failure` — Verifies retry after simulated failure without double counting.
22. `test_preservation_of_existing_data_during_normal_execution` — Verifies pipeline preserves existing DB records.
23. `test_operational_run_summary_metrics` — Verifies structured formatting of operational metrics.
24. `test_ai_summary_isolated_interface_default_fallback` — Verifies 100% offline fallback when AI is unconfigured.
25. `test_ai_summary_with_mock_llm_provider` — Verifies narrative enrichment without database mutation.
26. `test_ai_summary_graceful_error_handling` — Verifies pipeline reliability when AI endpoint is unreachable.
27. `test_database_isolation_and_transaction_safety` — Verifies real SQLite rollback when aggregation fails.

```bash
# Execution verification:
$ python3 -m unittest discover -s tests -p "test_*.py"
Ran 27 tests in 0.083s
OK
```

---

## 10. Known Limitations, Trade-offs & Assumptions

1. **Embedded Database Concurrency**: SQLite operates with file-level / table-level locking, which is ideal for single-process embedded pipelines, assessment testing, and reproducible local evaluation. For high-concurrency multi-worker environments, an enterprise RDBMS (e.g. PostgreSQL) or distributed store (e.g. Snowflake/BigQuery) would be substituted.
2. **Date-Level Re-aggregation Trade-off**: Re-aggregating all records for affected dates guarantees 100% mathematical precision and eliminates floating-point drift from additive deltas. For terabyte-scale partitions, a hybrid delta accumulator or streaming watermark window could be used.
3. **Timestamp Assumptions**: Event timestamps are expected in standard ISO-8601 formatting (`YYYY-MM-DDTHH:MM:SS`), and timezones (if present) are converted to naive UTC/local timestamps.
4. **Demonstration Video**: As instructed by the assignment scope, the 5–8 minute video recording is intentionally deferred. The complete speaking script and timing outline are provided in `docs/demo_video_script.md`.