# Late-Arriving Event Pipeline (Bytespoke DAI-040)

A robust Python and SQL data engineering pipeline built to process event streams arriving out of chronological order, maintain accurate daily business metrics, handle duplicate and malformed inputs, and automatically correct historical daily aggregates.

---

## 1. Problem Overview

In event-driven architectures, network latencies, offline mobile devices, and upstream batch delays cause events to arrive out of order. Naive ingestion pipelines using ingestion time rather than event time cause historical reports to be inaccurate.

This pipeline guarantees:
- **Event-Time Processing**: Business dates are strictly derived from `event_time`.
- **Deduplication**: Primary keys on `event_id` prevent duplicate ingestion and inflation.
- **Conflicting Duplicate Safety**: Modified payloads for existing IDs are flagged and rejected without overwriting truth.
- **Historical Recalculation**: Late events trigger recalculation of their historical `event_date`.
- **Atomic Transactions**: Aggregations use atomic SQL transactions (`BEGIN ... COMMIT / ROLLBACK`) with `ON CONFLICT(event_date) DO UPDATE`.
- **Stage-Gated Orchestration**: Fail-fast execution prevents corrupt dependent stages.
- **Independent Reconciliation**: Automated mathematical verification comparing `daily_aggregates` against `raw_events`.

---

## 2. Directory Structure

```text
late-arriving-event-pipeline/
├── README.md                  # Project documentation & operational run guide
├── pipeline.db                # SQLite database (auto-generated)
├── data/
│   └── sample_events.csv      # Sample event stream dataset
├── docs/
│   └── architecture.md        # Architecture design & data flow diagram
├── sql/
│   └── schema.sql             # DDL table schemas & indexes
├── src/
│   ├── __init__.py
│   ├── init_db.py             # Database schema initialization
│   ├── ingest.py              # Validation, deduplication & ingestion
│   ├── aggregate.py           # Daily metric calculation & upserts
│   ├── reconcile.py           # Verification & reconciliation check
│   ├── ai_summary.py          # Operational run metrics summary generator
│   └── main.py                # End-to-end pipeline orchestrator
└── tests/
    └── test_pipeline.py       # Isolated unit & integration test suite (18 tests)
```

---

## 3. Database Schema

### `raw_events`
Stores raw immutable validated event records.
- `event_id` (TEXT PRIMARY KEY)
- `event_time` (TEXT NOT NULL)
- `ingestion_time` (TEXT NOT NULL)
- `amount` (REAL NOT NULL CHECK(amount >= 0))
- `event_date` (TEXT NOT NULL)

### `daily_aggregates`
Stores daily business totals.
- `event_date` (TEXT PRIMARY KEY)
- `total_orders` (INTEGER NOT NULL DEFAULT 0)
- `total_amount` (REAL NOT NULL DEFAULT 0)
- `last_updated` (TEXT NOT NULL)

---

## 4. Setup and Execution

### Prerequisites
- Python 3.10+ (tested with Python 3.13)
- No external third-party dependencies required (uses Python standard library: `sqlite3`, `csv`, `unittest`).

### Execution Commands

#### 1. Run Complete Pipeline End-to-End
```bash
python3 src/main.py
```

#### 2. Explicit Database Initialization
```bash
python3 src/init_db.py
# Or with the orchestrator:
python3 src/main.py --init
```
*Note: Normal pipeline execution automatically creates tables if missing, but preserves all existing database records.*

#### 3. Custom Input File or Target Database
```bash
python3 src/main.py --csv data/sample_events.csv --db pipeline.db
```

#### 4. Run Automated Test Suite
```bash
python3 -m unittest discover -s tests -p "test_*.py"
```

---

## 5. Operational Run Summary Fields

Each pipeline execution produces a structured operational summary:
- **Overall Status**: `SUCCESS` or `FAILED`.
- **Input Records Read**: Total rows parsed from CSV.
- **New Events Inserted**: New valid records written to `raw_events`.
- **Duplicate Events**: Exact duplicates detected and skipped.
- **Conflicting Duplicates**: Existing IDs with mismatched amounts or timestamps.
- **Invalid / Rejected**: Malformed timestamps, missing IDs, or negative amounts.
- **Late-Arriving Events**: Events where `event_date < ingestion_date` or arrived out of chronological sequence.
- **Affected Event Dates**: List of dates modified by newly inserted records.
- **Aggregation Status**: `SUCCESS`, `FAILED`, or `SKIPPED`.
- **Reconciliation Status**: `SUCCESS`, `FAILED`, or `SKIPPED`.
- **Failure Stage & Reason**: Detailed diagnostics if any stage halts.
- **Daily Business Totals**: Breakdown of orders and total revenue per event date.

---

## 6. Failure Handling and Recovery Behavior

### Stage Gating (Fail-Fast)
The pipeline executes in strict chronological stages:
1. **Database Check / Initialization**
2. **CSV Ingestion**
3. **Daily Aggregates Recalculation**
4. **Data Reconciliation**

If any stage fails (e.g., corrupted CSV, missing file, database failure):
- Dependent downstream stages are **skipped** (`SKIPPED`).
- Overall status is marked **FAILED**.
- The CLI process exits with non-zero exit code `1`.

### Safe Retries & Partial Failure Recovery
- **No Double Counting**: Because `raw_events` enforces `PRIMARY KEY (event_id)`, retrying a failed pipeline run will never insert duplicate records or inflate totals.
- **Idempotent Aggregation**: If aggregation fails or aborts mid-transaction, SQLite rolls back changes. Rerunning `src/main.py` recalculates aggregates fresh from `raw_events`.
- **State Preservation**: The database is never dropped or cleared during standard pipeline execution.

---

## 7. Example Output (Actual Execution)

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