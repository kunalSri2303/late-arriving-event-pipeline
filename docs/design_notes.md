# Architecture Design & Decision Note: DAI-040 Late-Arriving Event Pipeline

## 1. Problem & Design Objectives
Event-driven pipelines inevitably receive out-of-order records due to network latency, client disconnects, and upstream retries. Aggregating records by ingestion timestamp distorts historical business metrics and generates misleading financial reports.
**Core Objectives:**
1. Maintain accurate daily financial metrics (order counts and total revenue) assigned strictly by business date.
2. Ingest stream records idempotently, preventing double counting and payload corruption.
3. Dynamically correct historical daily aggregates upon late arrival without corrupting concurrent or subsequent dates.
4. Provide independent reconciliation and fail-fast stage gating for production reliability.

---

## 2. Event-Time vs. Ingestion-Time Semantics
- **`event_time`**: The authoritative business timestamp indicating when the order actually took place. The business date (`event_date`) is derived directly from this value (`event_time.date()`).
- **`ingestion_time`**: Metadata recording when the pipeline gateway processed the payload. It is retained solely for auditability, SLA tracking, and lag calculation, but is **never** used to group business metrics.

---

## 3. Late-Arrival Detection Policy
An incoming record is classified as late-arriving if:
1. Its `event_date` is earlier than its `ingestion_date` (`event_date < ingestion_date`), OR
2. Its `event_time` is earlier than the maximum `event_time` already committed to `raw_events` (`event_time < max(event_time)`), OR
3. Its arrival lag exceeds a configurable watermark threshold (`time_diff > late_threshold_seconds`).

Late events are inserted into `raw_events` under their original historical `event_date`, triggering an atomic recalculation of that historical date.

---

## 4. Deduplication & Idempotency Strategy
- **Primary Key Constraint**: `raw_events.event_id` enforces uniqueness at the database engine level.
- **Identical Duplicates**: Exact duplicate payloads are safely skipped (`duplicate_count += 1`) without database write errors.
- **Conflicting Payload Safety**: If an incoming record reuses an existing `event_id` with an altered amount or timestamp, it is flagged as a conflicting duplicate (`conflicting_duplicates += 1`), logged with diagnostics, and rejected. The existing ground truth in `raw_events` is preserved.
- **Repeated Pipeline Runs**: Subsequent runs on identical files insert 0 rows and leave daily aggregates untouched.

---

## 5. Daily Aggregation & Historical Correction
- **Source of Truth**: `raw_events` is the immutable source of truth.
- **Recalculation Over Blind Increments**: Rather than applying error-prone additive deltas, the pipeline recalculates the affected date's totals directly:
  ```sql
  SELECT event_date, COUNT(*) AS total_orders, ROUND(SUM(amount), 2) AS total_amount
  FROM raw_events WHERE event_date IN (?) GROUP BY event_date;
  ```
- **Upsert Mechanics**: Aggregates are updated in `daily_aggregates` using SQLite `INSERT ... ON CONFLICT(event_date) DO UPDATE SET total_orders = excluded.total_orders, total_amount = excluded.total_amount, last_updated = ?`.

---

## 6. Transaction Safety & Failure Recovery
- **ACID Transactions**: Aggregation updates execute within an atomic transaction managed by Python's `with conn:` context manager (auto-commit on success, automatic rollback on exception).
- **Fail-Fast Stage Gating**: The orchestrator (`src/main.py`) validates prerequisites at each stage (Init → Ingest → Aggregate → Reconcile). A failure in an upstream stage immediately aborts downstream stages (`SKIPPED`) and exits with status `1`.
- **Safe Recovery**: If aggregation fails mid-batch, the transaction rolls back cleanly. Rerunning `src/main.py` recalculates aggregates fresh from `raw_events` without data loss or duplicate records.
- **Isolated AI Layer**: Operational summary generation is isolated behind a clear interface in `src/ai_summary.py`. It falls back gracefully to deterministic metrics when AI credentials or endpoints are unavailable, ensuring zero network failures can halt pipeline execution.

---

## 7. Trade-offs, Limitations & Assumptions
- **SQLite Single-Writer Lock**: SQLite provides lightweight, zero-dependency persistence ideal for embedded and assessment pipelines. For high-throughput concurrent writers, PostgreSQL or distributed engines would replace SQLite.
- **Full Date Re-aggregation**: Recalculating affected dates guarantees 100% mathematical consistency at the cost of O(N) reads per affected date. For high-volume partitions, incremental delta trees or micro-batch watermarks could be introduced.
- **In-Memory Assumptions**: Timestamps assume standard ISO-8601 formatting, converted to naive UTC/local timestamps.
- **Deferred Video**: The 5–8 minute demonstration video recording is intentionally deferred; all demonstration steps and talking points are fully documented in `docs/demo_video_script.md`.
