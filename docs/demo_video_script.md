# Bytespoke DAI-040: Demo Video Script & Presentation Outline (5–8 Minutes)

This script provides the complete walkthrough and speaking notes for recording the assessment submission video.

---

## Video Outline & Timing Breakdown

| Timestamp | Section | Visual Focus | Key Talking Points |
|:---|:---|:---|:---|
| **0:00 – 1:00** | Introduction & Problem Overview | Architecture diagram (`docs/architecture.md`), slide/code overview | Introduce DAI-040 challenge: late-arriving events out-of-order vs ingestion time; need for financial data integrity and deduplication. |
| **1:00 – 2:15** | Architecture & Database Design | `sql/schema.sql`, `docs/design_notes.md` | Show `raw_events` vs `daily_aggregates`. Explain event-time vs ingestion-time semantics and why `raw_events` is the single source of truth. |
| **2:15 – 4:00** | Live Demonstration: Before & After Correction | Terminal running `python3 src/demo.py` | 1. Ingest normal events (`evt_001`, `evt_002`, `evt_003`). Show 2026-10-10 baseline: 2 orders, $1100.00.<br>2. Ingest `evt_004` (occurred Oct 10, ingested Oct 11). Show detection and dynamic aggregate recalculation.<br>3. Highlight quantified delta: +1 order, +$500.00 revenue on historical date 2026-10-10. Oct 11 remains unchanged! |
| **4:00 – 5:30** | Idempotency & Conflict Handling | Terminal running `python3 src/demo.py` (Step 3) | 1. Show repeated ingestion skipping duplicate event IDs.<br>2. Demonstrate conflicting duplicate (`evt_001` with modified amount $9999): engine rejects overwrite and preserves ground truth.<br>3. Demonstrate reconciliation engine detecting deliberate tampering and recovering cleanly. |
| **5:30 – 6:45** | Orchestration & Automated Tests | Terminal: `python3 src/main.py` & `python3 -m unittest` | 1. Run `python3 src/main.py` showing stage gating and operational summary.<br>2. Run test suite: show all 27 automated tests passing in milliseconds (via unittest or pytest). |
| **6:45 – 7:30** | Conclusion & Key Takeaways | `README.md`, summary slide | Summarize reliability guarantees: ACID transactions, zero data loss, strict idempotency, mathematical reconciliation. |

---

## Demonstration Script & Commands
Run these commands in terminal during the screen recording:
```bash
# 1. Run the interactive demonstration of before-and-after correction & idempotency:
python3 src/demo.py

# 2. Run the end-to-end production orchestrator:
python3 src/main.py

# 3. Execute the full automated test suite:
python3 -m unittest discover -s tests -p "test_*.py"
# (Or using pytest):
pytest -v
```
