import os
import urllib.request
import json


def generate_ai_run_summary(
    ingest_summary=None,
    agg_results=None,
    reconciliation=None,
    overall_status=None,
    aggregation_status=None,
    reconciliation_status=None,
    failure_stage=None,
    failure_reason=None,
) -> str:
    """
    Generates a structured operational summary of the pipeline execution run.
    Contains: input records read, new inserted, duplicates, conflicting duplicates,
    invalid/rejected, late-arriving events, affected event dates, aggregation status,
    reconciliation status, and overall success or failure.
    Provides graceful fallback when an external AI service is not configured.
    """
    # If a single dictionary of pipeline results was passed as the first argument
    if isinstance(ingest_summary, dict) and "ingest" in ingest_summary and agg_results is None:
        run_data = ingest_summary
        ingest_summary = run_data.get("ingest", {})
        agg_results = run_data.get("aggregate", [])
        reconciliation = run_data.get("reconciliation", {})
        overall_status = run_data.get("status", overall_status)
        aggregation_status = run_data.get("aggregation_status", aggregation_status)
        reconciliation_status = run_data.get("reconciliation_status", reconciliation_status)
        failure_stage = run_data.get("failure_stage", failure_stage)
        failure_reason = run_data.get("failure_reason", failure_reason)

    ingest_summary = ingest_summary or {}
    agg_results = agg_results or []
    reconciliation = reconciliation or {}

    read_count = ingest_summary.get("read", 0)
    inserted_count = ingest_summary.get("inserted", 0)
    duplicate_count = ingest_summary.get("duplicate", 0)
    conflicting_count = ingest_summary.get("conflicting_duplicates", 0)
    invalid_count = ingest_summary.get("invalid", 0)
    late_count = ingest_summary.get("late", 0)
    affected_dates = ingest_summary.get("affected_dates", [])

    if reconciliation_status is None:
        reconciled = reconciliation.get("reconciled", False)
        reconciliation_status = "SUCCESS" if reconciled else ("SKIPPED" if not agg_results and not read_count else "FAILED")

    if aggregation_status is None:
        aggregation_status = "SUCCESS" if agg_results or inserted_count == 0 else "SKIPPED"

    if overall_status is None:
        overall_status = "SUCCESS" if reconciliation_status == "SUCCESS" and aggregation_status == "SUCCESS" else "FAILED"

    affected_dates_str = ", ".join(affected_dates) if affected_dates else "None"

    summary_lines = [
        "=== Operational Pipeline Run Summary ===",
        f"• Overall Status:         {overall_status}",
        f"• Input Records Read:     {read_count}",
        f"• New Events Inserted:    {inserted_count}",
        f"• Duplicate Events:       {duplicate_count}",
        f"• Conflicting Duplicates: {conflicting_count}",
        f"• Invalid / Rejected:     {invalid_count}",
        f"• Late-Arriving Events:   {late_count}",
        f"• Affected Event Dates:   {affected_dates_str}",
        f"• Aggregation Status:     {aggregation_status}",
        f"• Reconciliation Status:  {reconciliation_status}",
    ]

    if failure_stage:
        summary_lines.append(f"• Failure Stage:          {failure_stage}")
    if failure_reason:
        summary_lines.append(f"• Failure Reason:         {failure_reason}")

    if agg_results:
        summary_lines.append("• Daily Business Totals:")
        for row in agg_results:
            event_date, orders, amount = row[0], row[1], row[2]
            summary_lines.append(f"   - {event_date}: {orders} orders, ${amount:.2f} total revenue")

    return "\n".join(summary_lines)
