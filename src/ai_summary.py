import json
import os
import urllib.error
import urllib.request
from typing import Any, Dict, List, Optional


def build_deterministic_summary(
    read_count: int,
    inserted_count: int,
    duplicate_count: int,
    conflicting_count: int,
    invalid_count: int,
    late_count: int,
    affected_dates_str: str,
    aggregation_status: str,
    reconciliation_status: str,
    overall_status: str,
    failure_stage: Optional[str] = None,
    failure_reason: Optional[str] = None,
    agg_results: Optional[List[Any]] = None,
) -> str:
    """Builds the standardized, authoritative deterministic operational summary text."""
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
            summary_lines.append(
                f"   - {event_date}: {orders} orders, ${amount:.2f} total revenue"
            )

    return "\n".join(summary_lines)


class AISummarizerInterface:
    """
    Isolated interface for optional AI operational commentary.
    Supports local free models (e.g., Ollama, vLLM, llama.cpp) or
    OpenAI-compatible cloud endpoints via standard library HTTP requests.
    Zero external third-party SDK dependencies required.
    """

    def __init__(
        self,
        endpoint_url: Optional[str] = None,
        api_key: Optional[str] = None,
        model_name: Optional[str] = None,
        timeout: int = 5,
    ):
        self.endpoint_url = (
            endpoint_url
            or os.getenv("LOCAL_LLM_URL")
            or os.getenv("LLM_ENDPOINT_URL")
        )
        self.api_key = api_key or os.getenv("LLM_API_KEY") or os.getenv("OPENAI_API_KEY")
        self.model_name = model_name or os.getenv("LLM_MODEL_NAME", "llama3")
        self.timeout = timeout

    def is_configured(self) -> bool:
        """Returns True if an AI endpoint or key is configured and AI is enabled."""
        enabled = os.getenv("AI_SUMMARY_ENABLED", "0").lower() in ("1", "true", "yes")
        return enabled and bool(self.endpoint_url)

    def generate_commentary(self, prompt_context: str) -> Optional[str]:
        """
        Sends structured metrics to the AI model and returns a narrative summary.
        Catches any connection or parsing errors to ensure pipeline reliability.
        Crucially: output is purely narrative and NEVER modifies raw events or aggregates.
        """
        if not self.endpoint_url:
            return None

        payload = {
            "model": self.model_name,
            "prompt": (
                "Provide a concise 2-sentence executive summary of this data pipeline run:\n"
                f"{prompt_context}"
            ),
            "stream": False,
        }
        data = json.dumps(payload).encode("utf-8")
        headers = {"Content-Type": "application/json"}
        if self.api_key:
            headers["Authorization"] = f"Bearer {self.api_key}"

        req = urllib.request.Request(self.endpoint_url, data=data, headers=headers, method="POST")
        try:
            with urllib.request.urlopen(req, timeout=self.timeout) as resp:
                if resp.status == 200:
                    resp_body = resp.read().decode("utf-8")
                    parsed = json.loads(resp_body)
                    # Support Ollama ('response') or OpenAI ('choices[0].message.content')
                    if "response" in parsed:
                        return parsed["response"].strip()
                    elif "choices" in parsed and len(parsed["choices"]) > 0:
                        choice = parsed["choices"][0]
                        if "message" in choice and "content" in choice["message"]:
                            return choice["message"]["content"].strip()
                        elif "text" in choice:
                            return choice["text"].strip()
        except Exception as exc:
            # Non-blocking graceful error handling
            print(f"[AI Summary Notice] AI service unavailable ({exc}); using deterministic summary.")
            return None
        return None


def generate_ai_run_summary(
    ingest_summary=None,
    agg_results=None,
    reconciliation=None,
    overall_status=None,
    aggregation_status=None,
    reconciliation_status=None,
    failure_stage=None,
    failure_reason=None,
    ai_interface: Optional[AISummarizerInterface] = None,
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
        reconciliation_status = (
            "SUCCESS"
            if reconciled
            else ("SKIPPED" if not agg_results and not read_count else "FAILED")
        )

    if aggregation_status is None:
        aggregation_status = (
            "SUCCESS" if agg_results or inserted_count == 0 else "SKIPPED"
        )

    if overall_status is None:
        overall_status = (
            "SUCCESS"
            if reconciliation_status == "SUCCESS" and aggregation_status == "SUCCESS"
            else "FAILED"
        )

    affected_dates_str = ", ".join(affected_dates) if affected_dates else "None"

    deterministic_summary = build_deterministic_summary(
        read_count=read_count,
        inserted_count=inserted_count,
        duplicate_count=duplicate_count,
        conflicting_count=conflicting_count,
        invalid_count=invalid_count,
        late_count=late_count,
        affected_dates_str=affected_dates_str,
        aggregation_status=aggregation_status,
        reconciliation_status=reconciliation_status,
        overall_status=overall_status,
        failure_stage=failure_stage,
        failure_reason=failure_reason,
        agg_results=agg_results,
    )

    # Optional AI enrichment
    summarizer = ai_interface or AISummarizerInterface()
    if summarizer.is_configured():
        ai_commentary = summarizer.generate_commentary(deterministic_summary)
        if ai_commentary:
            return (
                f"{deterministic_summary}\n"
                "• AI Operational Commentary:\n"
                f"   {ai_commentary}"
            )

    return deterministic_summary
