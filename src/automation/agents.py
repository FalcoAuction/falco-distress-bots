from __future__ import annotations

from typing import Any, Dict, Iterable

from .enrichment_trigger_agent import write_enrichment_trigger_report
from .falco_analyst_agent import write_falco_analyst_report
from .lead_triage_agent import write_lead_triage_report
from .source_watch_agent import write_source_watch_report


def write_agent_reports(
    run_id: str,
    stage_results: Iterable[Dict[str, Any]],
    run_summary: Dict[str, Any],
) -> Dict[str, Any]:
    source_watch = write_source_watch_report(run_id, stage_results)
    lead_triage = write_lead_triage_report(run_id, run_summary)
    enrichment_trigger = write_enrichment_trigger_report(run_id, run_summary)
    falco_analyst = write_falco_analyst_report(run_id, run_summary)
    return {
        "ok": True,
        "source_watch": source_watch,
        "lead_triage": lead_triage,
        "enrichment_trigger": enrichment_trigger,
        "falco_analyst": falco_analyst,
    }
