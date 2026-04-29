# src/run_all.py

import os
from datetime import datetime

from .bots import foreclosure_tennessee_bot
from .bots import public_notices_bot
from .bots import tax_pages_bot
from .bots import tn_foreclosure_notices_bot
from .bots import propstream_bot
from .bots import api_tax_delinquent_bot


def run_bot(name: str, fn):
    print(f"\n=== RUNNING: {name} ===")
    try:
        result = fn()

        # Always print returned summaries (Stage2/3 return dicts)
        if isinstance(result, dict):
            print(f"[{name}] summary {result}")

        print(f"=== DONE: {name} ===")
    except Exception as e:
        print(f"=== ERROR: {name} === {type(e).__name__}: {e}")


def main():
    print("RUN_ALL VERSION CHECK - 2026-04-29 (Supabase direct, Notion removed)")
    print(f"RUN_ALL UTC START: {datetime.utcnow().isoformat()}")

    # ---------------- Stage 1: Ingestion ----------------
    # Bots write directly to Supabase homeowner_requests via supabase_store.
    # Idempotent upsert on pipeline_lead_key — safe to re-run.
    run_bot("ForeclosureTennesseeBot", foreclosure_tennessee_bot.run)
    run_bot("TNForeclosureNoticesBot", tn_foreclosure_notices_bot.run)
    run_bot("PublicNoticesBot", public_notices_bot.run)
    run_bot("TaxPagesBot", tax_pages_bot.run)
    run_bot("PropStreamBot", propstream_bot.run)
    run_bot("API_TaxDelinquentBot", api_tax_delinquent_bot.run)

    # ---------------- Stage 2 + 3: Enrichment / Grading / Packaging --------
    # These stages were Notion-coupled (queried Notion DB to find unenriched
    # leads, wrote enrichment back to Notion). Notion is no longer used.
    #
    # Enrichment now happens via the Vercel cron at
    # https://falco.llc/api/cron/refresh-dialer (BatchData skip-trace +
    # AVM backfill, runs daily at 6 AM UTC). Grading + packaging are
    # vault-era artifacts and aren't needed for the dialer flow.
    #
    # Set FALCO_ENABLE_LEGACY_STAGES=1 to re-enable for local dev if you
    # want to test the old Notion-coupled enrichment paths.
    if os.environ.get("FALCO_ENABLE_LEGACY_STAGES", "").strip() == "1":
        def _run_attom_enrichment():
            from .enrichment.attom_enricher import run as _run
            return _run()

        def _run_comps():
            from .enrichment.comps import run as _run
            return _run()

        def _run_grading():
            from .grading.grade import run as _run
            return _run()

        def _run_packaging():
            from .packaging.packager import run as _run
            return _run()

        run_bot("Stage2_ATTOMEnrichment", _run_attom_enrichment)
        run_bot("Stage2_CompsEngine", _run_comps)
        run_bot("Stage3_AuctionFitGrading", _run_grading)
        run_bot("Stage3_PDFPackaging", _run_packaging)
    else:
        print(
            "\n[run_all] Stage 2/3 skipped (Notion-coupled). "
            "Enrichment now runs via Vercel cron /api/cron/refresh-dialer. "
            "Set FALCO_ENABLE_LEGACY_STAGES=1 to opt back in."
        )

    print(f"RUN_ALL UTC END: {datetime.utcnow().isoformat()}")


if __name__ == "__main__":
    main()
