# src/run_all.py

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
    print("RUN_ALL VERSION CHECK - 2026-02-20 (ATTOM premium operational)")
    print(f"RUN_ALL UTC START: {datetime.utcnow().isoformat()}")

    # ---------------- Stage 1: Ingestion ----------------
    run_bot("ForeclosureTennesseeBot", foreclosure_tennessee_bot.run)
    run_bot("TNForeclosureNoticesBot", tn_foreclosure_notices_bot.run)
    run_bot("PublicNoticesBot", public_notices_bot.run)
    run_bot("TaxPagesBot", tax_pages_bot.run)
    run_bot("PropStreamBot", propstream_bot.run)
    run_bot("API_TaxDelinquentBot", api_tax_delinquent_bot.run)

    # ---------------- Stage 2: Enrichment + Comps ----------------
    def _run_attom_enrichment():
        from .enrichment.attom_enricher import run as _run
        return _run()

    def _run_comps():
        from .enrichment.comps import run as _run
        return _run()

    run_bot("Stage2_ATTOMEnrichment", _run_attom_enrichment)
    run_bot("Stage2_CompsEngine", _run_comps)

    # ---------------- Stage 3: Grading + Packaging ----------------
    def _run_grading():
        from .grading.grade import run as _run
        return _run()

    def _run_packaging():
        from .packaging.packager import run as _run
        return _run()

    run_bot("Stage3_AuctionFitGrading", _run_grading)
    run_bot("Stage3_PDFPackaging", _run_packaging)

    print(f"RUN_ALL UTC END: {datetime.utcnow().isoformat()}")


if __name__ == "__main__":
    main()
