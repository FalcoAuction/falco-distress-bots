# src/run_all.py

import os
import traceback
import uuid
from datetime import datetime, timezone

from .bots import foreclosure_tennessee_bot
from .bots import public_notices_bot
from .bots import tax_pages_bot
from .bots import tn_foreclosure_notices_bot
from .bots import propstream_bot
from .bots import api_tax_delinquent_bot
from .bots import lis_pendens_bot
from .bots import substitution_of_trustee_bot
from .bots import official_tax_sales_bot
from .bots import sheriff_sales_bot
from .enrichment import notice_extractor
from .enrichment import notice_pdf_extractor
from .enrichment import bankruptcy_overlay
from .enrichment import probate_overlay
from .core.run_metadata import store_run_metadata
from .telemetry import run_logger


def run_bot(name: str, fn):
    print(f"\n=== RUNNING: {name} ===")
    try:
        if not callable(fn) and hasattr(fn, "__dict__"):
            if callable(getattr(fn, "run", None)):
                result = fn.run()
            elif callable(getattr(fn, "main", None)):
                result = fn.main()
            else:
                raise TypeError(f"Module '{name}' has no callable 'run' or 'main' attribute")
        else:
            result = fn()

        # Always print returned summaries (Stage2/3 return dicts)
        if isinstance(result, dict):
            print(f"[{name}] summary {result}")

        print(f"=== DONE: {name} ===")
    except Exception as e:
        print(f"=== ERROR: {name} === {type(e).__name__}: {e}")


def main():
    run_id = str(uuid.uuid4())
    os.environ["FALCO_RUN_ID"] = run_id
    os.environ.setdefault("FALCO_SQLITE_PATH", "data/falco.db")
    store_run_metadata(run_id)
    utc_start = datetime.now(timezone.utc).replace(microsecond=0).isoformat()

    print("RUN_ALL VERSION CHECK - 2026-02-20 (ATTOM premium operational)")
    print(f"RUN_ALL UTC START: {utc_start}")
    print(f"RUN_ALL RUN_ID: {run_id}")

    run_logger.start_run(run_id)

    try:
        # ---------------- Stage 1: Ingestion ----------------
        run_bot("LisPendensBot", lis_pendens_bot.run)
        run_bot("SubstitutionOfTrusteeBot", substitution_of_trustee_bot.run)
        run_bot("ForeclosureTennesseeBot", foreclosure_tennessee_bot.run)
        run_bot("TNForeclosureNoticesBot", tn_foreclosure_notices_bot.run)
        run_bot("PublicNoticesBot", public_notices_bot.run)
        run_bot("TaxPagesBot", tax_pages_bot.run)
        run_bot("PropStreamBot", propstream_bot.run)
        run_bot("API_TaxDelinquentBot", api_tax_delinquent_bot.run)
        run_bot("OfficialTaxSalesBot", official_tax_sales_bot.run)
        run_bot("SheriffSalesBot", sheriff_sales_bot.run)

        run_bot("Stage1_NoticeExtractor", notice_extractor.run)
        run_bot("Stage1_NoticePDFExtractor", notice_pdf_extractor.run)

        # ---------------- Stage 2: Enrichment + Comps ----------------
        def _run_attom_enrichment():
            from .enrichment.attom_enricher import run as _run
            return _run()

        def _run_comps():
            from .enrichment.comps import run as _run
            return _run()

        run_bot("Stage2_ATTOMEnrichment", _run_attom_enrichment)
        run_bot("Stage2_CompsEngine", _run_comps)
        run_bot("Stage2_BankruptcyOverlay", bankruptcy_overlay.run)
        run_bot("Stage2_ProbateOverlay", probate_overlay.run)

        from .scoring.scorer import score_leads_for_run
        score_leads_for_run(run_id)

        # ---------------- Stage 3: Grading + Packaging ----------------
        def _run_grading():
            from .grading.grade import run as _run
            return _run()

        def _run_packaging():
            from .packaging.packager import run as _run
            result = _run()
            out_dir = os.path.join(os.getcwd(), "out", "packets", run_id)
            print(f"[Stage3_PDFPackaging] packets_created_this_run={result.get('packaged_count', 0)} out_dir={out_dir}")
            return result

        run_bot("Stage3_AuctionFitGrading", _run_grading)
        run_bot("Stage3_PDFPackaging", _run_packaging)

        utc_end = datetime.now(timezone.utc).replace(microsecond=0).isoformat()
        print(f"RUN_ALL UTC END: {utc_end}")

        run_logger.finish_run_success(run_id, {
            "run_id": run_id,
            "utc_start": utc_start,
            "utc_end": utc_end,
        })

    except Exception:
        utc_end = datetime.now(timezone.utc).replace(microsecond=0).isoformat()
        tb = traceback.format_exc()
        print(f"RUN_ALL FATAL ERROR:\n{tb}")
        run_logger.finish_run_failed(run_id, tb, {
            "run_id": run_id,
            "utc_start": utc_start,
            "utc_end": utc_end,
        })
        raise


if __name__ == "__main__":
    main()
