# src/run_all.py

from datetime import datetime

from .bots import foreclosure_tennessee_bot
from .bots import public_notices_bot
from .bots import tax_pages_bot
from .bots import tn_foreclosure_notices_bot


def run_bot(name: str, fn):
    print(f"\n=== RUNNING: {name} ===")
    try:
        fn()
        print(f"=== DONE: {name} ===")
    except Exception as e:
        print(f"=== ERROR: {name} === {type(e).__name__}: {e}")


def main():
    print("RUN_ALL VERSION CHECK - 2026-02-18")
    print(f"RUN_ALL UTC START: {datetime.utcnow().isoformat()}")

    run_bot("ForeclosureTennesseeBot", foreclosure_tennessee_bot.run)
    run_bot("TNForeclosureNoticesBot", tn_foreclosure_notices_bot.run)
    run_bot("PublicNoticesBot", public_notices_bot.run)
    run_bot("TaxPagesBot", tax_pages_bot.run)

    print(f"RUN_ALL UTC END: {datetime.utcnow().isoformat()}")


if __name__ == "__main__":
    main()
