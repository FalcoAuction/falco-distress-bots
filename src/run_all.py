print("RUN_ALL VERSION CHECK - 2026-02-18")

from .bots.public_notices_bot import run as run_public_notices
from .bots.tax_pages_bot import run as run_tax_pages

def main():
    run_public_notices()
    run_tax_pages()

if __name__ == "__main__":
    main()
