# src/bots/foreclosure_tennessee_bot.py

from bs4 import BeautifulSoup
from ..utils import fetch

BASE_URL = "https://foreclosuretennessee.com/"


def run():

    print(f"[ForeclosureTNBot] DEBUG MODE - seed={BASE_URL}")

    try:
        html = fetch(BASE_URL)
    except Exception as e:
        print("[ForeclosureTNBot] Fetch failed:", e)
        return

    soup = BeautifulSoup(html, "html.parser")

    # Print table headers
    headers = [th.get_text(strip=True) for th in soup.select("table thead th")]
    print(f"[DEBUG HEADERS] -> {headers}")

    rows = soup.select("table tbody tr")
    print(f"[ForeclosureTNBot] rows_found={len(rows)}")

    # Print first 5 rows only
    for i, row in enumerate(rows[:5]):
        cols = [c.get_text(strip=True) for c in row.find_all("td")]
        print(f"[DEBUG ROW {i}] -> {cols}")

    print("[ForeclosureTNBot] DEBUG COMPLETE")
