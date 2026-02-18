# src/config.py

# Public notice sources we can scrape without logins/CAPTCHA:
# tnlegalpub exposes listing pages + individual notice pages.
SEED_URLS_PUBLIC_NOTICES = [
    "https://tnlegalpub.com/notice_type/foreclosure/",
]

# Hard limit while stabilizing: how many listing pages to paginate through per seed.
PUBLIC_NOTICE_MAX_LIST_PAGES = 5

# Keywords (kept for compatibility with other code, even if not used in the new notice-level crawler)
TRUSTEE_KEYWORDS = [
    "TRUSTEE'S SALE",
    "TRUSTEE’S SALE",
    "SUBSTITUTE TRUSTEE",
    "SUBSTITUTE TRUSTEE’S",
    "SUBSTITUTE TRUSTEE'S",
    "NOTICE OF FORECLOSURE",
    "FORECLOSURE SALE",
    "NOTICE OF SUBSTITUTE TRUSTEE",
    "NOTICE OF TRUSTEE",
    "NOTICE OF SALE",
]

ESTATE_KEYWORDS = [
    "ESTATE OF",
    "PROBATE",
    "ADMINISTRATOR",
    "EXECUTOR",
    "PERSONAL REPRESENTATIVE",
]
