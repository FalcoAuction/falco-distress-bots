# src/config.py

# Public notice sources we can scrape without logins/CAPTCHA:
# tnlegalpub exposes notice listings + individual notice pages.
SEED_URLS_PUBLIC_NOTICES = [
    "https://tnlegalpub.com/notice_type/foreclosure/",
]

# Hard limit so we don't crawl forever while stabilizing.
# This is "pages of listings", not individual notices.
PUBLIC_NOTICE_MAX_LIST_PAGES = 5

# Keywords used to identify foreclosure/trustee sale content
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

# Optional: estate keyword bucket (keep if you use it elsewhere)
ESTATE_KEYWORDS = [
    "ESTATE OF",
    "PROBATE",
    "ADMINISTRATOR",
    "EXECUTOR",
    "PERSONAL REPRESENTATIVE",
]
