# src/config.py

# -----------------------------
# Public Notices (tnlegalpub)
# -----------------------------
SEED_URLS_PUBLIC_NOTICES = [
    "https://tnlegalpub.com/notice_type/foreclosure/",
]

PUBLIC_NOTICE_MAX_LIST_PAGES = 5

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

# -----------------------------
# County Tax Pages (TaxPagesBot)
# -----------------------------
# Put real county tax seed pages here later.
# For now, keep it empty so TaxPagesBot does nothing but also doesn't crash.
SEED_URLS_COUNTY_TAX = []

TAX_KEYWORDS = [
    "tax sale",
    "delinquent",
    "delinquent taxes",
    "tax delinquent",
    "back taxes",
    "trustee",
    "foreclosure",
]
