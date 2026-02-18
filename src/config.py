# src/config.py

# -----------------------------
# tnlegalpub (Foreclosure notice type)
# -----------------------------
SEED_URLS_PUBLIC_NOTICES = [
    "https://tnlegalpub.com/notice_type/foreclosure/",
]
PUBLIC_NOTICE_MAX_LIST_PAGES = 25  # deeper crawl

TRUSTEE_KEYWORDS = [
    "TRUSTEE'S SALE",
    "TRUSTEE’S SALE",
    "SUBSTITUTE TRUSTEE",
    "NOTICE OF FORECLOSURE",
    "FORECLOSURE SALE",
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
# ForeclosureTennessee.com (active upstream feed)
# -----------------------------
FORECLOSURE_TN_SEED_URL = "https://foreclosuretennessee.com/"
FORECLOSURE_TN_MAX_PAGES = 4

# -----------------------------
# County Tax Pages (TaxPagesBot) - keep stable, empty for now
# -----------------------------
SEED_URLS_COUNTY_TAX = []
TAX_KEYWORDS = [
    "tax sale",
    "delinquent",
    "delinquent taxes",
    "tax delinquent",
    "back taxes",
    "foreclosure",
]

# -----------------------------
# Geo targeting (Nashville radius)
# IMPORTANT: store UPPERCASE so comparisons are simple
# -----------------------------
TARGET_COUNTIES = [
    "DAVIDSON",
    "WILLIAMSON",
    "RUTHERFORD",
    "SUMNER",
    "WILSON",
    "DICKSON",
    "MAURY",
    "ROBERTSON",
    "CHEATHAM",
    "MONTGOMERY",
]
