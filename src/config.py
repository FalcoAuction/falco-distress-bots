# src/config.py

# ============================================================
# FALCO DISTRESS ENGINE CONFIG
# ============================================================

# ============================================================
# GEOGRAPHY CONTROL
# ============================================================

# Leave empty list [] to allow STATEWIDE
# Add counties like ["Davidson County", "Williamson County"] to restrict
TARGET_COUNTIES = []


# ============================================================
# PUBLIC NOTICE SOURCES (LISTING / SEARCH PAGES)
# ============================================================

SEED_URLS_PUBLIC_NOTICES = [
    "https://tnlegalpub.com/notice_type/foreclosure/",
    "https://www.foreclosurestn.com/",
    "https://www.tnpublicnotice.com/Search.aspx",
]

# Max pages to attempt per listing source
PUBLIC_NOTICES_MAX_LIST_PAGES = 10

# Minimum days until sale to write to Notion (0 = any future)
PUBLIC_NOTICES_MIN_DAYS_OUT = 0

# Debug mode (prints verbose logs)
PUBLIC_NOTICES_DEBUG = False


# ============================================================
# FORECLOSURE TENNESSEE (PRIMARY HIGH-SIGNAL SOURCE)
# ============================================================

SEED_URLS_FORECLOSURE_TN = [
    "https://foreclosuretennessee.com/"
]


# ============================================================
# COUNTY TAX / CLERK & MASTER SEEDS (TaxPagesBot expects this)
# ============================================================

# Leave empty if you’re not using tax pages yet.
# If empty, TaxPagesBot should ideally just print "no seeds" and exit.
SEED_URLS_COUNTY_TAX = []

# Example seeds you can add later:
# SEED_URLS_COUNTY_TAX = [
#     "https://www.examplecounty.gov/tax-sale",
# ]


# ============================================================
# KEYWORD SIGNALS
# ============================================================

TRUSTEE_KEYWORDS = [
    "trustee sale",
    "substitute trustee",
    "substitute trustee sale",
    "trustee's sale",
    "foreclosure",
    "notice of foreclosure",
    "notice of sale",
]

ESTATE_KEYWORDS = [
    "estate of",
    "executor",
    "administrator",
    "probate",
]

TAX_KEYWORDS = [
    "tax sale",
    "delinquent tax",
    "clerk and master",
    "trustee tax sale",
    "sheriff sale",
]


# ============================================================
# SCORING CONFIG (optional knobs)
# ============================================================

URGENT_DAYS_THRESHOLD = 7
HOT_DAYS_THRESHOLD = 14


# ============================================================
# SAFETY LIMITS
# ============================================================

MAX_NOTICE_LINKS_PER_SOURCE = 200
MAX_NOTICE_TEXT_CHARS = 8000
