# src/config.py

"""
FALCO DISTRESS ENGINE CONFIG

Future-proof goals:
- Each bot can have its OWN days-to-sale (DTS) window.
- Bots do NOT accidentally inherit global env vars unless you want them to.
- Global defaults exist, but per-bot config overrides are supported.
- Env vars can override anything when you’re running actions / prod.

Precedence (highest -> lowest):
1) Env per-bot:     FALCO_<BOTKEY>_DTS_MIN / FALCO_<BOTKEY>_DTS_MAX
2) Config per-bot:  <BOTKEY>_DTS_WINDOW
3) Env global:      FALCO_DTS_MIN / FALCO_DTS_MAX
4) Config global:   GLOBAL_DTS_WINDOW
"""

# ============================================================
# GEOGRAPHY CONTROL
# ============================================================
# Leave [] to allow statewide (unless env restricts).
# If set, MUST match "X County" format (e.g., "Davidson County").
TARGET_COUNTIES = []


# ============================================================
# GLOBAL DEFAULTS
# ============================================================
# Default window used if bot-specific window is not set.
GLOBAL_DTS_WINDOW = (21, 90)

# Default allowed counties if env not set and TARGET_COUNTIES empty.
# (Bots treat this as "base county names" without "County")
DEFAULT_ALLOWED_COUNTIES_BASE = [
    "Davidson",
    "Rutherford",
    "Sumner",
    "Williamson",
    "Wilson",
    "Maury",
    "Montgomery",
    "Cheatham",
    "Robertson",
    "Dickson",
    "Knox",
    "Hamilton",
]


# ============================================================
# PUBLIC NOTICE SOURCES (LISTING / SEARCH PAGES)
# ============================================================
SEED_URLS_PUBLIC_NOTICES = [
    "https://tnlegalpub.com/notice_type/foreclosure/",
    "https://www.foreclosurestn.com/",
    "https://www.tnpublicnotice.com/Search.aspx",
]

PUBLIC_NOTICES_MAX_LIST_PAGES = 10

# Per-bot window (THIS is the important fix)
# TNLegalPub etc can have sale dates that are much farther out than courthouse lists.
PUBLIC_NOTICES_DTS_WINDOW = (0, 180)

# Truncate raw snippets before sending to Notion (prevents massive blobs)
PUBLIC_NOTICES_RAW_SNIPPET_MAX_CHARS = 1200

# Debug mode
PUBLIC_NOTICES_DEBUG = False


# ============================================================
# FORECLOSURE TENNESSEE (PRIMARY HIGH-SIGNAL SOURCE)
# ============================================================
SEED_URLS_FORECLOSURE_TN = ["https://foreclosuretennessee.com/"]

FORECLOSURE_TN_DTS_WINDOW = (21, 90)


# ============================================================
# TN FORECLOSURE NOTICES (TNForeclosureNoticesBot)
# ============================================================
TNFN_DTS_WINDOW = (21, 90)


# ============================================================
# COUNTY TAX / CLERK & MASTER SEEDS (TaxPagesBot expects this)
# ============================================================
SEED_URLS_COUNTY_TAX = []
TAXPAGES_DTS_WINDOW = (0, 365)


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
