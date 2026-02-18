# src/config.py

# ============================================================
# FALCO DISTRESS ENGINE CONFIG
# ============================================================

# ============================================================
# GEOGRAPHY CONTROL
# ============================================================

# Leave empty list [] to allow STATEWIDE
# Add counties like ["Davidson", "Williamson"] to restrict
TARGET_COUNTIES = []


# ============================================================
# PUBLIC NOTICE SOURCES (LISTING / SEARCH PAGES)
# ============================================================

SEED_URLS_PUBLIC_NOTICES = [
    # WordPress foreclosure listing
    "https://tnlegalpub.com/notice_type/foreclosure/",

    # TN Press Association foreclosure repository
    "https://www.foreclosurestn.com/",

    # Statewide legal notices search hub
    "https://www.tnpublicnotice.com/Search.aspx",
]

# Max pages to attempt per listing source
PUBLIC_NOTICES_MAX_LIST_PAGES = 10

# Minimum days until sale to write to Notion
# Set to 0 to capture everything future-dated
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
# KEYWORD SIGNALS
# ============================================================

TRUSTEE_KEYWORDS = [
    "trustee sale",
    "substitute trustee",
    "substitute trustee sale",
    "trustee's sale",
    "foreclosure",
    "notice of foreclosure",
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
]


# ============================================================
# SCORING CONFIG
# ============================================================

# Days-to-sale thresholds used by scoring.py
URGENT_DAYS_THRESHOLD = 7
HOT_DAYS_THRESHOLD = 14


# ============================================================
# SAFETY LIMITS
# ============================================================

# Hard caps so we never crawl uncontrolled volume
MAX_NOTICE_LINKS_PER_SOURCE = 200
MAX_NOTICE_TEXT_CHARS = 8000
