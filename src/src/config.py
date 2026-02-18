import os

NOTION_TOKEN = os.environ.get("NOTION_TOKEN", "").strip()
NOTION_DATABASE_ID = os.environ.get("NOTION_DATABASE_ID", "").strip()

# We will add real URLs here next step
SEED_URLS_PUBLIC_NOTICES = []
SEED_URLS_COUNTY_TAX = []

TRUSTEE_KEYWORDS = [
    "substitute trustee",
    "trustee’s sale",
    "trustees sale",
    "foreclosure",
    "notice of sale",
    "deed of trust",
    "default"
]

ESTATE_KEYWORDS = [
    "notice to creditors",
    "estate of",
    "letters testamentary",
    "letters of administration",
    "administrator",
    "executor",
    "probate"
]

TAX_KEYWORDS = [
    "delinquent tax",
    "tax sale",
    "court sale",
    "clerk and master",
    "chancery",
    "auction"
]
