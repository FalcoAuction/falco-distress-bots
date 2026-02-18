import os

NOTION_TOKEN = os.environ.get("NOTION_TOKEN", "").strip()
NOTION_DATABASE_ID = os.environ.get("NOTION_DATABASE_ID", "").strip()

# We will add real URLs later
SEED_URLS_PUBLIC_NOTICES = [
    "https://www.tnpublicnotice.com/",
    "https://tnlegalpub.com/notice_type/foreclosure/",
]

SEED_URLS_COUNTY_TAX = [
    "https://chanceryclerkandmaster.nashville.gov/fees/property-tax-schedule/",
    "https://chanceryclerkandmaster.nashville.gov/fees/delinquent-tax-sales/",
]


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
