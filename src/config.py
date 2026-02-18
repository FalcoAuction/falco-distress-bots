# === Public Notice Seeds ===
# IMPORTANT: Do NOT scrape the tnpublicnotice homepage.
# Use search endpoints so results are already filtered.

SEED_URLS_PUBLIC_NOTICES = [
    # Trustee sale keyword search
    "https://www.tnpublicnotice.com/search?keyword=trustee%20sale",
    # Foreclosure keyword search (optional)
    "https://www.tnpublicnotice.com/search?keyword=foreclosure",
]

# === County Tax Seeds (pipeline pages for now) ===
SEED_URLS_COUNTY_TAX = [
    # Put your county tax / delinquent tax list pages here
    # Example placeholders:
    # "https://.../davidson/tax-sale",
]

# === Keyword Sets ===
TRUSTEE_KEYWORDS = [
    "substitute trustee", "trustee sale", "notice of sale",
    "foreclosure", "deed of trust", "power of sale"
]

ESTATE_KEYWORDS = [
    "estate of", "probate", "executor", "administrator"
]

TAX_KEYWORDS = [
    "tax sale", "delinquent", "delinquent tax", "back taxes",
    "tax lien", "trustee tax"
]
