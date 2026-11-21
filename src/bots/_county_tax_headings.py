"""County-specific newspaper headings observed in the delinquent-tax category."""

import re


# Match the published proceeding, including the jurisdiction where available.
_CIVIC_OR_PRIVATE = {
    "davidson": (
        r"NOTICE OF HOMEOWNERS ASSOCIATION LIEN SALE\b",
        r"LEGAL NOTICE PROPOSED BUDGET FOR FISCAL YEAR \d{4} Metropolitan Government of Nashville",
        r"(?:PUBLIC NOTICE FOR PUBLICATION ONE TIME ONLY ON MAIN STREET NASHVILLE WEBSITE .*? )?"
        r"NOTICE OF ZONING AMENDMENT PROPOSALS\b",
        r"Notice is hereby given of public hearings, .*?nashville\.legistar\.com.*?BILL BL2026-1395",
        r"NOTICE OF PUBLIC HEARING ON THE ARTS CENTER REDEVELOPMENT PLAN\b",
        r"NOTICE OF SHERIFF'S SALE .*?(?:Green Hills Center for Rehabilitation LLC|Jo Anne Simpson vs\.)",
    ),
    "montgomery": (
        r"(?:\d+ )?BY ORDER OF THE CITY OF CLARKSVILLE BOARD OF ZONING APPEALS A PUBLIC HEARING",
        r"LEGAL NOTICE - CITY ZONING .*?Zoning Ordinance of the City of Clarksville",
    ),
    "dickson": (
        r"OFFICIAL BALLOT - STATE OF TENNESSEE, DICKSON COUNTY\b",
        r"CITY OF DICKSON PUBLIC NOTICE SPECIAL SESSION OF DICKSON CITY COUNCIL\b",
        r"PUBLIC NOTICE PASSAGE OF ORDINANCE #1583 .*?annual budget",
    ),
    "williamson": (
        r".{0,30}PUBLIC HEARING FOR ORDINANCE No\. 26-11 .*?NOLENSVILLE .*?ANNUAL BUDGET",
        r"Page 1 ORDINANCE 2026- 014 .*?THOMPSON'S STATION.*?ANNUAL BUDGET",
        r"City of Spring Hill .*?NOTICE OF INTENT TO EXCEED CERTIFIED TAX RATE",
        r"(?:Law approved template Dec\. 2023 )?RESOLUTION 2026-47 A RESOLUTION TO ANNEX FIVE PROPERTIES",
        r"N O T I C E WHEREAS, petition to amend Ordinance No\. 6958, known as the Zoning Ordinance",
        r"Notice is hereby given .*?City of Franklin will hold a Public Hearing concerning Ordinance 2026-10",
        r"PUBLIC NOTICE Take notice, .*?City of Chattanooga's Operations and Capital budgets",
        r"(?:TN-\d+ )?PUBLIC NOTICE THE (?:PLANNING COMMISSION|BOARD OF ZONING APPEALS) OF THE CITY OF FAIRVIEW",
        r"TN-\d+ CITY OF FAIRVIEW, TENNESSEE FY \d{4} BUDGET",
        r"SENATE JOINT RESOLUTION NO\. 25 .*?Constitution of Tennessee, relative to bail",
    ),
}

_FORECLOSURE = {
    "davidson": r"FURTHER POSTPONEMENT ANNOUNCEMENT OF SUBSTITUTE TRUSTEE'S NOTICE OF SALE",
    "montgomery": r"Notice of Sale by Foreclosure .*?Substitute Trustee",
    "williamson": r"(?:SALE HAS BEEN ADJOURNED .*?WILLIAMSON COUNTY COURTHOUSE.*?NOTICE OF FORECLOSURE"
                  r"|SUBSTITUTE TRUSTEE'S NOTICE OF FORECLOSURE SALE)",
}


def newspaper_tax_heading(county, excerpt):
    if county not in _CIVIC_OR_PRIVATE:
        return None
    text = " ".join(excerpt.split()).replace("’", "'")
    if re.search(r"\b(?:delinquent (?:real (?:property|estate) |property )?tax|tax sale|back taxes)", text, re.I):
        return None
    if county in _FORECLOSURE and re.match(_FORECLOSURE[county], text, re.I):
        return "foreclosure"
    if any(re.match(pattern, text, re.I) for pattern in _CIVIC_OR_PRIVATE[county]):
        return "exclude"
    return None
