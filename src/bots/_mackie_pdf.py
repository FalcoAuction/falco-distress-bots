"""Read the address-only table in Mackie Wolf's Tennessee sale reports."""

import re

from ._regions import TN_COUNTIES


ADDRESS_HEADERS = {
    "Sale Date File Address Full County Sale Trustee",
    "Sale Date File Address Full County Sale Trustee Designation",
}
CITY_ZIP = re.compile(r"([A-Za-z][A-Za-z .'’-]*) TN ([0-9]{5}(?:-[0-9]{4})?)")
UNIT = re.compile(r"(?:UNIT|APT\.?|SUITE|STE\.?|#)\s*[A-Z0-9]+(?:-[A-Z0-9]+)*", re.I)


def address_columns(header_rest, following_lines, platforms):
    """Return street, city, ZIP, county and trustee designation from one record."""
    # The street is the first column after the file number. Searching for a
    # second number inside it can turn a numbered street into a false owner.
    if not re.match(r"^[0-9]+[A-Za-z]?\s+\S", header_rest):
        return None
    # Require a single TN city/ZIP before any footer. Page breaks may occur
    # between the street and city; unit lines belong to the street column.
    tails = [(i, CITY_ZIP.fullmatch(line)) for i, line in enumerate(following_lines)]
    tails = [(i, m) for i, m in tails if m]
    if len(tails) != 1:
        return None
    i, tail = tails[0]
    wrapped = following_lines[:i]
    if wrapped and (len(wrapped) != 1 or not UNIT.fullmatch(wrapped[0])):
        return None
    address, _, platform = header_rest.rpartition(" ")
    if platform.upper() not in platforms:
        return None
    # Resolve county only at the printed column boundary. Keep multiword
    # county names intact and never use an unknown designation as a county.
    words = address.split()
    for size in (2, 1):
        county = " ".join(words[-size:])
        street = " ".join(words[:-size])
        if (county.lower().replace(" ", "") in TN_COUNTIES
                and re.fullmatch(r"[0-9]+[A-Za-z]?\s+.*[A-Za-z].*", street)):
            return " ".join([street, *wrapped]), tail[1], tail[2], county, platform.upper()
    return None
