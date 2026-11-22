import unittest
from datetime import date

from src.bots.shelby_substitute_trustee_bot import LOAN_DOC, ShelbySubstituteTrusteeBot, build_lead

ROW = {"instrument": "26101152", "code": "APPT", "grantors": ["ALI ABDUL K", "FRIPP GAIL D", "ACME MORTGAGE LLC"],
       "grantees": ["SMITH JOHN S TR"], "recorded": "2026-11-19", "detail_url": "https://example/appt"}
DOT = {"doc_type": "MORTGAGE", "mortgage_amount": 204768.0, "instrument_date": "2021-03-01",
       "grantors": ["FRIPP GAIL D", "ALI ABDUL KAREEM"], "street_number": "9500", "street_name": "AFTON GROVE RD",
       "city": "CORDOVA", "state": "TN", "zip": "38018", "parcel_id": "D0215TD00060"}


class MortgageDocTypeTests(unittest.TestCase):
    def test_mortgage_and_deed_of_trust_both_accepted(self):
        for t in ("MORTGAGE", "DEED OF TRUST", "Deed of Trust"):
            self.assertTrue(LOAN_DOC.search(t), t)
        for t in ("WARRANTY DEED", "RELEASE OF DEED OF TRUST", "MORTGAGE ASSIGNMENT", "RELEASE", ""):
            self.assertFalse(LOAN_DOC.search(t), t)

    def test_mortgage_doc_gives_address_loan_and_borrowers(self):
        lead = build_lead(ShelbySubstituteTrusteeBot(), ROW, {"instrument_date": "2026-11-18"}, DOT,
                          today=date(2026, 11, 20))
        self.assertEqual(lead.property_address, "9500 AFTON GROVE RD, CORDOVA, TN 38018")
        self.assertEqual(lead.raw_payload["party_basis"], "deed_of_trust")
        self.assertEqual(lead.raw_payload["lenders"], ["ACME MORTGAGE LLC"])
        self.assertEqual(lead.raw_payload["loan"]["original_amount"], 204768.0)
        self.assertTrue(lead.mortgage_balance and lead.mortgage_balance < 204768)


if __name__ == "__main__":
    unittest.main()
