import unittest
from copy import deepcopy
from unittest.mock import Mock

from src.bots._knox_poh_assessor import lookup, refresh

DAY = "2026-11-22"
CASE = dict(parcel_id="068LB02001", case_number="C201514188", property_address="5104 Camelia Road, Knoxville, TN")
BILL = dict(account_number="068LB-02001", parcelId="123", bill_type="Real", isPreliminary=False,
            hideFromOnlineServices=False, owner="A & B", taxYear="2026",
            parcelInformation=dict(propertyAddress="5104 CAMELIA RD", ownerName="A & B",
                                   propertyIdentifier="R", specialInterest="0"),
            taxInformation=dict(totalPropertyValue=100200, assessedValue=25050))


class CountyRollTests(unittest.TestCase):
    def read(self, bills, case=CASE):
        fetch = Mock()
        response = fetch.return_value
        response.status_code = 200
        response.json.return_value = {"status": 1, "due": {"PPT": {
            "accounts": [{"account_number": "068LB-02001", "parcel_id": "123"}], "bills": bills}}}
        return lookup(fetch, case, DAY)

    def test_current_full_value_and_account_suffix(self):
        r = self.read([{**BILL, "taxYear": "2025"}, BILL])
        self.assertEqual(r["appraised_value"], 100200)
        self.assertEqual(r["owner"], "A & B")
        self.assertEqual(r["source_url"], "https://propertytax.knoxcountytn.gov/property-tax/bill/068LB-02001/2026")
        self.assertEqual(r["case_number"], CASE["case_number"])

    def test_conflicting_and_incomplete_roll_records(self):
        for change in ({"parcelId": "other"}, {"owner": "X"}, {"taxYear": "2025"}, {"isPreliminary": True},
                       {"account_number": "068LB-020"}, {"bill_type": "Personal"}, {"hideFromOnlineServices": True}):
            with self.subTest(change=change):
                self.assertIsNone(self.read([{**BILL, **change}]))
        self.assertIsNone(self.read([BILL, BILL]))
        for field, value in (("propertyAddress", "5104 CAMELIA RD UNIT A"), ("ownerName", "X"), ("specialInterest", "1")):
            altered = deepcopy(BILL)
            altered["parcelInformation"][field] = value
            self.assertIsNone(self.read([altered]))
        self.assertIsNone(self.read([BILL], {**CASE, "parcel_id": "068LB020"}))

    def test_no_lookup_for_cancelled_hearing(self):
        db, fetch = Mock(), Mock()
        self.assertEqual(refresh(db, {"old": {**CASE, "document_kind": "poh_agenda", "meeting_cancelled": True}},
                                 fetch, DAY), {})
        fetch.assert_not_called()
        db.table.return_value.select.assert_not_called()
