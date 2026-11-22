import unittest
from unittest.mock import Mock

from src.bots._warn_metro import worksite_parcel


class WorksiteParcelTests(unittest.TestCase):
    address = "20 Culvert Street, Nashville, Tennessee 37210"
    attrs = {"APN": "10508026600", "PropAddr": "20 CULVERT ST", "Owner": "RECORDED CO",
             "TotlAppr": 9648500}

    def lookup(self, features, **extra):
        fetch = Mock(return_value=Mock(status_code=200, json=lambda: {"features": features, **extra}))
        return worksite_parcel(self.address, fetch)

    def test_exact_site_keeps_county_owner_and_source(self):
        found = self.lookup([{"attributes": self.attrs}])
        self.assertEqual(found["recorded_owner"], "RECORDED CO")
        self.assertEqual(found["county_appraisal"], 9648500)
        self.assertIn("10508026600", found["parcel_id"])
        self.assertIn("PropAddr", found["source_url"])

    def test_ambiguous_incomplete_and_wrong_site_withheld(self):
        f = {"attributes": self.attrs}
        self.assertIsNone(self.lookup([f, f]))
        self.assertIsNone(self.lookup([f], exceededTransferLimit=True))
        self.assertIsNone(self.lookup([{"attributes": {**self.attrs, "PropAddr": "201 CULVERT ST"}}]))
        fetch = Mock()
        self.assertIsNone(worksite_parcel("20 Culvert Street, Suite 2, Nashville, TN 37210", fetch))
        fetch.assert_not_called()
