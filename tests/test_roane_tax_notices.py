import unittest

from src.bots._roane_tax import exhibit_leads
from src.bots.tn_public_notice_bot import TnPublicNoticeBot


class RoaneTaxTests(unittest.TestCase):
    def test_one_lead_per_complete_real_parcel(self):
        row = {"excerpt": "City of Rockwood 2025 Delinquent Real Property Taxes Example "
               "05-044 - - 044-039.00 Example 05-054K-B-54K-001.01 "
               "Personal Property Taxes 05-044--044-038.00", "notice_id": "1"}
        leads = exhibit_leads(row)
        self.assertEqual([l.raw_payload["parcel"] for l in leads], ["073 044 03900 000", "073 054K B 00101 000"])
        self.assertEqual(leads[0].pipeline_lead_key, exhibit_leads(dict(row, notice_id="2"))[0].pipeline_lead_key)
        self.assertIsNone(leads[0].trustee_sale_date)

    def test_county_exhibit_uses_control_map_and_special_interest(self):
        row = {"excerpt": "Exhibit A 2021 Roane County Delinquent Tax Real Property 3/47P/A/47O/8/1 "
               "Name 3/47P/A/47O/9/", "notice_id": "3"}
        lead, = exhibit_leads(row)
        self.assertEqual(lead.raw_payload["parcel"], "073 047O A 00800 001")

    def test_municipal_tax_caption_requires_property_evidence(self):
        row = {"excerpt": "IN THE CHANCERY COURT FOR ROANE COUNTY CITY OF KINGSTON v. DELINQUENT TAXPAYERS",
               "notice_id": "1", "county": "roane"}
        bot = TnPublicNoticeBot()
        self.assertIsNone(bot._build_lead(row, "TAX_LIEN", "Tax Sales"))
        row["excerpt"] += " property address: 123 Main Street, Kingston TN"
        self.assertIsNotNone(bot._build_lead(row, "TAX_LIEN", "Tax Sales"))
