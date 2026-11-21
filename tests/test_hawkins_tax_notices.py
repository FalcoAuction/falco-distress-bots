import unittest

from src.bots.tn_public_notice_bot import TnPublicNoticeBot


class HawkinsTaxNotices(unittest.TestCase):
    def build(self, excerpt, county="hawkins", distress="TAX_LIEN"):
        return TnPublicNoticeBot()._build_lead(
            {"notice_id": "1", "county": county, "excerpt": excerpt}, distress, "Tax Sales")

    def test_estate_sale_is_excluded_only_from_hawkins_tax_category(self):
        notice = "CHANCERY COURT SALE\nRE: ESTATE OF Example\n123 First Street"
        self.assertIsNone(self.build(notice))
        self.assertIsNotNone(self.build(notice, county="knox"))
        self.assertIsNotNone(self.build(notice + " Delinquent real property tax sale"))

    def test_tax_rate_notice_and_real_tax_case(self):
        self.assertIsNone(self.build("TOWN OF MOUNT CARMEL, TENNESSEE PUBLIC NOTICE ORDINANCE #26-549: tax rate"))
        self.assertIsNotNone(self.build("Hawkins County tax sale, case 7290, Parcel No. 001-002.00"))
