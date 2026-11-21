import unittest
from unittest.mock import Mock
from src.bots.stalled_build_bot import review, stalled, StalledBuildBot


class StalledBuildTests(unittest.TestCase):
    def setUp(self):
        self.parcel = {"APN": "123", "Owner": "Builder, LLC", "OwnDate": 100}
        self.permit = {"Permit__": "456", "Date_Issued": 200, "Const_Cost": 900000}
        self.case = {"Property_APN": "123", "Property_Owner": "BUILDER LLC", "Status": "OPEN",
                     "Request_Nbr": "789", "Date_Received": 300,
                     "Violations_Noted": "OPEN VACANT BUILDING"}

    def test_contractor_change_and_large_budget_are_not_distress(self):
        self.assertEqual(review(self.permit, self.parcel, [])['status'], 'watchlist')

    def test_matching_open_official_structural_case_supports_review(self):
        got = review(self.permit, self.parcel, [self.case])
        self.assertEqual(got['status'], 'corroborated')
        self.assertEqual(got['evidence'][0]['case'], '789')

    def test_resolved_different_owner_parcel_or_old_case_cannot_qualify(self):
        for key, value in [('Status', 'DONE'), ('Property_APN', '124'),
                           ('Property_Owner', 'Other LLC'), ('Date_Received', 50),
                           ('Date_Received', None), ('Request_Nbr', None)]:
            self.assertEqual(review(self.permit, self.parcel, [{**self.case, key: value}])['status'], 'watchlist')

    def test_complaint_alone_is_not_an_official_structural_finding(self):
        case = {**self.case, 'Violations_Noted': 'BUILDING PERMIT REQUIRED',
                'Reported_Problem': 'Abandoned and structurally dangerous'}
        self.assertEqual(review(self.permit, self.parcel, [case])['status'], 'watchlist')

    def test_sale_after_permit_keeps_old_project_out(self):
        parcel = {**self.parcel, 'OwnDate': 250}
        self.assertFalse(stalled(self.permit, parcel))
        self.assertEqual(review(self.permit, parcel, [self.case])['status'], 'watchlist')

    def test_collector_keeps_budget_separate_from_county_value(self):
        bot = StalledBuildBot()
        permit = {**self.permit, 'Parcel': '123'}
        parcel = {**self.parcel, 'TotlAppr': 95000, 'PropAddr': '1 MAIN ST'}
        responses = []
        for items in ([permit], [parcel], []):
            response = Mock(status_code=200)
            response.json.return_value = {'features': [{'attributes': x} for x in items]}
            responses.append(response)
        bot.fetch = Mock(side_effect=responses)
        lead = bot.scrape()[0]
        self.assertEqual(lead.property_value, 95000)
        self.assertEqual(lead.raw_payload['permit']['Const_Cost'], 900000)
        self.assertEqual(lead.raw_payload['stalled_review']['status'], 'watchlist')


if __name__ == '__main__':
    unittest.main()
