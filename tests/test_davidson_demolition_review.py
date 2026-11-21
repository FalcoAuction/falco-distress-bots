import unittest

from src.bots.davidson_demolition_bot import DavidsonDemolitionBot, permit_review


class DemolitionReviewTests(unittest.TestCase):
    def test_contractor_is_only_a_permit_contact(self):
        lead = DavidsonDemolitionBot()._build_lead({"Permit__": "123", "Address": "1 OAK",
                                                   "Contact": "Example Contractor"})
        self.assertIsNone(lead.owner_name_records)

    def test_finding_requires_same_owner_and_current_case(self):
        p = {"Date_Issued": 20}
        parcel = {"Owner": "Owner", "OwnDate": 10, "APN": "1"}
        case = {"Status": "OPEN", "Request_Nbr": "1", "Property_APN": "1", "Property_Owner": "Owner",
                "Date_Received": 11, "Violations_Noted": "FAILURE TO SECURE"}
        self.assertEqual(permit_review(p, parcel, [case])["status"], "open_structural_case")
        for changed in ({"Status": "DONE"}, {"Property_Owner": "Prior owner"}, {"Date_Received": 9}):
            self.assertEqual(permit_review(p, parcel, [case | changed])["status"], "permit_only")
        self.assertEqual(permit_review(p, parcel | {"OwnDate": 21}, [case])["status"], "ownership_changed")
