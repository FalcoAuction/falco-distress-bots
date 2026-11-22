import unittest
from unittest.mock import Mock

from src.bots.hamilton_assessor_bot import HamiltonAssessorBot


class ChattanoogaParcelTests(unittest.TestCase):
    def test_county_review_checks_each_case_and_is_repeatable(self):
        bot = HamiltonAssessorBot()
        bot.county_file_stamp = "roll A"
        parcels = {k: {"property_address": street, "parcel": k, "land_use": "Apartments",
                       "owner": "RECORDED OWNER", "appraised": value}
                   for k, street, value in [("1", "20 MAIN ST", 100000), ("2", "22 MAIN ST", 500000),
                                            ("2 L000", "22 MAIN ST", 25000)]}
        ambiguous = set()
        addresses = bot._build_address_index(parcels, ambiguous)
        rows = []
        for i, street in enumerate(["20 MAIN ST", "22 MAIN ST", "22 MAIN ST UNIT A", "40 OTHER RD"]):
            case = f"CE{i}"
            rows.append({"id": str(i), "property_address": street + ", Chattanooga, TN", "case": case,
                         "pipeline_lead_key": bot.make_lead_key("chattanooga_codes", f"{case}|{street}")})
        rows[-1]["pipeline_lead_key"] = "manual"
        client = Mock()
        client.table.return_value.select.return_value.eq.return_value.order.return_value.limit.return_value.execute.return_value.data = rows
        bot._review_chattanooga_parcels(client, parcels, addresses, ambiguous)
        changes = client.rpc.call_args.args[1]["p_rows"]
        evidence = [row["raw_patch"]["chattanooga_parcel_review"] for row in changes]
        self.assertEqual([e["status"] for e in evidence], ["exact_county_address", "ambiguous_address", "no_exact_address"])
        self.assertEqual(evidence[0]["parcel"], "1")
        self.assertNotIn("county_appraisal", evidence[1])
        self.assertNotIn("recorded_owner", evidence[2])
        for row, prior in zip(rows, evidence):
            row["prior"] = prior
        client.rpc.reset_mock()
        bot._review_chattanooga_parcels(client, parcels, addresses, ambiguous)
        client.rpc.assert_not_called()
