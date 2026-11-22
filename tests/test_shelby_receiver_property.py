import base64
import json
from datetime import date
from unittest import TestCase
from unittest.mock import Mock

from src.bots._shelby_receiver_property import property_record
from src.bots.shelby_receiver_roster_bot import ShelbyReceiverBot


class ReceiverPropertyTests(TestCase):
    def setUp(self):
        self.day = date(2026, 11, 22)
        self.case = {"warrant": "21400079", "parcel": "072022 E00048", "address": "3072 SAINT CROIX PL"}
        self.listing = {"parcel":"072022 E00048", "alt_parcel":"0720220E000480", "sale_id":"TS2302",
                        "trustee_street":"3072 ST CROIX", "source_url":"https://apps2.shelbycountytrustee.com/Parcel?parcel=072022+E00048"}
        self.bill = {"parcel":self.listing["parcel"], "altParcelId":self.listing["alt_parcel"],
                     "billNumber":26260403, "paymentType":20, "description":"MEMPHIS REALTY", "year":"2026",
                     "location":"3072 ST CROIX", "primaryName":"PRINTED ROLL OWNER", "assessment":3080,
                     "classification":"C"}

    def review(self, changes=None, parcel=None, sale="TS2302", double=False, amount="$3,080.00"):
        bill = {**self.bill, **(changes or {})}
        token = base64.urlsafe_b64encode(json.dumps(bill).encode()).decode()
        row = "<tr><td><a href='?billNumberData="+token+"'>26260403</a></td>" + "".join(
            "<td>"+v+"</td>" for v in ["2026","Shelby","","","","",amount]) + "</tr>"
        head = "".join("<p>"+v+"</p>" for v in ["Location","3072 ST CROIX","Primary Owner","PRINTED ROLL OWNER",
                   "Parcel ID:",parcel or self.listing["parcel"],"Tax Sale ID:",sale])
        return property_record(head+"<table>"+row*(2 if double else 1)+"</table>",self.case,self.listing,self.day)

    def test_value_ratio_and_owner_have_independent_evidence(self):
        for code, assessed, estimate in (("R",1925,7700),("C",3080,7700),("I",20000,50000)):
            e=self.review({"assessment":assessed,"classification":code},amount=f"${assessed:,.2f}")
            self.assertEqual(e["estimated_appraisal"],estimate)
            self.assertEqual(e["tax_roll_owner"],"PRINTED ROLL OWNER")
            self.assertEqual(e["parcel"],self.listing["parcel"])
        e=self.review({"primaryName":"DIFFERENT PARTY"})
        self.assertEqual(e["owner_status"],"unverified")
        self.assertNotIn("tax_roll_owner",e)
        self.assertEqual(e["estimated_appraisal"],7700)
        e=self.review({"classification":"?"})
        self.assertEqual(e["assessed_value"],3080)
        self.assertNotIn("estimated_appraisal",e)
        self.assertEqual(self.review(amount="$1.00")["owner_status"],"trustee_primary_owner")
        self.assertNotIn("assessed_value",self.review(amount="$1.00"))

    def test_wrong_property_bill_or_sale_and_duplicate_bills_held(self):
        variants=[self.review(parcel="072022  E00048"), self.review(sale="TS9999"),self.review(double=True)]
        for change in ({"parcel":"072022 E00014"},{"altParcelId":"0720220E000140"},{"location":"3055 ST CROIX"},
                       {"billNumber":123},{"year":2025},{"paymentType":99},{"description":"PERSONALTY"}):
            variants.append(self.review(change))
        self.listing["trustee_street"]="3072 N ST CROIX"
        variants.append(self.review())
        for e in variants:
            self.assertNotIn("estimated_appraisal",e)
            self.assertNotIn("tax_roll_owner",e)

    def test_only_current_unique_overlaps_trigger_fetch(self):
        bot=ShelbyReceiverBot()
        bot.fetch=Mock(return_value=None)
        receipt={"cases":[{"parcel":"999999 00014"},{**self.case,"current_trustee_listings":[dict(self.listing),dict(self.listing)]},
                          {**self.case,"current_trustee_listings":[dict(self.listing)]}]}
        bot.attach_property_records(receipt,self.day)
        bot.fetch.assert_called_once_with(self.listing["source_url"],timeout=15)
        self.assertFalse(receipt["receiver_appointment_verified"])
        self.assertFalse(receipt["sale_authority_verified"])
        self.assertEqual(receipt["cases"][1]["current_trustee_listings"][0]["property_review"]["status"],"ambiguous_listing")
        self.assertEqual(receipt["cases"][2]["current_trustee_listings"][0]["property_review"]["status"],"source_unavailable")
