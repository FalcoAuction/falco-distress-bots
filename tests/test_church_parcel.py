"""Official-source response fixtures for the recurring church research lookup."""
import json
import unittest
from unittest.mock import MagicMock, Mock, patch

from src.bots._church_parcel import match
from src.bots.church_closure_bot import ChurchClosureBot


def closure(name, address, county):
    return dict(church=name + " United Methodist Church", address=address, county=county, state="TN")


def point(county, number, street, city):
    return dict(OBJECTID=1, County=county.upper(), Add_Number=number, StNam_Full=street,
                Post_City=city, Longitude=-88, Latitude=36, Zip_Code="37090")


def parcel(county, owner, street):
    return dict(COUNTY_NAME=county, OWNER=owner, OWNER2="", ADDRESS=street, PARCELID="001 002    00300 000 2027")


def lookup(*pages):
    return Mock(side_effect=[(page, f"https://example.org/{i}") for i, page in enumerate(pages)])


class ChurchParcelTests(unittest.TestCase):
    def test_berlin_county_owned_published_address(self):
        c = closure("Berlin", "1660 Old Berlin Road", "marshall")
        p = point("marshall", 1660, "OLD BERLIN RD", "LEWISBURG")
        result = match(c, lookup([p], [parcel("Marshall", "MARSHALL CO", "OLD BERLIN RD 1660")]))
        self.assertEqual(result["reason"], "county_owner_unconfirmed")
        self.assertNotIn("parcel_id", result)

    def test_good_hope_suffix_companion_blocks_geometry(self):
        c = closure("Good Hope", "1070 North Commerce Rd. Lebanon, TN 37090", "wilson")
        p = point("wilson", 1070, "N COMMERCE RD", "LEBANON")
        q = lookup([p, dict(p, OBJECTID=2, AddNum_Suf="A")])
        self.assertEqual(match(c, q)["reason"], "ambiguous_address_points")
        self.assertEqual(q.call_count, 1)

    def test_short_mountain_requires_independent_street_and_named_owner(self):
        c = closure("New Short Mountain", "7312 Short Mountain, Woodbury", "cannon")
        p = point("cannon", 7312, "SHORT MOUNTAIN RD", "WOODBURY")
        a = parcel("Cannon", "SHORT MOUNTAIN COMMUNITY CHURCH", "SHORT MOUNTAIN RD")
        self.assertEqual(match(c, lookup([p], [a]))["status"], "parcel_identified")
        for bad in (dict(a, ADDRESS="OFF SHORT MOUNTAIN RD"), dict(a, OWNER="COMMUNITY CHURCH"),
                    dict(a, COUNTY_NAME="Smith")):
            self.assertEqual(match(c, lookup([p], [bad]))["status"], "held")
        self.assertEqual(match(c, lookup([p], [a, a]))["reason"], "parcel_count")
        self.assertEqual(match(c, lookup([p, dict(p, StNam_Full="SHORT MOUNTAIN LN")]))["status"], "held")

    def test_shelby_exact_key_and_worship_use_required(self):
        c = closure("Temple", "4404 Sykes Rd, Millington", "shelby")
        p = point("shelby", 4404, "SYKES RD", "MILLINGTON")
        a = dict(ADDRESS="4404 SYKES RD", CITY="MILLINGTON", STATUS=730,
                 STRUCDESC="PLACE OF WORSHIP", GISLINK="M0126   00062")
        polygon = dict(PARCELID=a["GISLINK"])
        result = match(c, lookup([p], [a], [polygon]))
        self.assertEqual(result["parcel_id"], a["GISLINK"])
        self.assertNotIn("assessment_owner", result)
        for bad in (dict(a, GISLINK="M0126 00062"), dict(a, STATUS=731), dict(a, STRUCDESC="HOUSE")):
            self.assertEqual(match(c, lookup([p], [bad], [polygon]))["status"], "held")

    def test_refresh_guards_existing_data_and_replays_once_per_day(self):
        bot = ChurchClosureBot()
        c = closure("Pisgah", "4860 Old State Rt 1, New Johnsonville", "humphreys")
        raw = {"source": "twk_umc_closures", "church_closure": c, "church_review": {"status": "parcel_review"}}
        row = dict(id="1", property_address=c["address"] + ", TN", county=c["county"],
                   pipeline_lead_key=bot.make_lead_key(bot.name, c["church"] + "|" + c["address"]), raw_payload=raw)
        client = MagicMock()
        table = client.table.return_value
        for method in ("select", "eq", "or_", "order", "limit", "filter", "update"):
            getattr(table, method).return_value = table
        table.execute.return_value.data = [row]
        with patch("src.bots.church_closure_bot.match", wraps=lambda _: match(c, lookup([]))) as matcher:
            bot._refresh_parcels(client)
            written = table.update.call_args.args[0]["raw_payload"]
            self.assertEqual(written["church_review"], raw["church_review"])
            self.assertEqual(written["church_parcel_lookup"]["status"], "held")
            table.filter.assert_called_with("raw_payload", "eq", json.dumps(raw, separators=(",", ":")))
            row["raw_payload"] = written
            bot._refresh_parcels(client)
            matcher.assert_called_once()
            table.update.assert_called_once()
