import unittest
from unittest.mock import Mock

from src.bots._land_gis import county_rows


def response(body):
    return Mock(status_code=200, json=lambda: body)


class CountyPagingTests(unittest.TestCase):
    def test_short_intermediate_page_advances_by_records_received(self):
        get = Mock(side_effect=[
            response({"features": [{"attributes": {"OBJECTID": 2, "APN": "a"}}], "exceededTransferLimit": True}),
            response({"features": [{"attributes": {"OBJECTID": 8, "APN": "b"}}]}),
        ])
        self.assertEqual([r["parcelid"] for r in county_rows("davidson", get)], ["a", "b"])
        self.assertEqual(get.call_args.kwargs["params"]["resultOffset"], 1)

    def test_failed_query_cannot_look_like_a_county_with_zero_parcels(self):
        with self.assertRaises(ValueError):
            list(county_rows("rutherford", Mock(return_value=response({"error": {"code": 400}}))))
