"""CourtListener bot: token auth, loud failure on 401, Chapter 7 trustee.

    python -m unittest tests.test_courtlistener_trustee -v
"""

import os
import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
os.environ["NEXT_PUBLIC_SUPABASE_URL"] = ""
os.environ["SUPABASE_SERVICE_ROLE_KEY"] = ""

from src.bots import courtlistener_bankruptcy_bot as cl  # noqa: E402


class FakeResponse:
    def __init__(self, status, payload=None):
        self.status_code = status
        self._payload = payload or {}

    def json(self):
        return self._payload


def search_page(*hits):
    return {"results": list(hits), "next": None}


def hit(chapter, docket_id, name):
    return {"caseName": name, "chapter": chapter, "dateFiled": "2099-01-01", "docketNumber": f"26-{docket_id}",
            "docket_id": docket_id, "party": [name], "docket_absolute_url": f"/docket/{docket_id}/x/"}


class Bot(cl.CourtListenerBankruptcyBot):
    def __init__(self, responses):
        super().__init__()
        self.responses = responses
        self.calls = []

    def fetch(self, url, method="GET", timeout=30.0, **kwargs):
        self.calls.append((url, kwargs.get("params"), kwargs.get("headers")))
        return self.responses(url, kwargs.get("params") or {})


class CourtListenerTests(unittest.TestCase):
    def tearDown(self):
        os.environ.pop(cl.TOKEN_ENV, None)

    def test_401_fails_loudly_instead_of_zero_yield(self):
        bot = Bot(lambda url, params: FakeResponse(401))
        with self.assertRaises(cl.CourtListenerAuthError) as ctx:
            bot.scrape()
        self.assertIn(cl.TOKEN_ENV, str(ctx.exception))
        out = bot.run()
        self.assertEqual(out["status"], "failed")

    def test_token_header_and_chapter7_trustee_lookup(self):
        os.environ[cl.TOKEN_ENV] = "synthetic-token"

        def responses(url, params):
            if url == cl.CL_BANKRUPTCY_INFO:
                return FakeResponse(200, {"results": [{"trustee_str": "Pat Synthetic Trustee"}]})
            if params.get("court") == "tnmb":
                return FakeResponse(200, search_page(hit("7", 101, "Debtor Seven"), hit("13", 102, "Debtor Thirteen")))
            return FakeResponse(200, search_page())

        bot = Bot(responses)
        leads = bot.scrape()
        self.assertTrue(all(h and h.get("Authorization") == "Token synthetic-token" for _, _, h in bot.calls))
        by_name = {l.full_name: l for l in leads}
        self.assertEqual(by_name["Debtor Seven"].raw_payload["trustee"], "Pat Synthetic Trustee")
        self.assertIn("trustee=Pat Synthetic Trustee", by_name["Debtor Seven"].admin_notes)
        # Chapter 13 has a standing trustee, not a liquidating one: no lookup.
        self.assertIsNone(by_name["Debtor Thirteen"].raw_payload["trustee"])
        lookups = [c for c in bot.calls if c[0] == cl.CL_BANKRUPTCY_INFO]
        self.assertEqual([c[1] for c in lookups], [{"docket": 101}])

    def test_trustee_lookup_is_capped(self):
        os.environ[cl.TOKEN_ENV] = "synthetic-token"

        def responses(url, params):
            if url == cl.CL_BANKRUPTCY_INFO:
                return FakeResponse(200, {"results": []})
            if params.get("court") == "tnmb":
                return FakeResponse(200, search_page(*[hit("7", 200 + i, f"D{i}") for i in range(5)]))
            return FakeResponse(200, search_page())

        bot = Bot(responses)
        bot.max_trustee_lookups = 2
        leads = bot.scrape()
        self.assertEqual(len(leads), 5)
        self.assertEqual(len([c for c in bot.calls if c[0] == cl.CL_BANKRUPTCY_INFO]), 2)


if __name__ == "__main__":
    unittest.main()
