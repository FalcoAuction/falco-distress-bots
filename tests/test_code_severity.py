"""Door filter for nuisance code cases; no production writes or network."""
import copy
import importlib
import json
import os
import unittest
from types import SimpleNamespace
from unittest.mock import patch

from src.bots import _code_severity
from src.bots._base import LeadPayload
from src.bots._code_severity import code_severity, door_filter, severity_text
from src.bots.nashville_codes_bot import INACTIVE_REASON, NashvilleCodesBot


def row(violations=None, problem=None, distress="CODE_VIOLATION", **raw):
    codes = {k: v for k, v in (("Violations_Noted", violations), ("Reported_Problem", problem)) if v is not None}
    return {"distress_type": distress, "raw_payload": {"nashville_codes": codes, **raw}}


# (Violations_Noted, Reported_Problem, answer of public.lead_code_severity)
SQL_TABLE = [
    ("HIGH WEEDS", "grass is two feet tall", "nuisance"),
    ("OPEN STORAGE,JUNK,TRASH AND DEBRIS", "junk cars in yard", "nuisance"),
    ("INOP/UNLIC VEHICLE", None, "nuisance"),
    ("EXTERIOR REPAIR,ROOF", "roof is caving in", "nuisance"),
    ("BLDG MAINTENANCE", "broken windows", "nuisance"),
    ("DILAPIDATED SIGN", None, "nuisance"),
    ("DILAPIDATED FENCE", None, "severe"),
    ("DILAPIDATED SIGN,DILAPIDATED FENCE", None, "severe"),
    ("DILAPIDATED", None, "nuisance"),            # the rule needs the space after the word
    ("DILAPIDATED", "house is falling in", "severe"),  # fields join with a space
    ("DILAPIDATED,HIGH WEEDS", None, "nuisance"),  # comma, no space
    ("HIGH WEEDS", "VACANT LOT overgrown", "nuisance"),
    ("HIGH WEEDS", "VACANT HOUSE overgrown", "severe"),
    ("VACANT", "HOUSE has no power", "severe"),     # "VACANT" + " " + "HOUSE" across two fields
    ("", "VACANT", "nuisance"),
    ("HIGH WEEDS", "vacant and open, kids going in", "severe"),
    ("OPEN & ACCESSIBLE", None, "severe"),
    ("OPEN AND ACCESSIBLE", None, "severe"),
    ("OPEN &ACCESSIBLE", None, "nuisance"),
    ("unsafe structure", "lower case input", "severe"),
    ("Structural Damage", None, "severe"),
    ("UNFIT FOR HABITATION", None, "severe"),
    ("UNFIT", None, "nuisance"),
    ("CONDEMNED", None, "severe"),
    ("UNINHABITABLE", None, "severe"),
    ("FIRE DAMAGE", None, "severe"),
    ("FIRE-DAMAGED", None, "severe"),
    ("FIRE  DAMAGE", None, "nuisance"),             # two spaces
    ("DEMOLITION ORDER", None, "severe"),
    ("ROOF", "porch collapsing", "severe"),
    ("BOARD UP REQUIRED", None, "severe"),
    ("BOARDED", None, "severe"),
    ("DERELICT VEHICLE", None, "severe"),           # the rule does not ask what is derelict
    ("ABANDONED VEHICLE", None, "nuisance"),
    ("ABANDONED DWELLING", None, "severe"),
    ("TRASH", "abandoned home next door", "severe"),
    ("", "", "nuisance"),
    (None, None, "nuisance"),
]


class RuleTests(unittest.TestCase):
    def test_table_matches_sql_answers(self):
        self.assertGreaterEqual(len(SQL_TABLE), 25)
        for violations, problem, expected in SQL_TABLE:
            with self.subTest(violations=violations, problem=problem):
                self.assertEqual(code_severity(row(violations, problem)), expected)

    def test_only_code_and_demolition_rows_have_a_severity(self):
        for distress in ("PROBATE", "BANKRUPTCY", "TAX_LIEN", "", None):
            self.assertIsNone(code_severity(row("UNSAFE", distress=distress)))
        for distress in ("CODE_VIOLATION", "DEMOLITION", "code_violation", "Demolition"):
            self.assertEqual(code_severity(row("UNSAFE", distress=distress)), "severe")
            self.assertEqual(code_severity(row("HIGH WEEDS", distress=distress)), "nuisance")

    def test_empty_text_is_nuisance(self):
        for raw in (None, {}, [], "text", {"nashville_codes": None}, {"nashville_codes": "UNSAFE"},
                    {"nashville_codes": {"Violations_Noted": None}}, {"issue": ["UNSAFE"]}):
            with self.subTest(raw=raw):
                self.assertEqual(code_severity({"distress_type": "CODE_VIOLATION", "raw_payload": raw}), "nuisance")

    def test_florida_fields_count_only_when_state_is_fl(self):
        for state, expected in (("FL", "severe"), ("TN", "nuisance"), ("fl", "nuisance"), (None, "nuisance")):
            payload = {"state": state, "case_type": "UNSAFE STRUCTURE", "case_name": "SMITH"}
            self.assertEqual(code_severity({"distress_type": "CODE_VIOLATION", "raw_payload": payload}), expected)
        self.assertEqual(code_severity({"distress_type": "CODE_VIOLATION", "raw_payload": {
            "case_type": "CONDEMNED", "case_name": "UNSAFE"}}), "nuisance")
        self.assertEqual(code_severity({"distress_type": "CODE_VIOLATION", "raw_payload": {
            "state": "FL", "case_type": "OVERGROWTH", "case_name": "VACANT BUILDING"}}), "severe")

    def test_issue_summary_and_description_are_read(self):
        for issue, expected in (({"summary": "Unsafe building"}, "severe"), ({"description": "house was condemned"}, "severe"),
                                ({"summary": "Tall grass", "description": "junk cars"}, "nuisance"), ({"id": 77}, "nuisance")):
            self.assertEqual(code_severity({"distress_type": "CODE_VIOLATION", "raw_payload": {"issue": issue}}), expected)

    def test_text_is_joined_like_concat_ws(self):
        # NULLs are skipped; empty strings keep their separator.
        self.assertEqual(severity_text({"nashville_codes": {"Reported_Problem": "B"}, "issue": {"summary": "C"}}), "B C")
        self.assertEqual(severity_text({"nashville_codes": {"Violations_Noted": "", "Reported_Problem": "B"}}), " B")
        self.assertEqual(severity_text({"state": "FL"}), "")
        self.assertEqual(severity_text({"state": "FL", "case_name": "N", "issue": {"description": "D"}}), "N D")
        # An empty field between two words breaks a phrase, as it does in SQL.
        self.assertEqual(code_severity({"distress_type": "CODE_VIOLATION", "raw_payload": {
            "nashville_codes": {"Violations_Noted": "VACANT", "Reported_Problem": ""}, "issue": {"summary": "HOUSE"}}}), "nuisance")
        self.assertEqual(severity_text({"nashville_codes": {"Violations_Noted": 12, "Reported_Problem": True}}), "12 true")

    def test_accepts_a_lead_payload(self):
        lead = LeadPayload(bot_source="nashville_codes", pipeline_lead_key="k", distress_type="CODE_VIOLATION",
                           raw_payload={"nashville_codes": {"Violations_Noted": "UNSAFE STRUCTURE"}})
        self.assertEqual(code_severity(lead), "severe")

    def test_other_city_rows_are_read_from_their_own_fields(self):
        # Site migration 20261127000100: the rule reads each other city's own
        # violation field. These bots are still not filtered at the door.
        shapes = {
            "memphis_codes": {"memphis_311": {"REQUEST_TYPE": "CE-Unsafe Structure"}, "memphis_codes_review": {"request_type": "CE-Unsafe Structure"}},
            "chattanooga_codes": {"chattanooga_codes": {"descriptions": ["UNSAFE STRUCTURES"], "rows": [{"description": "CONDEMNED"}]}},
            "mtn_cities_codes": {"city": "Mt. Juliet", "issue": {"id": 1}, "mt_juliet_issue_review": {"condition_excerpt": "condemned house"}},
            "knoxville_poh": {"poh_section": "case", "violations": "UNFIT FOR HUMAN HABITATION", "body_excerpt": "DEMOLITION"},
            "johnson_city_bdsr": {"johnson_city_bdsr": {"violations": "Unsafe structure; demolition"}},
        }
        for name, raw in shapes.items():
            with self.subTest(bot=name):
                self.assertEqual(code_severity({"distress_type": "CODE_VIOLATION", "raw_payload": raw}), "severe")
        # Text kept only in a field the rule does not read is still not seen.
        self.assertEqual(code_severity({"distress_type": "CODE_VIOLATION", "raw_payload": {
            "memphis_311": {"REQUEST_TYPE": "CE-Roof"}, "memphis_codes_review": {"request_type": "CE-Unsafe Structure"}}}),
            "nuisance")


class SwitchTests(unittest.TestCase):
    def tearDown(self):
        importlib.reload(_code_severity)

    def test_on_by_default_and_off_by_environment(self):
        for value, expected in ((None, True), ("1", True), ("", True), ("0", False), ("false", False), ("OFF", False), (" no ", False)):
            env = {k: v for k, v in os.environ.items() if k != "CODE_NUISANCE_DOOR_FILTER"}
            if value is not None:
                env["CODE_NUISANCE_DOOR_FILTER"] = value
            with self.subTest(value=value), patch.dict(os.environ, env, clear=True):
                self.assertIs(importlib.reload(_code_severity).DOOR_FILTER_ON, expected)

    def test_filter_drops_only_nuisance(self):
        leads = [row("HIGH WEEDS"), row("UNSAFE"), row("HIGH WEEDS", distress="PROBATE")]
        with patch.object(_code_severity, "DOOR_FILTER_ON", True):
            self.assertEqual(_code_severity.door_filter(leads), ([leads[1], leads[2]], 1))
        with patch.object(_code_severity, "DOOR_FILTER_ON", False):
            self.assertEqual(_code_severity.door_filter(leads), (leads, 0))


def attrs(**kw):
    return {"ObjectId": 1, "Request_Nbr": "26-900", "Property_APN": "00102003000",
            "Property_Address": "100 FARM RD", "Property_Owner": "JANE DOE", "City": "Nashville",
            "Status": "OPEN", "Violations_Noted": "EXTERIOR REPAIR,ROOF", "Reported_Problem": "tall grass and junk",
            "Last_Activity_Date": 1795000000000, **kw}


SEVERE = dict(Violations_Noted="UNSAFE STRUCTURE,ROOF")


class Harness:
    """The real NashvilleCodesBot._write_staging over an in-memory staging table."""

    def __init__(self, staging=(), live=()):
        self.db = {"homeowner_requests_staging": list(staging), "homeowner_requests": list(live)}
        self.inserted = []
        self.official = []

    def candidates(self, client):
        return [{"id": r["id"], "pipeline_lead_key": r["pipeline_lead_key"],
                 "case_num": r["raw_payload"]["nashville_codes"]["Request_Nbr"],
                 "checked_on": (r["raw_payload"].get("nashville_codes_review") or {}).get("checked_on")}
                for r in self.db["homeowner_requests_staging"]]

    def insert(self, leads):
        for lead in leads:
            self.inserted.append(lead.pipeline_lead_key)
            self.db["homeowner_requests_staging"].append({
                "id": "row-" + lead.pipeline_lead_key[:8], "pipeline_lead_key": lead.pipeline_lead_key,
                "staging_status": "pending", "rejection_reason": None, "raw_payload": copy.deepcopy(lead.raw_payload)})
        return len(leads), 0

    def client(self):
        db = self.db

        class Query:
            def __init__(self, table): self.table = table; self.filters = []; self.values = None
            def select(self, cols): return self
            def eq(self, key, value):
                if key == "raw_payload":
                    value = json.loads(value)
                if key not in ("source", "bot_source"):
                    self.filters.append((key, value))
                return self
            def in_(self, key, values): self.filters.append((key, values)); return self
            def is_(self, key, value): self.filters.append((key, None)); return self
            def update(self, values): self.values = values; return self
            def execute(self):
                matched = [r for r in db[self.table] if all(
                    r.get(k) in v if isinstance(v, list) else r.get(k) == v for k, v in self.filters)]
                if self.values is not None:
                    for r in matched:
                        r.update(copy.deepcopy(self.values))
                return SimpleNamespace(data=copy.deepcopy(matched))
        return SimpleNamespace(table=Query)

    def run(self, records, on=True, official=None):
        """One run: `records` is what Metro lists as OPEN; `official` answers refresh queries."""
        bot = NashvilleCodesBot()
        leads = [lead for r in records if (lead := bot._build_lead(r)) is not None]
        with patch("src.bots.nashville_codes_bot._supabase", return_value=self.client()), \
             patch.object(bot, "_candidates", side_effect=self.candidates), \
             patch.object(bot, "_query", return_value=records if official is None else official), \
             patch("src.bots.nashville_codes_bot.BotBase._write_staging", side_effect=self.insert), \
             patch.object(_code_severity, "DOOR_FILTER_ON", on):
            staged, dupes = bot._write_staging(leads)
        return bot, staged, dupes

    def cases(self):
        return sorted(r["raw_payload"]["nashville_codes"]["Request_Nbr"] for r in self.db["homeowner_requests_staging"])


def stored(case="26-900", **kw):
    record = attrs(Request_Nbr=case, **kw)
    return {"id": "old-" + case, "pipeline_lead_key": NashvilleCodesBot.make_lead_key("nashville_codes", case),
            "staging_status": "pending", "rejection_reason": None,
            "raw_payload": {"nashville_codes": record, "unrelated": {"keep": True}}}


class DoorTests(unittest.TestCase):
    def test_fixture_cases_pass_the_bot_gate_and_differ_only_by_the_sql_rule(self):
        bot = NashvilleCodesBot()
        self.assertEqual(code_severity(bot._build_lead(attrs())), "nuisance")
        self.assertEqual(code_severity(bot._build_lead(attrs(**SEVERE))), "severe")

    def test_new_nuisance_case_is_counted_and_not_staged(self):
        h = Harness()
        bot, staged, dupes = h.run([attrs()])
        self.assertEqual((staged, dupes, h.cases()), (0, 0, []))
        self.assertEqual(bot.door_counts, {"nuisance_not_staged": 1})

    def test_new_severe_case_is_staged(self):
        h = Harness()
        bot, staged, dupes = h.run([attrs(**SEVERE), attrs(Request_Nbr="26-901", ObjectId=2)])
        self.assertEqual((staged, dupes, h.cases()), (1, 0, ["26-900"]))
        self.assertEqual(bot.door_counts, {"nuisance_not_staged": 1})

    def test_existing_nuisance_row_is_refreshed_exactly_as_with_the_filter_off(self):
        results = []
        for on in (True, False):
            h = Harness(staging=[stored()], live=[{k: v for k, v in stored(id="live").items()
                                                  if k not in ("staging_status", "rejection_reason")}])
            bot, staged, dupes = h.run([attrs()], on=on, official=[attrs(Status="DONE")])
            results.append((copy.deepcopy(h.db), bot.refresh_counts, staged, dupes))
            row_ = h.db["homeowner_requests_staging"][0]
            self.assertEqual(len(h.db["homeowner_requests_staging"]), 1)   # never deleted, never duplicated
            self.assertEqual((row_["staging_status"], row_["rejection_reason"]), ("rejected", INACTIVE_REASON))
            self.assertEqual(row_["raw_payload"]["nashville_codes"]["Status"], "DONE")
            self.assertEqual(row_["raw_payload"]["unrelated"], {"keep": True})
            self.assertEqual(h.db["homeowner_requests"][0]["raw_payload"]["nashville_codes"]["Status"], "DONE")
            self.assertEqual(bot.refresh_counts["updated"], 2)
            self.assertEqual((staged, dupes, h.inserted), (0, 1, []))
            self.assertEqual(bot.door_counts, {"nuisance_not_staged": 0})  # a stored case is not "dropped"
        self.assertEqual(results[0], results[1])

    def test_existing_nuisance_row_keeps_text_updates(self):
        h = Harness(staging=[stored()])
        h.run([attrs(Reported_Problem="tall grass, more junk")])
        row_ = h.db["homeowner_requests_staging"][0]
        self.assertEqual(row_["raw_payload"]["nashville_codes"]["Reported_Problem"], "tall grass, more junk")
        self.assertEqual(row_["staging_status"], "pending")

    def test_escalated_case_is_staged_on_the_run_where_it_turns_severe(self):
        h = Harness()
        bot, staged, _ = h.run([attrs()])
        self.assertEqual((staged, h.cases(), bot.door_counts["nuisance_not_staged"]), (0, [], 1))
        bot, staged, _ = h.run([attrs(Violations_Noted="EXTERIOR REPAIR,ROOF,UNSAFE STRUCTURE")])
        self.assertEqual((staged, h.cases(), bot.door_counts["nuisance_not_staged"]), (1, ["26-900"], 0))
        # The complaint text alone can escalate it too.
        h = Harness()
        h.run([attrs()])
        _, staged, _ = h.run([attrs(Reported_Problem="house was condemned")])
        self.assertEqual((staged, h.cases()), (1, ["26-900"]))

    def test_switch_off_stages_every_case_the_bot_builds(self):
        h = Harness()
        bot, staged, dupes = h.run([attrs(), attrs(Request_Nbr="26-901", ObjectId=2, **SEVERE)], on=False)
        self.assertEqual((staged, dupes, h.cases()), (2, 0, ["26-900", "26-901"]))
        self.assertEqual(bot.door_counts, {"nuisance_not_staged": 0})

    def test_repeat_run_changes_nothing_and_counts_again(self):
        h = Harness()
        records = [attrs(), attrs(Request_Nbr="26-901", ObjectId=2, **SEVERE)]
        first = h.run(records)
        snapshot = copy.deepcopy(h.db)
        second = h.run(records)
        self.assertEqual((first[1], first[2], first[0].door_counts), (1, 0, {"nuisance_not_staged": 1}))
        self.assertEqual((second[1], second[2], second[0].door_counts), (0, 1, {"nuisance_not_staged": 1}))
        self.assertEqual(h.inserted, [NashvilleCodesBot.make_lead_key("nashville_codes", "26-901")])
        self.assertEqual(h.cases(), ["26-901"])
        # The second run writes nothing: the stored severe case was checked today.
        for before, after in zip(snapshot["homeowner_requests_staging"], h.db["homeowner_requests_staging"]):
            self.assertEqual(before, after)

    def test_run_result_and_health_notes_carry_the_count(self):
        h = Harness()
        bot = NashvilleCodesBot()
        records = [attrs(), attrs(Request_Nbr="26-901", ObjectId=2, **SEVERE)]
        health = []
        with patch("src.bots.nashville_codes_bot._supabase", return_value=h.client()), \
             patch("src.bots._base._supabase", return_value=None), \
             patch.object(bot, "_candidates", side_effect=h.candidates), \
             patch.object(bot, "_query", return_value=records), \
             patch("src.bots.nashville_codes_bot.BotBase._write_staging", side_effect=h.insert), \
             patch.object(bot, "_report_health", side_effect=lambda **kw: health.append(kw)), \
             patch.object(_code_severity, "DOOR_FILTER_ON", True):
            result = bot.run()
        self.assertEqual(result["nuisance_not_staged"], 1)
        self.assertEqual((result["fetched"], result["staged"], result["duplicates"]), (2, 1, 0))
        self.assertEqual(health[-1]["notes"]["nuisance_not_staged"], 1)
        self.assertEqual(health[-1]["notes"]["key_count"], 2)
        self.assertIn("fingerprint", health[-1]["notes"])

    def test_other_bots_report_no_door_count(self):
        from src.bots._base import BotBase

        class Plain(BotBase):
            name = "plain"
            def scrape(self): return []
        bot = Plain()
        health = []
        with patch("src.bots._base._supabase", return_value=None), \
             patch.object(bot, "_report_health", side_effect=lambda **kw: health.append(kw)):
            result = bot.run()
        self.assertNotIn("nuisance_not_staged", result)
        self.assertIsNone(health[-1]["notes"])

    def test_runner_log_and_summary_carry_the_count(self):
        import io
        from contextlib import redirect_stdout
        from src.bots import _run_new
        out = io.StringIO()
        fake = lambda: {"status": "ok", "staged": 1, "duplicates": 0, "fetched": 2, "nuisance_not_staged": 1}
        with redirect_stdout(out), patch.object(_run_new, "close_open_runs"):
            summary = _run_new.run_pipeline([("nashville_codes", fake)], 600, summary_path=None)
        self.assertIn("1 nuisance not staged", out.getvalue())
        self.assertEqual(summary["bots"][0]["nuisance_not_staged"], 1)


if __name__ == "__main__":
    unittest.main()
