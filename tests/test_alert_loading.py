"""Alert history must include older runs hidden by the API response cap."""
import os
import unittest
from types import SimpleNamespace
from unittest.mock import patch

os.environ["NEXT_PUBLIC_SUPABASE_URL"] = ""
os.environ["SUPABASE_SERVICE_ROLE_KEY"] = ""

from src.bots._alert import load_rows


class HistoryQuery:
    def __init__(self, rows, cap=500, fail_at=None):
        self.rows = rows
        self.cap = cap
        self.fail_at = fail_at
        self.start = 0
        self.end = 5999
        self.offsets = []

    def __getattr__(self, name):
        return lambda *args, **kwargs: self

    def range(self, start, end):
        self.start, self.end = start, end
        self.offsets.append(start)
        return self

    def execute(self):
        if self.fail_at is not None and self.start >= self.fail_at:
            raise RuntimeError("simulated history read failure")
        end = min(self.end + 1, self.start + self.cap)
        return SimpleNamespace(data=self.rows[self.start:end], count=len(self.rows))


class AlertLoadingTests(unittest.TestCase):
    def test_history_beyond_response_cap_is_not_lost(self):
        rows = [{"bot_source": "source", "staged_count": 0} for _ in range(2368)]
        rows[-1] = {"bot_source": "source", "staged_count": 1}
        query = HistoryQuery(rows)
        with patch("src.bots._alert._supabase", return_value=query):
            self.assertEqual(load_rows(), rows)
        self.assertEqual(query.offsets, [0, 500, 1000, 1500, 2000])

    def test_lower_server_cap_does_not_end_history_early(self):
        rows = [{"bot_source": "source"} for _ in range(230)]
        query = HistoryQuery(rows, cap=100)
        with patch("src.bots._alert._supabase", return_value=query):
            self.assertEqual(load_rows(), rows)
        self.assertEqual(query.offsets, [0, 100, 200])

    def test_later_page_failure_cannot_look_like_complete_history(self):
        query = HistoryQuery([{}] * 1001, fail_at=500)
        with patch("src.bots._alert._supabase", return_value=query):
            with self.assertRaisesRegex(RuntimeError, "history read failure"):
                load_rows()

    def test_empty_window_returns_no_rows(self):
        with patch("src.bots._alert._supabase", return_value=HistoryQuery([])):
            self.assertEqual(load_rows(), [])
