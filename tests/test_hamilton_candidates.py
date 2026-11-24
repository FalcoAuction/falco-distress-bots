"""The Hamilton value bot must read every candidate, not only the first pages."""
import unittest

from src.bots.hamilton_assessor_bot import HamiltonAssessorBot


class Query:
    def __init__(self, client, table):
        self.client, self.table, self.after, self.size = client, table, None, None

    def select(self, *_):
        return self

    def or_(self, *_):
        return self

    def order(self, *_):
        return self

    def range(self, *_):
        raise AssertionError("offset paging must not be used")

    def gt(self, column, value):
        assert column == "id"
        self.after = value
        return self

    def limit(self, n):
        self.size = n
        return self

    def execute(self):
        rows = [r for r in self.client.rows[self.table] if self.after is None or r["id"] > self.after]
        self.client.calls.append((self.table, self.after))

        class Result:
            data = [dict(r) for r in rows[:self.size]]
        return Result()


class Client:
    def __init__(self, rows):
        self.rows, self.calls = rows, []

    def table(self, name):
        return Query(self, name)


class Candidates(unittest.TestCase):
    def test_every_row_is_read_by_id_after_the_last_page(self):
        staged = [{"id": f"{i:05d}"} for i in range(1234)]
        live = [{"id": f"{i:05d}"} for i in range(7)]
        client = Client({"homeowner_requests": live, "homeowner_requests_staging": staged})
        bot = HamiltonAssessorBot()
        self.addCleanup(bot.session.close)
        got = bot._candidates(client)
        self.assertEqual(len(got), 1241)
        self.assertEqual(len({(r["__table__"], r["id"]) for r in got}), 1241)
        self.assertIn(("homeowner_requests_staging", "00999"), client.calls)


if __name__ == "__main__":
    unittest.main()
