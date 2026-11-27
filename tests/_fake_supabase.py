"""In-memory stand-in for the Supabase client, for reader tests.

Implements the PostgREST pieces the bots use: select lists with aliases and
JSON paths ("alias:raw_payload->key->>field"), eq / neq / in_ / is_ / not_.is_
/ gt / gte, order by one column, range and limit, update with filters, and
rpc("apply_staging_enrichment"). Every executed read is recorded in
`client.reads` as {"table", "select", "filters", "rows", "ids"} so a test can
assert what a bot asked for.
"""
from __future__ import annotations

import copy
import json
import re


def _split(select):
    return [p.strip() for p in select.split(",") if p.strip()]


def _path(row, expr):
    """Value of "col", "col->a->b" or "col->a->>b" on a row."""
    steps = re.split(r"(->>|->)", expr.strip())
    value = row.get(steps[0].strip())
    for op, key in zip(steps[1::2], steps[2::2]):
        value = value.get(key.strip()) if isinstance(value, dict) else None
        if op == "->>" and value is not None and not isinstance(value, str):
            value = json.dumps(value)
    return value


def project(row, select):
    if select.strip() == "*":
        return copy.deepcopy(row)
    out = {}
    for part in _split(select):
        alias, expr = part.split(":", 1) if ":" in part else (None, part)
        name = (alias or re.split(r"->>|->", expr)[-1]).strip()
        out[name] = copy.deepcopy(_path(row, expr))
    return out


class _Result:
    def __init__(self, data):
        self.data = data


class _Not:
    def __init__(self, query):
        self.query = query

    def is_(self, column, value):
        return self.query._add(("not.is", column, value), lambda r: _path(r, column) is not None)


class Query:
    def __init__(self, client, table):
        self.client, self.table = client, table
        self.select_text = None
        self.filters, self.tests = [], []
        self.order_by, self.desc = None, False
        self.window = None
        self.patch = None

    def _add(self, record, test):
        self.filters.append(record)
        self.tests.append(test)
        return self

    def select(self, columns="*", **_):
        self.select_text = columns
        return self

    def update(self, patch):
        self.patch = patch
        return self

    def insert(self, rows, **_):
        self.client.inserts.append((self.table, rows))
        return self

    upsert = insert

    def eq(self, column, value):
        return self._add(("eq", column, value), lambda r: _path(r, column) == value)

    def neq(self, column, value):
        # SQL semantics: NULL <> 'x' is not true.
        return self._add(("neq", column, value),
                         lambda r: _path(r, column) is not None and _path(r, column) != value)

    def in_(self, column, values):
        values = list(values)
        return self._add(("in", column, values), lambda r: _path(r, column) in values)

    def is_(self, column, value):
        return self._add(("is", column, value), lambda r: _path(r, column) is None)

    @property
    def not_(self):
        return _Not(self)

    def gt(self, column, value):
        return self._add(("gt", column, value), lambda r: _path(r, column) is not None and _path(r, column) > value)

    def gte(self, column, value):
        return self._add(("gte", column, value), lambda r: _path(r, column) is not None and _path(r, column) >= value)

    def filter(self, column, op, value):
        assert op == "eq"
        return self._add(("filter.eq", column, value),
                         lambda r: json.dumps(_path(r, column), separators=(",", ":")) == value)

    def order(self, column, desc=False, **_):
        self.order_by, self.desc = column, desc
        return self

    def range(self, start, end):
        self.window = (start, end + 1)
        return self

    def limit(self, n):
        self.window = (0, n)
        return self

    def execute(self):
        rows = [r for r in self.client.tables.setdefault(self.table, []) if all(t(r) for t in self.tests)]
        if self.patch is not None:
            for r in rows:
                r.update(copy.deepcopy(self.patch))
            self.client.updates.append({"table": self.table, "patch": copy.deepcopy(self.patch),
                                        "ids": [r.get("id") for r in rows], "filters": list(self.filters)})
            return _Result([copy.deepcopy(r) for r in rows])
        if self.select_text is None:
            return _Result([])
        if self.order_by:
            rows.sort(key=lambda r: (_path(r, self.order_by) is None, _path(r, self.order_by)), reverse=self.desc)
        if self.window:
            rows = rows[self.window[0]:self.window[1]]
        rows = rows[:self.client.max_rows]
        self.client.reads.append({"table": self.table, "select": self.select_text,
                                  "filters": list(self.filters), "rows": len(rows),
                                  "ids": [r.get("id") for r in rows]})
        return _Result([project(r, self.select_text) for r in rows])


class _Rpc:
    def __init__(self, client, name, args):
        self.client, self.name, self.args = client, name, args

    def execute(self):
        self.client.rpcs.append((self.name, copy.deepcopy(self.args)))
        done = 0
        for item in self.args.get("p_rows", []):
            for r in self.client.tables.get("homeowner_requests_staging", []):
                if r.get("id") == item["id"]:
                    for src, dest in (("raw_patch", "raw_payload"), ("meta_patch", "phone_metadata")):
                        if item.get(src):
                            r[dest] = {**(r.get(dest) or {}), **copy.deepcopy(item[src])}
                    done += 1
        return _Result(done)


class FakeClient:
    def __init__(self, staging=(), live=(), max_rows=1000):
        self.tables = {"homeowner_requests_staging": [copy.deepcopy(r) for r in staging],
                       "homeowner_requests": [copy.deepcopy(r) for r in live]}
        self.max_rows = max_rows
        self.reads, self.updates, self.inserts, self.rpcs = [], [], [], []

    def table(self, name):
        return Query(self, name)

    def rpc(self, name, args):
        return _Rpc(self, name, args)

    # helpers for assertions
    def row(self, row_id, table="homeowner_requests_staging"):
        return next(r for r in self.tables[table] if r.get("id") == row_id)

    def staging_reads(self):
        return [r for r in self.reads if r["table"] == "homeowner_requests_staging"]

    def written_ids(self, table="homeowner_requests_staging"):
        ids = {i for u in self.updates if u["table"] == table for i in u["ids"]}
        if table == "homeowner_requests_staging":
            ids |= {item["id"] for _, args in self.rpcs for item in args.get("p_rows", [])}
        return sorted(ids)

    def heavy_ids(self, column, table="homeowner_requests_staging"):
        """Ids whose whole `column` object was downloaded."""
        out = set()
        for r in self.reads:
            if r["table"] == table and (r["select"].strip() == "*" or column in _split(r["select"])):
                out.update(r["ids"])
        return sorted(out)
