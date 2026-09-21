from contextlib import contextmanager
from datetime import date
from decimal import Decimal
from types import SimpleNamespace

import pytest

from lakehouse.publish import MARTS, publish


class Cursor:
    def __init__(self, rows):
        self.rows = rows

    def execute(self, query):
        self.pending = list(self.rows[query.split(".")[-1]])

    def fetchmany(self, _size):
        rows, self.pending = self.pending, []
        return rows


class Serving:
    def __init__(self, fail=False, corrupt=False):
        self.active = "previous_release"
        self.data = {}
        self.fail = fail
        self.corrupt = corrupt
        self.switches = 0

    def command(self, query):
        if query.startswith("ALTER TABLE"):
            self.data[query.split()[2]] = []
        elif query.startswith("INSERT INTO"):
            self.candidate = query.split("'")[1]
        elif query.startswith("EXCHANGE TABLES"):
            self.active = self.candidate
            self.switches += 1

    def insert(self, target, rows, column_names):
        self.data.setdefault(target, []).extend(rows)
        if self.fail:
            self.fail = False
            raise ConnectionError("Simulated failure after partial insert")

    def query(self, _query):
        return SimpleNamespace(result_rows=[(self.active,)])

    @contextmanager
    def query_row_block_stream(self, query):
        table = query.split(" FROM ")[1].split()[0]
        rows = [r[1:] for r in self.data[table]]
        if self.corrupt and rows:
            rows.append(rows[0])
        yield [rows]


@pytest.fixture
def inputs():
    project = SimpleNamespace(serving="qa_analytics", gold="qa_gold")
    rows = {}
    for name, schema in MARTS.items():
        rows[name] = [
            tuple(
                date(2020, 1, 1)
                if kind == "Date"
                else "example"
                if kind == "String"
                else Decimal("10.00")
                if kind.startswith("Decimal")
                else 0.5
                if kind == "Float64"
                else 1
                for _, kind in schema
            )
        ]
    registry = SimpleNamespace(
        require_stage=lambda *args: None, record_candidate=lambda *args: None
    )
    return project, rows, registry


def test_partial_insert_keeps_previous_release_and_retry_is_idempotent(inputs):
    project, rows, registry = inputs
    client = Serving(fail=True)
    key = "a" * 64
    with pytest.raises(ConnectionError):
        publish(project, registry, key, Cursor(rows), client)
    assert client.active == "previous_release"
    publish(project, registry, key, Cursor(rows), client)
    assert client.active == key
    assert all(len(r) == 1 for r in client.data.values())
    publish(project, registry, key, Cursor(rows), client)
    assert client.switches == 1


def test_reconciliation_blocks_publication(inputs):
    project, rows, registry = inputs
    client = Serving(corrupt=True)
    with pytest.raises(RuntimeError, match="reconciliation"):
        publish(project, registry, "a" * 64, Cursor(rows), client)
    assert client.active == "previous_release"
