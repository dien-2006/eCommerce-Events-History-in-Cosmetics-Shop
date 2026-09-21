"""Publish validated immutable releases; a single atomic pointer switches all marts."""

import hashlib
import json
from datetime import date, datetime
from decimal import Decimal

from lakehouse.config import digest

MARTS = {
    "daily_sales_by_category": [
        ("event_date", "Date"),
        ("category_code", "String"),
        ("brand", "String"),
        ("revenue", "Decimal(20, 2)"),
        ("purchase_events", "UInt64"),
        ("purchasing_sessions", "UInt64"),
        ("avg_purchase_event_value", "Decimal(20, 4)"),
    ],
    "funnel_steps_daily": [
        ("event_date", "Date"),
        ("sessions_total", "UInt64"),
        ("sessions_view", "UInt64"),
        ("sessions_cart_after_view", "UInt64"),
        ("sessions_purchase_after_cart", "UInt64"),
        ("conv_view_to_cart", "Float64"),
        ("conv_cart_to_purchase", "Float64"),
        ("conv_view_to_purchase", "Float64"),
    ],
    "rfm_segments": [
        ("snapshot_date", "Date"),
        ("user_id", "UInt64"),
        ("recency_days", "Int32"),
        ("frequency", "UInt64"),
        ("monetary", "Decimal(20, 2)"),
        ("r_score", "Int32"),
        ("f_score", "Int32"),
        ("m_score", "Int32"),
        ("segment", "String"),
    ],
}


def canonical(value):
    if isinstance(value, date | datetime):
        return value.isoformat()
    if isinstance(value, Decimal):
        return format(value.normalize(), "f")
    if isinstance(value, float):
        return format(value, ".12g")
    return value


def typed_rows(rows, schema):
    """PyHive returns DATE as ISO strings; ClickHouse's binary writer requires date objects."""
    result = []
    for row in rows:
        values = []
        for value, (_, kind) in zip(row, schema, strict=True):
            if kind == "Date" and isinstance(value, str):
                value = date.fromisoformat(value)
            elif kind.startswith("Decimal") and not isinstance(value, Decimal):
                value = Decimal(str(value))
            values.append(value)
        result.append(values)
    return result


class Fingerprint:
    """Order-independent row multiset digest. Counts detect duplicate or missing writes."""

    def __init__(self):
        self.rows = 0
        self.total = 0

    def add(self, rows):
        for row in rows:
            payload = json.dumps([canonical(v) for v in row], separators=(",", ":"))
            self.total = (self.total + int(hashlib.sha256(payload.encode()).hexdigest(), 16)) % (
                1 << 256
            )
            self.rows += 1

    def value(self):
        return {"rows": self.rows, "sha256_sum": f"{self.total:064x}"}


def initialize_serving(project, client):
    database = project.serving
    client.command(f"CREATE DATABASE IF NOT EXISTS {database} ENGINE=Atomic")
    client.command(f"""CREATE TABLE IF NOT EXISTS {database}.current_release (
      release_id String, published_at DateTime64(3, 'UTC')
    ) ENGINE=MergeTree ORDER BY tuple()""")
    for name, schema in MARTS.items():
        definitions = ", ".join(f"{c} {t}" for c, t in schema)
        columns = ", ".join(c for c, _ in schema)
        sort = {
            "daily_sales_by_category": "event_date, category_code, brand",
            "funnel_steps_daily": "event_date",
            "rfm_segments": "snapshot_date, user_id",
        }[name]
        client.command(f"""CREATE TABLE IF NOT EXISTS {database}._versions_{name} (
          _release_id String, {definitions}
        ) ENGINE=MergeTree PARTITION BY _release_id ORDER BY ({sort})""")
        client.command(f"""CREATE VIEW IF NOT EXISTS {database}.{name} AS
          SELECT {columns} FROM {database}._versions_{name}
          WHERE _release_id=(SELECT release_id FROM {database}.current_release LIMIT 1)""")


def current_release(project, client):
    result = client.query(
        f"SELECT release_id FROM {project.serving}.current_release LIMIT 1"
    ).result_rows
    return result[0][0] if result else None


def publish(project, registry, key, cursor, client):
    registry.require_stage(key, "transform")
    digest(key)
    initialize_serving(project, client)
    if current_release(project, client) == key:
        # The pointer commit succeeded but its PostgreSQL checkpoint was interrupted.
        return {"release_id": key, "already_published": True}
    results = {}
    for name, schema in MARTS.items():
        target = f"{project.serving}._versions_{name}"
        columns = [c for c, _ in schema]
        # Only the unpublished candidate is replaced. The active release stays readable.
        client.command(f"ALTER TABLE {target} DROP PARTITION '{key}'")
        cursor.execute(f"SELECT {', '.join(columns)} FROM {project.gold}.{name}")
        expected = Fingerprint()
        while rows := cursor.fetchmany(10000):
            rows = typed_rows(rows, schema)
            expected.add(rows)
            client.insert(
                target, [[key, *row] for row in rows], column_names=["_release_id", *columns]
            )
        actual = Fingerprint()
        with client.query_row_block_stream(
            f"SELECT {', '.join(columns)} FROM {target} WHERE _release_id='{key}'"
        ) as stream:
            for block in stream:
                actual.add(block)
        if expected.value() != actual.value():
            raise RuntimeError(f"Publication reconciliation failed for {name}")
        results[name] = actual.value()
    registry.record_candidate(key, results)
    pointer = f"{project.serving}._candidate_{key[:16]}"
    client.command(f"CREATE TABLE IF NOT EXISTS {pointer} AS {project.serving}.current_release")
    client.command(f"TRUNCATE TABLE {pointer}")
    client.command(f"INSERT INTO {pointer} SELECT '{key}', now64(3)")
    # Atomic database EXCHANGE swaps the pointer, not three independent serving tables.
    client.command(f"EXCHANGE TABLES {project.serving}.current_release AND {pointer}")
    # Keep the old pointer for investigation; cleanup is an explicit retention operation.
    return {"release_id": key, "tables": results, "already_published": False}
