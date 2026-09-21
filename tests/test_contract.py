import hashlib
import io
from datetime import date
from decimal import Decimal

import pytest

from lakehouse.config import RAW_COLUMNS, as_of_date, identifier, sql_string, validate_header
from lakehouse.landing import read_header, spool_source
from lakehouse.publish import Fingerprint, typed_rows
from lakehouse.registry import run_key


@pytest.mark.parametrize("value", ["x;drop table t", "../data", "a.b", "a'", "UPPER", "", "a" * 49])
def test_reject_untrusted_identifier(value):
    with pytest.raises(ValueError):
        identifier(value)


@pytest.mark.parametrize("value", ["2026-09-11' or 1=1", "2026-02-30", "20260911"])
def test_reject_untrusted_date(value):
    with pytest.raises(ValueError):
        as_of_date(value)


def test_contract_handles_bom_and_reordered_columns():
    header = list(reversed(RAW_COLUMNS)) + ["payment_method"]
    stream = io.BytesIO(("\ufeff" + ",".join(header) + "\n").encode())
    assert read_header(stream) == header
    assert stream.tell() == 0


def test_unknown_and_duplicate_columns_fail_loudly():
    with pytest.raises(ValueError):
        validate_header([*RAW_COLUMNS, "order_id"])
    with pytest.raises(ValueError):
        validate_header([*RAW_COLUMNS, "price"])


def test_source_snapshot_is_independent_of_later_file_changes(tmp_path):
    path = tmp_path / "input.csv"
    original = b"exact raw bytes\r\n"
    path.write_bytes(original)
    spool, sha, size = spool_source(path)
    path.write_bytes(b"new data")
    with spool:
        assert spool.read() == original
    assert sha == hashlib.sha256(original).hexdigest()
    assert size == len(original)


def test_file_and_project_identity():
    assert run_key("cosmetics", "manual__one") == run_key("cosmetics", "manual__one")
    assert run_key("cosmetics", "manual__one") != run_key("other", "manual__one")


def test_fingerprint_detects_loss_and_duplicates_independent_of_order():
    rows = [(date(2020, 1, 1), Decimal("10.20")), (date(2020, 1, 2), Decimal("5.00"))]
    first, reordered, duplicate = Fingerprint(), Fingerprint(), Fingerprint()
    first.add(rows)
    reordered.add(list(reversed(rows)))
    duplicate.add([*rows, rows[0]])
    assert first.value() == reordered.value()
    assert first.value() != duplicate.value()


def test_spark_literal_escaping():
    assert sql_string("a'b\\c") == "'a''b\\\\c'"


def test_thrift_date_is_converted_for_clickhouse_binary_insert():
    rows = typed_rows([("2020-01-01", "10.20")], [("day", "Date"), ("value", "Decimal(20, 2)")])
    assert rows == [[date(2020, 1, 1), Decimal("10.20")]]
