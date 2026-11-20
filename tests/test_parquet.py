"""End-to-end tests for the optional CSV + Parquet dual delivery.

Covers: single-execution consistency between the two files, column/row
fidelity, the supported type matrix, NULL vs empty string, duplicate column
rejection, unsupported type rejection, the combined 8 MiB budget and the
no-download rule for failed/cancelled rounds.
"""
from __future__ import annotations

import datetime
import io
import time
from decimal import Decimal
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from fastapi.testclient import TestClient

from app.main import app, registry, store
from app.registry import CANCELLED, FAILED, RUNNING, SUCCEEDED

SAMPLE = Path(__file__).resolve().parent.parent / "sample_data" / "observations.csv"


@pytest.fixture()
def client():
    registry._rounds.clear()
    store._dataset = None
    with TestClient(app) as c:
        yield c
    registry.shutdown()
    registry._rounds.clear()
    store._dataset = None


def _upload(client):
    with open(SAMPLE, "rb") as fh:
        resp = client.post(
            "/api/upload",
            files={"file": ("observations.csv", fh, "text/csv")},
        )
    assert resp.status_code == 200, resp.text
    return resp.json()


def _wait_terminal(client, round_id, timeout=15.0):
    deadline = time.monotonic() + timeout
    last = None
    while time.monotonic() < deadline:
        last = client.get(f"/api/rounds/{round_id}").json()
        if last["state"] in (SUCCEEDED, FAILED, CANCELLED):
            return last
        time.sleep(0.05)
    raise AssertionError(f"round did not finish: {last}")


def _submit(client, sql, with_parquet=True, station="ST001"):
    resp = client.post(
        "/api/rounds",
        json={"station_id": station, "sql": sql, "with_parquet": with_parquet},
    )
    assert resp.status_code == 201, resp.text
    return resp.json()["round_id"]


def _read_parquet(payload: bytes) -> pa.Table:
    return pq.read_table(pa.BufferReader(payload))


# --------------------------------------------------------------------- happy path

ALL_TYPES_SQL = """
SELECT
  CAST(temperature_c > 0 AS BOOLEAN) AS b,
  CAST(1 AS INTEGER) AS i,
  CAST(9999999999 AS BIGINT) AS bi,
  CAST(1.5 AS DOUBLE) AS d,
  CAST(12.34 AS DECIMAL(10,2)) AS dec,
  DATE '2024-01-02' AS dt,
  TIMESTAMP '2024-01-02 03:04:05.123456' AS ts,
  station_id AS v,
  CAST(NULL AS VARCHAR) AS nul,
  '' AS empty_s
FROM observations
ORDER BY ts_utc
"""


def test_parquet_round_success_type_matrix(client):
    _upload(client)
    round_id = _submit(client, ALL_TYPES_SQL)
    final = _wait_terminal(client, round_id)
    assert final["state"] == SUCCEEDED, final
    assert final["with_parquet"] is True
    assert final["row_count"] == 5  # ST001 has 5 rows
    assert final["parquet_bytes"] > 0
    assert final["total_bytes"] == final["result_bytes"] + final["parquet_bytes"]
    assert final["download_url"].endswith("/result")
    assert final["download_url_parquet"].endswith("/result.parquet")

    dl_csv = client.get(f"/api/rounds/{round_id}/result")
    dl_pq = client.get(f"/api/rounds/{round_id}/result.parquet")
    assert dl_csv.status_code == 200
    assert dl_pq.status_code == 200
    assert "result_" in dl_pq.headers["content-disposition"]
    assert dl_pq.headers["content-disposition"].endswith('.parquet"')

    table = _read_parquet(dl_pq.content)
    # Column names and order are identical to the CSV header.
    csv_header = dl_csv.text.splitlines()[0]
    assert ",".join(table.column_names) == csv_header
    assert table.column_names == [
        "b", "i", "bi", "d", "dec", "dt", "ts", "v", "nul", "empty_s",
    ]
    # Type + precision preservation.
    schema = table.schema
    assert schema.field("b").type == pa.bool_()
    assert schema.field("i").type == pa.int32()
    assert schema.field("bi").type == pa.int64()
    assert schema.field("d").type == pa.float64()
    assert schema.field("dec").type == pa.decimal128(10, 2)
    assert schema.field("dt").type == pa.date32()
    assert schema.field("ts").type == pa.timestamp("us")
    assert schema.field("v").type == pa.string()
    assert schema.field("nul").type == pa.string()

    rows = table.to_pylist()
    assert len(rows) == 5
    assert rows[0]["dec"] == Decimal("12.34")
    assert rows[0]["dt"] == datetime.date(2024, 1, 2)
    assert rows[0]["ts"] == datetime.datetime(2024, 1, 2, 3, 4, 5, 123456)
    # NULL and empty string stay distinct.
    assert all(r["nul"] is None for r in rows)
    assert all(r["empty_s"] == "" for r in rows)


def test_csv_and_parquet_come_from_same_single_execution(client):
    """random() differs between executions: if the SQL ran twice (or the
    files were built from different streams) the values would not match."""
    _upload(client)
    sql = (
        "SELECT random() AS r, ts_utc FROM observations "
        "ORDER BY ts_utc"
    )
    round_id = _submit(client, sql)
    final = _wait_terminal(client, round_id)
    assert final["state"] == SUCCEEDED, final

    csv_lines = client.get(f"/api/rounds/{round_id}/result").text.strip().splitlines()
    assert csv_lines[0] == "r,ts_utc"
    table = _read_parquet(client.get(f"/api/rounds/{round_id}/result.parquet").content)
    pq_rows = table.to_pylist()
    assert len(pq_rows) == len(csv_lines) - 1
    for line, row in zip(csv_lines[1:], pq_rows):
        csv_r, csv_ts = line.split(",", 1)
        # CSV renders str(float); the Parquet value is the very same double.
        assert csv_r == str(row["r"])
        assert csv_ts == row["ts_utc"]


def test_row_order_and_duplicate_rows_preserved(client):
    _upload(client)
    # UNION ALL keeps duplicates; ORDER BY fixes the row order.
    sql = (
        "SELECT ts_utc, temperature_c FROM observations "
        "UNION ALL "
        "SELECT ts_utc, temperature_c FROM observations "
        "ORDER BY ts_utc"
    )
    round_id = _submit(client, sql)
    final = _wait_terminal(client, round_id)
    assert final["state"] == SUCCEEDED, final
    assert final["row_count"] == 10  # 5 rows duplicated

    csv_rows = client.get(f"/api/rounds/{round_id}/result").text.strip().splitlines()[1:]
    table = _read_parquet(client.get(f"/api/rounds/{round_id}/result.parquet").content)
    pq_rows = [(r["ts_utc"], str(r["temperature_c"])) for r in table.to_pylist()]
    csv_pairs = [tuple(line.rsplit(",", 1)) for line in csv_rows]
    assert csv_pairs == pq_rows
    # Duplicates really exist.
    assert len(set(csv_pairs)) < len(csv_pairs)


def test_empty_result_set_still_delivers_both_files(client):
    _upload(client)
    round_id = _submit(client, "SELECT station_id FROM observations WHERE 1 = 0")
    final = _wait_terminal(client, round_id)
    assert final["state"] == SUCCEEDED, final
    assert final["row_count"] == 0
    dl_csv = client.get(f"/api/rounds/{round_id}/result")
    assert dl_csv.text == "station_id\n"
    table = _read_parquet(client.get(f"/api/rounds/{round_id}/result.parquet").content)
    assert table.column_names == ["station_id"]
    assert table.num_rows == 0


# ------------------------------------------------------------- schema violations

def test_duplicate_column_names_fail_and_name_the_column(client):
    _upload(client)
    round_id = _submit(client, "SELECT 1 AS dup, 2 AS other, 3 AS dup")
    final = _wait_terminal(client, round_id)
    assert final["state"] == FAILED
    assert "'dup'" in final["error"]
    assert "duplicate column" in final["error"].lower()
    # Nothing is downloadable.
    assert client.get(f"/api/rounds/{round_id}/result").status_code == 409
    assert client.get(f"/api/rounds/{round_id}/result.parquet").status_code == 409


def test_duplicate_columns_allowed_for_plain_csv_round(client):
    """CSV-only rounds keep the previous lenient behaviour."""
    _upload(client)
    round_id = _submit(client, "SELECT 1 AS dup, 2 AS dup", with_parquet=False)
    final = _wait_terminal(client, round_id)
    assert final["state"] == SUCCEEDED, final
    dl = client.get(f"/api/rounds/{round_id}/result")
    assert dl.status_code == 200
    assert dl.text.splitlines()[0] == "dup,dup"


@pytest.mark.parametrize(
    "expression,duck_type",
    [
        ("CAST(1 AS HUGEINT)", "HUGEINT"),
        ("CAST(1 AS SMALLINT)", "SMALLINT"),
        ("CAST('0101' AS BLOB)", "BLOB"),
        ("INTERVAL '1 day'", "INTERVAL"),
        ("CAST(1.5 AS REAL)", "FLOAT"),
        ("TIME '01:02:03'", "TIME"),
        ("TIMESTAMP WITH TIME ZONE '2024-01-02 03:04:05+00'", "TIMESTAMP WITH TIME ZONE"),
        ("NULL", "NULL"),
    ],
)
def test_unsupported_types_fail_and_name_column(client, expression, duck_type):
    _upload(client)
    sql = f"SELECT station_id, {expression} AS bad_col FROM observations"
    round_id = _submit(client, sql)
    final = _wait_terminal(client, round_id)
    assert final["state"] == FAILED
    assert "bad_col" in final["error"]
    assert duck_type in final["error"]
    assert "not supported" in final["error"]
    assert client.get(f"/api/rounds/{round_id}/result").status_code == 409
    assert client.get(f"/api/rounds/{round_id}/result.parquet").status_code == 409


# ------------------------------------------------------------------ size budget

def test_combined_size_limit_fails_round(client, monkeypatch):
    """CSV alone would fit, but CSV + Parquet together exceed the budget."""
    import app.worker as worker_mod

    monkeypatch.setattr(worker_mod, "MAX_RESULT_BYTES", 64 * 1024)
    _upload(client)
    # md5 strings do not compress: CSV ~46 KiB (< 64 KiB) but the Parquet
    # file is a similar size, so the combined budget is exceeded.
    sql = (
        "WITH RECURSIVE seq(x) AS (SELECT 1 UNION ALL SELECT x+1 FROM seq "
        "WHERE x < 1200) SELECT x, md5(CAST(x AS VARCHAR)) AS h FROM seq"
    )
    round_id = _submit(client, sql)
    final = _wait_terminal(client, round_id)
    assert final["state"] == FAILED
    assert "8 MiB" in final["error"]
    assert "combined" in final["error"].lower()
    assert client.get(f"/api/rounds/{round_id}/result").status_code == 409
    assert client.get(f"/api/rounds/{round_id}/result.parquet").status_code == 409


def test_csv_only_round_unchanged_limit(client, monkeypatch):
    """Plain CSV rounds keep the original 'CSV bytes only' budget."""
    import app.worker as worker_mod

    monkeypatch.setattr(worker_mod, "MAX_RESULT_BYTES", 64 * 1024)
    _upload(client)
    # Same data as the combined-limit test: CSV alone fits the budget.
    sql = (
        "WITH RECURSIVE seq(x) AS (SELECT 1 UNION ALL SELECT x+1 FROM seq "
        "WHERE x < 1200) SELECT x, md5(CAST(x AS VARCHAR)) AS h FROM seq"
    )
    round_id = _submit(client, sql, with_parquet=False)
    final = _wait_terminal(client, round_id)
    assert final["state"] == SUCCEEDED, final
    assert final["result_bytes"] < 64 * 1024


# ------------------------------------------------------- no download on failure

def test_cancelled_parquet_round_offers_no_downloads(client):
    _upload(client)
    infinite_sql = (
        "WITH RECURSIVE seq(x) AS (SELECT 1 UNION ALL SELECT x+1 FROM seq) "
        "SELECT count(*) FROM seq"
    )
    round_id = _submit(client, infinite_sql)
    deadline = time.monotonic() + 3
    while time.monotonic() < deadline:
        if client.get(f"/api/rounds/{round_id}").json()["state"] == RUNNING:
            break
        time.sleep(0.02)
    cancel = client.post(f"/api/rounds/{round_id}/cancel")
    assert cancel.status_code == 200
    final = _wait_terminal(client, round_id)
    assert final["state"] == CANCELLED
    assert client.get(f"/api/rounds/{round_id}/result").status_code == 409
    assert client.get(f"/api/rounds/{round_id}/result.parquet").status_code == 409


def test_timed_out_parquet_round_offers_no_downloads(client):
    _upload(client)
    infinite_sql = (
        "WITH RECURSIVE seq(x) AS (SELECT 1 UNION ALL SELECT x+1 FROM seq) "
        "SELECT count(*) FROM seq"
    )
    round_id = _submit(client, infinite_sql)
    final = _wait_terminal(client, round_id, timeout=20.0)
    assert final["state"] == FAILED
    assert "terminated" in final["error"]
    assert client.get(f"/api/rounds/{round_id}/result").status_code == 409
    assert client.get(f"/api/rounds/{round_id}/result.parquet").status_code == 409


def test_csv_only_round_has_no_parquet_download(client):
    _upload(client)
    round_id = _submit(client, "SELECT * FROM observations", with_parquet=False)
    final = _wait_terminal(client, round_id)
    assert final["state"] == SUCCEEDED
    assert final["with_parquet"] is False
    assert "download_url_parquet" not in final
    resp = client.get(f"/api/rounds/{round_id}/result.parquet")
    assert resp.status_code == 404


def test_default_submission_is_csv_only(client):
    """Omitting with_parquet keeps the legacy behaviour (backwards compat)."""
    _upload(client)
    resp = client.post(
        "/api/rounds",
        json={"station_id": "ST001", "sql": "SELECT count(*) AS n FROM observations"},
    )
    assert resp.status_code == 201
    round_id = resp.json()["round_id"]
    assert resp.json()["with_parquet"] is False
    final = _wait_terminal(client, round_id)
    assert final["state"] == SUCCEEDED
    assert client.get(f"/api/rounds/{round_id}/result.parquet").status_code == 404


# ------------------------------------------------------------------ unit checks

def test_arrow_type_mapping_unit():
    from app.parquet_io import ParquetSchemaError, arrow_type_for, build_schema

    assert arrow_type_for("BOOLEAN", "c") == pa.bool_()
    assert arrow_type_for("INTEGER", "c") == pa.int32()
    assert arrow_type_for("BIGINT", "c") == pa.int64()
    assert arrow_type_for("DOUBLE", "c") == pa.float64()
    assert arrow_type_for("DECIMAL(18,4)", "c") == pa.decimal128(18, 4)
    assert arrow_type_for("DATE", "c") == pa.date32()
    assert arrow_type_for("TIMESTAMP", "c") == pa.timestamp("us")
    assert arrow_type_for("VARCHAR", "c") == pa.string()

    with pytest.raises(ParquetSchemaError, match="mycol"):
        arrow_type_for("HUGEINT", "mycol")
    with pytest.raises(ParquetSchemaError, match="'a'"):
        build_schema(["x", "a", "a"], ["INTEGER", "INTEGER", "VARCHAR"])
