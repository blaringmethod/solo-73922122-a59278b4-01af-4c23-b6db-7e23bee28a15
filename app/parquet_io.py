"""Parquet serialisation for query results, built from the same fetched
batches that feed the CSV output.

Design notes
------------
* The user SQL is executed exactly once per round. The worker streams
  ``fetchmany`` batches from the cursor; every batch is serialised to CSV
  text *and* appended to an in-memory Parquet writer, so both files always
  describe the identical result (same columns, column order, row order and
  duplicate rows).
* Only the types required by the delivery contract are supported:
  BOOLEAN, INTEGER, BIGINT, DOUBLE, DECIMAL(p,s), DATE, TIMESTAMP and
  VARCHAR. Anything else (HUGEINT, BLOB, INTERVAL, lists, ...) aborts the
  whole round and names the offending column.
* NULL and empty string are distinct values in Parquet; pyarrow preserves
  that distinction natively.
* The Parquet stream is written into an in-memory buffer so the worker can
  enforce the combined 8 MiB budget (CSV bytes + Parquet bytes) before
  anything is published.
"""
from __future__ import annotations

import re
from typing import Sequence

import pyarrow as pa
import pyarrow.parquet as pq

# DuckDB result type name -> Arrow type factory. DECIMAL needs its
# precision/scale parsed out and is handled separately below.
_SIMPLE_TYPE_MAP = {
    "BOOLEAN": pa.bool_,
    "INTEGER": pa.int32,
    "BIGINT": pa.int64,
    "DOUBLE": pa.float64,
    "DATE": pa.date32,
    "VARCHAR": pa.string,
}

# DuckDB TIMESTAMP is microsecond precision; keep it exactly.
_TIMESTAMP_TYPE = pa.timestamp("us")

_DECIMAL_RE = re.compile(r"^DECIMAL\((\d+),\s*(\d+)\)$")

# Largest precision pa.decimal128 can represent (DuckDB DECIMAL maxes at 38).
_MAX_DECIMAL128_PRECISION = 38

SUPPORTED_TYPE_NAMES = (
    "BOOLEAN, INTEGER, BIGINT, DOUBLE, DECIMAL(p,s), DATE, TIMESTAMP, VARCHAR"
)


class ParquetSchemaError(ValueError):
    """Raised when a result set cannot be represented as Parquet.

    The message always names the offending column(s) so the operator can
    fix the query; the whole round fails.
    """


def find_duplicate_columns(columns: Sequence[str]) -> list[str]:
    """Return the column names that appear more than once (order of first
    repeated appearance)."""
    seen: set[str] = set()
    duplicates: list[str] = []
    for name in columns:
        if name in seen and name not in duplicates:
            duplicates.append(name)
        seen.add(name)
    return duplicates


def arrow_type_for(duckdb_type: str, column_name: str) -> pa.DataType:
    """Map one DuckDB result type to an Arrow type.

    Raises :class:`ParquetSchemaError` naming the column when the type is
    outside the supported delivery set.
    """
    # DuckDB renders some types quoted (e.g. "NULL"); normalise first.
    normalised = duckdb_type.strip().strip('"').upper()

    factory = _SIMPLE_TYPE_MAP.get(normalised)
    if factory is not None:
        return factory()
    if normalised == "TIMESTAMP":
        return _TIMESTAMP_TYPE

    match = _DECIMAL_RE.match(normalised)
    if match:
        precision, scale = int(match.group(1)), int(match.group(2))
        if precision > _MAX_DECIMAL128_PRECISION:
            raise ParquetSchemaError(
                f"column '{column_name}' has type DECIMAL({precision},{scale}) "
                f"whose precision exceeds decimal128 "
                f"({_MAX_DECIMAL128_PRECISION} digits); the whole round failed"
            )
        return pa.decimal128(precision, scale)

    raise ParquetSchemaError(
        f"column '{column_name}' has type {normalised or duckdb_type!r} which is "
        f"not supported for Parquet delivery (supported: {SUPPORTED_TYPE_NAMES}); "
        "the whole round failed"
    )


def build_schema(
    columns: Sequence[str], duckdb_types: Sequence[str]
) -> pa.Schema:
    """Build the Arrow schema for a result set.

    Raises :class:`ParquetSchemaError` on duplicate column names or
    unsupported column types, always naming the responsible column(s).
    """
    duplicates = find_duplicate_columns(columns)
    if duplicates:
        quoted = ", ".join(f"'{name}'" for name in duplicates)
        raise ParquetSchemaError(
            f"duplicate column name(s) in result: {quoted}; Parquet requires "
            "unique column names; the whole round failed"
        )
    fields = []
    for name, duckdb_type in zip(columns, duckdb_types):
        fields.append(pa.field(name, arrow_type_for(duckdb_type, name)))
    return pa.schema(fields)


class StreamingParquetBuilder:
    """Incrementally writes fetched row batches to an in-memory Parquet file.

    ``bytes_so_far`` exposes the number of bytes already flushed to the
    buffer so the caller can enforce the combined size budget after every
    batch; ``finish`` returns the complete file (header + row groups +
    footer) and must be called exactly once.
    """

    def __init__(self, schema: pa.Schema) -> None:
        self._schema = schema
        self._sink = pa.BufferOutputStream()
        # Snappy is the de-facto standard for warehouse ingestion.
        self._writer = pq.ParquetWriter(self._sink, schema, compression="snappy")
        self._closed = False

    @property
    def bytes_so_far(self) -> int:
        return self._sink.tell()

    def write_batch(self, rows: Sequence[tuple]) -> None:
        """Append one fetched batch (list of row tuples) to the file."""
        if self._closed:
            raise RuntimeError("parquet builder already finished")
        if not rows:
            return
        columns = list(zip(*rows))
        arrays = [
            pa.array(list(values), type=self._schema[i].type, from_pandas=False)
            for i, values in enumerate(columns)
        ]
        table = pa.Table.from_arrays(arrays, schema=self._schema)
        self._writer.write_table(table)

    def finish(self) -> bytes:
        """Close the file and return the complete Parquet payload."""
        if self._closed:
            raise RuntimeError("parquet builder already finished")
        self._closed = True
        self._writer.close()
        return self._sink.getvalue().to_pybytes()

    def abort(self) -> None:
        """Best-effort cleanup when the round fails mid-stream."""
        if not self._closed:
            self._closed = True
            try:
                self._writer.close()
            except Exception:
                pass


def read_back_for_check(payload: bytes) -> pa.Table:
    """Decode a Parquet payload (used by tests and sanity checks)."""
    return pq.read_table(pa.BufferReader(payload))
