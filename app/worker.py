"""Sandboxed query worker executed in a dedicated thread per round.

Isolation model
---------------
* Every round gets its own thread, its own in-memory DuckDB connection and
  its own copy of the selected station's rows. The only relation visible to
  user SQL is the ``observations`` table of that connection; connections of
  other rounds are unreachable, so cross-round data access is impossible.
* Every connection is created with ``enable_external_access=false`` which
  disables file system, network and extension access inside DuckDB.
* The static SQL guard (``app.sql_guard``) rejects anything but a single
  read-only SELECT/WITH statement before execution.

Cancellation / timeout
----------------------
DuckDB's native ``connection.interrupt()`` is used to abort a running query
immediately; the worker surfaces it as an ordinary failure and no partial
output is ever published. A watchdog additionally interrupts at the 5 s
deadline so a slow query cannot block anything else.

Dual CSV + Parquet output
-------------------------
When a round requests Parquet delivery, the query still executes exactly
once: the worker streams batches from a single cursor and serialises each
batch to CSV text and to an in-memory Parquet writer. The round succeeds
only if both files are complete and their combined size stays within the
8 MiB budget; otherwise nothing is published.
"""
from __future__ import annotations

import threading
from dataclasses import dataclass
from typing import Any, Optional

import duckdb

from .config import (
    BATCH_SIZE,
    MAX_RESULT_BYTES,
    TABLE_COLUMNS,
    TABLE_NAME,
)
from .sql_guard import SqlRejected, validate_sql

# Outcome markers used by the supervisor.
MSG_DONE = "done"
MSG_ERROR = "error"
MSG_TOO_LARGE = "too_large"


@dataclass
class WorkerResult:
    kind: str
    csv_bytes: bytes = b""
    parquet_bytes: bytes = b""
    row_count: int = 0
    error: str = ""
    result_bytes: int = 0
    parquet_result_bytes: int = 0


def _locked_connection() -> duckdb.DuckDBPyConnection:
    # enable_external_access=false blocks file system, network and extension
    # access inside DuckDB itself (second layer behind the static SQL guard).
    return duckdb.connect(
        ":memory:",
        config={
            "enable_external_access": False,
            "allow_unsigned_extensions": False,
            "autoinstall_known_extensions": False,
            "autoload_known_extensions": False,
        },
    )


def _build_relation(con: duckdb.DuckDBPyConnection, columns: dict[str, list]) -> None:
    con.execute(
        f"""
        CREATE TABLE {TABLE_NAME} (
            {TABLE_COLUMNS[0]} VARCHAR,
            {TABLE_COLUMNS[1]} VARCHAR,
            {TABLE_COLUMNS[2]} DOUBLE
        )
        """
    )
    rows = list(
        zip(
            columns[TABLE_COLUMNS[0]],
            columns[TABLE_COLUMNS[1]],
            columns[TABLE_COLUMNS[2]],
        )
    )
    con.executemany(
        f"INSERT INTO {TABLE_NAME} VALUES (?, ?, ?)", rows
    )


def _csv_quote(value: Any) -> str:
    if value is None:
        return ""
    text = str(value)
    if any(ch in text for ch in (",", '"', "\n", "\r")):
        text = '"' + text.replace('"', '""') + '"'
    return text


def _csv_header(columns: list[str]) -> bytes:
    return (",".join(_csv_quote(c) for c in columns) + "\n").encode("utf-8")


def _format_csv(columns: list[str], rows: list[tuple]) -> bytes:
    out = []
    for row in rows:
        out.append(",".join(_csv_quote(v) for v in row))
    return ("\n".join(out) + ("\n" if rows else "")).encode("utf-8")


class QueryWorker(threading.Thread):
    """Runs one validated query to completion or interruption."""

    def __init__(
        self,
        round_id: str,
        sql: str,
        columns: dict[str, list],
        with_parquet: bool = False,
    ) -> None:
        super().__init__(name=f"query-{round_id[:8]}", daemon=True)
        self.round_id = round_id
        self._sql = sql
        self._columns = columns
        self._with_parquet = with_parquet
        self._con: Optional[duckdb.DuckDBPyConnection] = None
        self._interrupted = threading.Event()
        self._lock = threading.Lock()
        self.result: Optional[WorkerResult] = None

    # ---------------------------------------------------------- cancellation
    def interrupt(self, reason: str) -> None:
        """Ask DuckDB to abort the running query (safe from another thread)."""
        self._interrupted.set()
        with self._lock:
            if self._con is not None:
                try:
                    self._con.interrupt()
                except Exception:
                    # The connection may already be closed; nothing to do.
                    pass

    def is_interrupted(self) -> bool:
        return self._interrupted.is_set()

    # ------------------------------------------------------------------ main
    def run(self) -> None:
        try:
            try:
                validated = validate_sql(self._sql)
            except SqlRejected as exc:
                self.result = WorkerResult(
                    MSG_ERROR, error=f"SQL rejected: {exc}"
                )
                return

            con = _locked_connection()
            with self._lock:
                # If cancellation arrived before the connection existed, stop now.
                if self._interrupted.is_set():
                    con.close()
                    self.result = WorkerResult(
                        MSG_ERROR, error="cancelled before execution started"
                    )
                    return
                self._con = con

            try:
                self._execute(validated.text, con)
            finally:
                with self._lock:
                    self._con = None
                try:
                    con.close()
                except Exception:
                    pass
        except Exception as exc:  # noqa: BLE001 - report every failure textually
            if self._interrupted.is_set():
                self.result = WorkerResult(
                    MSG_ERROR, error="query was interrupted"
                )
            else:
                self.result = WorkerResult(
                    MSG_ERROR,
                    error=f"{type(exc).__name__}: {str(exc)[:300]}",
                )

    def _execute(self, sql: str, con: duckdb.DuckDBPyConnection) -> None:
        _build_relation(con, self._columns)

        cur = con.execute(sql)
        description = cur.description or []
        result_columns = [d[0] for d in description]
        if not result_columns:
            self.result = WorkerResult(
                MSG_ERROR, error="query did not return a result set"
            )
            return

        # When Parquet delivery is requested, both files are serialised from
        # this one cursor stream: the SQL runs exactly once and CSV/Parquet
        # share columns, column order, row order and duplicate rows.
        parquet_builder = None
        if self._with_parquet:
            # Imported lazily so plain CSV rounds never touch pyarrow.
            from .parquet_io import (
                ParquetSchemaError,
                StreamingParquetBuilder,
                build_schema,
            )

            duckdb_types = [str(d[1]) for d in description]
            try:
                schema = build_schema(result_columns, duckdb_types)
            except ParquetSchemaError as exc:
                # Duplicate column names / unsupported types: the message
                # names the responsible column(s); the whole round fails.
                self.result = WorkerResult(MSG_ERROR, error=str(exc))
                return
            parquet_builder = StreamingParquetBuilder(schema)

        chunks: list[bytes] = [_csv_header(result_columns)]
        csv_total = len(chunks[0])
        total_rows = 0

        try:
            while True:
                if self._interrupted.is_set():
                    if parquet_builder is not None:
                        parquet_builder.abort()
                    self.result = WorkerResult(
                        MSG_ERROR, error="query was interrupted; no partial result"
                    )
                    return
                batch = cur.fetchmany(BATCH_SIZE)
                if not batch:
                    break
                chunk = _format_csv(result_columns, batch)
                csv_total += len(chunk)
                total_rows += len(batch)
                if parquet_builder is not None:
                    parquet_builder.write_batch(batch)
                    combined = csv_total + parquet_builder.bytes_so_far
                    if combined > MAX_RESULT_BYTES:
                        # Whole round fails; accumulated output is discarded.
                        parquet_builder.abort()
                        self.result = WorkerResult(
                            MSG_TOO_LARGE,
                            error=(
                                f"combined CSV+Parquet size exceeds "
                                f"{MAX_RESULT_BYTES} bytes (8 MiB); the whole "
                                "round failed and no files are kept"
                            ),
                            result_bytes=combined,
                        )
                        return
                elif csv_total > MAX_RESULT_BYTES:
                    # Whole round fails; accumulated chunks are discarded.
                    self.result = WorkerResult(
                        MSG_TOO_LARGE,
                        error=(
                            f"result CSV exceeds {MAX_RESULT_BYTES} bytes "
                            "(8 MiB); the whole round failed and no result is kept"
                        ),
                        result_bytes=csv_total,
                    )
                    return
                chunks.append(chunk)
        except Exception:
            if parquet_builder is not None:
                parquet_builder.abort()
            raise

        csv_payload = b"".join(chunks)
        if parquet_builder is not None:
            parquet_payload = parquet_builder.finish()
            combined = csv_total + len(parquet_payload)
            if combined > MAX_RESULT_BYTES:
                self.result = WorkerResult(
                    MSG_TOO_LARGE,
                    error=(
                        f"combined CSV+Parquet size exceeds "
                        f"{MAX_RESULT_BYTES} bytes (8 MiB); the whole round "
                        "failed and no files are kept"
                    ),
                    result_bytes=combined,
                )
                return
            self.result = WorkerResult(
                MSG_DONE,
                csv_bytes=csv_payload,
                parquet_bytes=parquet_payload,
                row_count=total_rows,
                result_bytes=csv_total,
                parquet_result_bytes=len(parquet_payload),
            )
            return

        self.result = WorkerResult(
            MSG_DONE,
            csv_bytes=csv_payload,
            row_count=total_rows,
            result_bytes=csv_total,
        )
