"""The Arrow bulk ingest of Parquet files, and the INSERT path next to it."""

from __future__ import annotations

from typing import Any

import dlt
import pyarrow as pa
import pytest
from adbc_driver_manager import AdbcStatusCode
from adbc_driver_manager import Error as AdbcError
from dlt.destinations.exceptions import (
    DatabaseTerminalException,
    DatabaseTransientException,
    DatabaseUndefinedRelation,
)
from dlt.destinations.impl.sqlalchemy.load_jobs import SqlalchemyJsonLInsertJob

from dlt_hardcharger.job_client import HardchargerArrowIngestJob, ingest_exception

from .conftest import MakePipeline, Query, load_formats


def _job_classes(pipeline: dlt.Pipeline, monkeypatch: pytest.MonkeyPatch) -> dict[str, str]:
    """Record the class of the load job the job client makes for each table."""
    from dlt_hardcharger.job_client import HardchargerJobClient

    made: dict[str, str] = {}
    original = HardchargerJobClient.create_load_job

    def recording(
        self: Any, table: Any, file_path: str, load_id: str, restore: bool = False
    ) -> Any:
        job = original(self, table, file_path, load_id, restore)
        made[table["name"]] = type(job).__name__
        return job

    monkeypatch.setattr(HardchargerJobClient, "create_load_job", recording)
    return made


def test_parquet_is_the_default_and_is_ingested_as_arrow(
    pipeline: dlt.Pipeline, query: Query, monkeypatch: pytest.MonkeyPatch
) -> None:
    made = _job_classes(pipeline=pipeline, monkeypatch=monkeypatch)
    info = pipeline.run([{"id": i, "kids": [{"k": i}]} for i in range(100)], table_name="rows")
    assert load_formats(info=info, table_name="rows") == {"parquet"}
    assert load_formats(info=info, table_name="_dlt_pipeline_state") == {"parquet"}
    assert made["rows"] == made["rows__kids"] == HardchargerArrowIngestJob.__name__
    assert set(made.values()) == {HardchargerArrowIngestJob.__name__}
    assert query(f"SELECT count(*), sum(id) FROM {pipeline.dataset_name}.rows") == [(100, 4950)]


def test_typed_jsonl_files_are_sent_as_inserts(
    pipeline: dlt.Pipeline, query: Query, monkeypatch: pytest.MonkeyPatch
) -> None:
    made = _job_classes(pipeline=pipeline, monkeypatch=monkeypatch)
    info = pipeline.run(
        [{"id": i} for i in range(100)], table_name="rows", loader_file_format="typed-jsonl"
    )
    assert load_formats(info=info, table_name="rows") == {"typed-jsonl"}
    assert made["rows"] == SqlalchemyJsonLInsertJob.__name__
    assert query(f"SELECT count(*), sum(id) FROM {pipeline.dataset_name}.rows") == [(100, 4950)]


def test_a_large_table_arrives_whole(pipeline: dlt.Pipeline, query: Query) -> None:
    """Enough rows for the driver to send the file in several parts at once."""
    rows = 300_000
    table = pa.table(
        {
            "id": pa.array(range(rows), type=pa.int64()),
            "label": pa.array([f"row {i}" for i in range(rows)]),
        }
    )
    info = pipeline.run(table, table_name="big")
    assert not info.has_failed_jobs
    assert query(
        f"SELECT count(*), count(DISTINCT id), sum(id) FROM {pipeline.dataset_name}.big"
    ) == [(rows, rows, rows * (rows - 1) // 2)]


def test_many_files_load_at_once(make_pipeline: MakePipeline, query: Query) -> None:
    """Twenty tables, so dlt's twenty load workers each ingest a file at the same time."""
    pipeline = make_pipeline()

    @dlt.source
    def many() -> Any:
        def make(number: int) -> Any:
            @dlt.resource(name=f"t{number}")
            def rows() -> Any:
                yield pa.table({"id": pa.array(range(50_000), type=pa.int64())})

            return rows

        return [make(number) for number in range(20)]

    info = pipeline.run(many())
    assert not info.has_failed_jobs
    for number in range(20):
        assert query(f"SELECT count(*) FROM {pipeline.dataset_name}.t{number}") == [(50_000,)]


def test_columns_are_matched_by_name_not_position(pipeline: dlt.Pipeline, query: Query) -> None:
    dataset = pipeline.dataset_name
    pipeline.run(pa.table({"a": [1], "b": ["one"], "c": [1.5]}), table_name="t")
    # The same columns in another order, then a file without one of them.
    pipeline.run(pa.table({"c": [2.5], "a": [2], "b": ["two"]}), table_name="t")
    pipeline.run(pa.table({"b": ["three"], "a": [3]}), table_name="t")
    assert query(f"SELECT a, b, c FROM {dataset}.t ORDER BY a") == [
        (1, "one", 1.5),
        (2, "two", 2.5),
        (3, "three", None),
    ]


@pytest.mark.parametrize(
    ("status", "expected"),
    [
        (AdbcStatusCode.INVALID_ARGUMENT, DatabaseTerminalException),
        (AdbcStatusCode.INTEGRITY, DatabaseTerminalException),
        (AdbcStatusCode.UNAUTHORIZED, DatabaseTerminalException),
        (AdbcStatusCode.NOT_FOUND, DatabaseUndefinedRelation),
        (AdbcStatusCode.IO, DatabaseTransientException),
        (AdbcStatusCode.TIMEOUT, DatabaseTransientException),
        (AdbcStatusCode.UNKNOWN, DatabaseTransientException),
    ],
)
def test_ingest_errors_are_classified_by_adbc_status(
    status: AdbcStatusCode, expected: type[Exception]
) -> None:
    error = AdbcError("the driver's message", status_code=status)
    classified = ingest_exception(error=error)
    assert type(classified) is expected
    assert "the driver's message" in str(classified)
