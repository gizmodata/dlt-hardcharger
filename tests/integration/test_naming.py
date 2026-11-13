"""Identifiers: DuckDB keeps their case and compares them without it."""

from __future__ import annotations

import dlt
import pytest
from dlt.common.schema.exceptions import SchemaIdentifierNormalizationCollision
from dlt.pipeline.exceptions import PipelineStepFailed

from .conftest import Query


def _collision(raised: pytest.ExceptionInfo[PipelineStepFailed]) -> str:
    cause: BaseException | None = raised.value
    while cause is not None and not isinstance(cause, SchemaIdentifierNormalizationCollision):
        cause = cause.__cause__
    assert cause is not None, f"no naming collision in {raised.value!r}"
    return str(cause)


def test_default_naming_folds_to_snake_case(pipeline: dlt.Pipeline, query: Query) -> None:
    pipeline.run([{"Id": 1, "FullName": "Ada", "fullName": "ada"}], table_name="People")
    # snake_case naming makes FullName and fullName one column, as on every destination.
    assert query(f"SELECT id, full_name FROM {pipeline.dataset_name}.people") == [(1, "ada")]


def test_case_sensitive_naming_keeps_the_case(
    pipeline: dlt.Pipeline, query: Query, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("SCHEMA__NAMING", "sql_cs_v1")
    dataset = pipeline.dataset_name
    pipeline.run([{"Id": 1, "FullName": "Ada"}], table_name="People")
    pipeline.run([{"Id": 2, "FullName": "Grace", "Born": 1906}], table_name="People")
    assert query(
        "SELECT table_name, column_name FROM duckdb_columns()"
        f" WHERE schema_name = '{dataset}' AND table_name = 'People'"
        " AND column_name NOT LIKE '\\_dlt%' ESCAPE '\\' ORDER BY column_index"
    ) == [("People", "Id"), ("People", "FullName"), ("People", "Born")]
    assert pipeline.dataset()["People"].select("Id", "Born").order_by("Id").fetchall() == [
        (1, None),
        (2, 1906),
    ]


def test_columns_that_differ_only_in_case_are_refused_before_any_ddl(
    pipeline: dlt.Pipeline, query: Query, monkeypatch: pytest.MonkeyPatch
) -> None:
    """With a case-sensitive naming convention, ``Name`` and ``name`` would be one column."""
    monkeypatch.setenv("SCHEMA__NAMING", "sql_cs_v1")
    with pytest.raises(PipelineStepFailed) as raised:
        pipeline.run([{"Id": 1, "Name": "upper", "name": "lower"}], table_name="People")
    message = _collision(raised=raised)
    assert "A `column` name `name` in table `People` collides with `Name`" in message
    assert "Destination is case insensitive" in message
    # dlt stopped before it created anything: no table, no half-loaded data.
    assert query(
        f"SELECT count(*) FROM duckdb_tables() WHERE schema_name = '{pipeline.dataset_name}'"
        " AND table_name = 'People'"
    ) == [(0,)]


def test_tables_that_differ_only_in_case_are_refused(
    pipeline: dlt.Pipeline, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("SCHEMA__NAMING", "sql_cs_v1")
    pipeline.run([{"Id": 1}], table_name="Orders")
    with pytest.raises(PipelineStepFailed) as raised:
        pipeline.run([{"Id": 2}], table_name="orders")
    assert "A `table` name `orders` collides with `Orders`" in _collision(raised=raised)


def test_reserved_words_and_long_names(pipeline: dlt.Pipeline, query: Query) -> None:
    long_column = "c" * 300
    pipeline.run(
        [{"select": 1, "from": "x", "order": [{"group": 1}], long_column: "long"}],
        table_name="table",
    )
    dataset = pipeline.dataset_name
    assert query(f'SELECT "select", "from", "{long_column}" FROM {dataset}."table"') == [
        (1, "x", "long")
    ]
    assert query(f'SELECT "group" FROM {dataset}."table__order"') == [(1,)]
