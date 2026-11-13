"""Reading what was loaded: pipeline.dataset(), the SQL client, state kept at the destination."""

from __future__ import annotations

from pathlib import Path

import dlt
import pyarrow as pa
import pytest
from dlt.destinations.exceptions import DatabaseTerminalException, DatabaseUndefinedRelation

from .conftest import MakePipeline, Query


@pytest.fixture
def loaded(pipeline: dlt.Pipeline) -> dlt.Pipeline:
    rows = [
        {"id": i, "city": ("Lima", "Pune", "Oslo")[i % 3], "temp": 10.5 + i, "tags": [{"t": i}]}
        for i in range(1, 7)
    ]
    assert not pipeline.run(rows, table_name="readings").has_failed_jobs
    return pipeline


def test_dataset_relations(loaded: dlt.Pipeline) -> None:
    dataset = loaded.dataset()
    readings = dataset.readings
    assert readings.select("id", "city").order_by("id").limit(2).fetchall() == [
        (1, "Pune"),
        (2, "Oslo"),
    ]
    assert readings.where("temp", "gt", 15).select("id").order_by("id").fetchall() == [
        (5,),
        (6,),
    ]
    assert readings.select("temp").max().fetchscalar() == 16.5

    table = readings.select("id", "temp").order_by("id").arrow()
    assert isinstance(table, pa.Table)
    assert table.column("id").to_pylist() == [1, 2, 3, 4, 5, 6]

    counts = {row[0]: row[1] for row in dataset.row_counts().fetchall()}
    assert counts == {"readings": 6, "readings__tags": 6}


def test_dataset_sql_is_duckdb_sql(loaded: dlt.Pipeline) -> None:
    """dlt parses a query with sqlglot's DuckDB dialect before it sends it."""
    relation = loaded.dataset()(
        "SELECT city, list(id ORDER BY id) AS ids, count(*) FILTER (WHERE temp > 13) AS warm"
        " FROM readings GROUP BY ALL ORDER BY city"
    )
    assert relation.fetchall() == [("Lima", [3, 6], 2), ("Oslo", [2, 5], 1), ("Pune", [1, 4], 1)]
    joined = loaded.dataset()(
        "SELECT r.city, t.t FROM readings AS r JOIN readings__tags AS t"
        " ON t._dlt_parent_id = r._dlt_id WHERE r.id = 4"
    )
    assert joined.fetchall() == [("Pune", 4)]


def test_sql_client(loaded: dlt.Pipeline) -> None:
    with loaded.sql_client() as client:
        table = client.make_qualified_table_name("readings")
        assert client.has_dataset()
        assert client.execute_sql(f"SELECT count(*) FROM {table} WHERE id > %s", 4) == [(2,)]
        assert client.execute_sql(f"SELECT city FROM {table} WHERE id = :id", id=3) == [("Lima",)]
        with client.execute_query(f"SELECT id, temp FROM {table} ORDER BY id LIMIT 2") as cursor:
            assert cursor.fetchone() == (1, 11.5)
            assert cursor.df()["id"].tolist() == [2] if _has_pandas() else True

        # A missing table is an undefined relation; a missing column is not (dlt creates what
        # is undefined and must not take a typo for it).
        with pytest.raises(DatabaseUndefinedRelation, match="does not exist"):
            client.execute_sql(f'SELECT * FROM "{loaded.dataset_name}"."no_such_table"')
        with pytest.raises(DatabaseTerminalException, match="Binder Error") as raised:
            client.execute_sql(f"SELECT no_such_column FROM {table}")
        assert not isinstance(raised.value, DatabaseUndefinedRelation)


def _has_pandas() -> bool:
    try:
        import pandas  # noqa: F401
    except ImportError:
        return False
    return True


def test_state_and_schema_come_back_from_the_destination(
    loaded: dlt.Pipeline, make_pipeline: MakePipeline, tmp_path: Path, query: Query
) -> None:
    """A pipeline with no local files (a new machine) restores itself, then carries on."""

    @dlt.resource(name="events", primary_key="id", write_disposition="merge")
    def events(since=dlt.sources.incremental("id")):  # type: ignore[no-untyped-def]
        yield [{"id": i} for i in range(1, 6) if i > (since.last_value or 0)]

    loaded.run(events())
    fresh = make_pipeline(
        pipeline_name=loaded.pipeline_name,
        dataset_name=loaded.dataset_name,
        pipelines_dir=tmp_path / "another-machine",
    )
    assert fresh.first_run
    fresh.sync_destination()
    assert not fresh.first_run
    assert set(fresh.default_schema.data_table_names()) == {
        "readings",
        "readings__tags",
        "events",
    }
    assert fresh.state["destination_type"] == "dlt_hardcharger.destinations.hardcharger"
    # The incremental cursor came back too: nothing is extracted again.
    info = fresh.run(events())
    assert not info.has_failed_jobs
    assert query(f"SELECT count(*) FROM {loaded.dataset_name}.events") == [(5,)]
    assert fresh.last_trace.last_normalize_info.row_counts.get("events", 0) == 0
