"""Tables that change between loads: new columns, new tables, nested tables, variants."""

from __future__ import annotations

import dlt
import pytest

from .conftest import MakePipeline, Query

FORMATS = ["parquet", "typed-jsonl"]


def _columns(query: Query, dataset: str, table: str) -> dict[str, str]:
    return {
        name: data_type
        for name, data_type in query(
            "SELECT column_name, data_type FROM duckdb_columns()"
            f" WHERE schema_name = '{dataset}' AND table_name = '{table}'"
            " AND column_name NOT LIKE '\\_dlt%' ESCAPE '\\' ORDER BY column_index"
        )
    }


@pytest.mark.parametrize("file_format", FORMATS)
def test_columns_and_tables_appear_as_the_data_grows(
    pipeline: dlt.Pipeline, query: Query, file_format: str
) -> None:
    dataset = pipeline.dataset_name

    def load(rows: list[dict], table_name: str = "people") -> None:  # type: ignore[type-arg]
        info = pipeline.run(rows, table_name=table_name, loader_file_format=file_format)  # type: ignore[arg-type]
        assert not info.has_failed_jobs

    load([{"id": 1, "name": "Ada"}])
    assert _columns(query=query, dataset=dataset, table="people") == {
        "id": "BIGINT",
        "name": "VARCHAR",
    }

    # A new column: the rows already there read NULL in it.
    load([{"id": 2, "name": "Grace", "born": 1906}])
    assert query(f"SELECT id, born FROM {dataset}.people ORDER BY id") == [(1, None), (2, 1906)]

    # A file without a column the table has: the column takes its default (NULL).
    load([{"id": 3, "born": 1912}])
    assert query(f"SELECT name FROM {dataset}.people WHERE id = 3") == [(None,)]

    # A nested list becomes a child table, and a list inside it a grandchild.
    load([{"id": 4, "name": "Edsger", "papers": [{"year": 1968, "cites": [{"by": "x"}]}]}])
    assert query(
        f"SELECT p.id, c.year, g.by FROM {dataset}.people AS p"
        f" JOIN {dataset}.people__papers AS c ON c._dlt_parent_id = p._dlt_id"
        f" JOIN {dataset}.people__papers__cites AS g ON g._dlt_parent_id = c._dlt_id"
    ) == [(4, 1968, "x")]

    # A value of another type goes to a variant column next to the first.
    load([{"id": 5, "born": "unknown"}])
    assert _columns(query=query, dataset=dataset, table="people") == {
        "id": "BIGINT",
        "name": "VARCHAR",
        "born": "BIGINT",
        "born__v_text": "VARCHAR",
    }
    assert query(f"SELECT born, born__v_text FROM {dataset}.people WHERE id = 5") == [
        (None, "unknown")
    ]

    # A second table in the same dataset.
    load([{"code": "nl"}], table_name="countries")
    assert query(f"SELECT code FROM {dataset}.countries") == [("nl",)]
    assert query(f"SELECT count(*) FROM {dataset}.people") == [(5,)]

    # dlt recorded each schema version it applied.
    versions = query(f"SELECT count(DISTINCT version_hash) FROM {dataset}._dlt_version")
    assert versions[0][0] >= 5


def test_a_unique_column_added_to_a_table_that_exists(
    make_pipeline: MakePipeline, query: Query
) -> None:
    """A unique column that arrives later is added with its constraint."""
    pipeline = make_pipeline(create_unique_indexes=True, create_primary_keys=True)
    dataset = pipeline.dataset_name
    pipeline.run([{"id": 1}], table_name="things", primary_key="id")
    info = pipeline.run(
        [{"id": 2, "code": "b"}],
        table_name="things",
        primary_key="id",
        columns={"code": {"data_type": "text", "unique": True}},
    )
    assert not info.has_failed_jobs
    assert query(f"SELECT id, code FROM {dataset}.things ORDER BY id") == [(1, None), (2, "b")]
    assert query(
        "SELECT constraint_type, constraint_column_names FROM duckdb_constraints()"
        f" WHERE schema_name = '{dataset}' AND table_name = 'things'"
        " AND constraint_type <> 'NOT NULL' ORDER BY 1, 2"
    ) == [("PRIMARY KEY", ["id"]), ("UNIQUE", ["_dlt_id"]), ("UNIQUE", ["code"])]
