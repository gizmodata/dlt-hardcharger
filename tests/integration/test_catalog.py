"""The catalog is the URL's path; dlt's dataset is a schema in it."""

from __future__ import annotations

from collections.abc import Iterator

import dlt
import hardcharger
import pytest

from .conftest import MakePipeline, Query, server_url


@pytest.fixture(scope="module")
def lake(query: Query) -> Iterator[str]:
    """A second catalog, attached next to the server's default one."""
    query("ATTACH ':memory:' AS lake")
    yield "lake"
    query("DETACH lake")


def _where(query: Query, table: str, dataset: str) -> list[tuple[str, str]]:
    return query(  # type: ignore[return-value]
        "SELECT database_name, schema_name FROM duckdb_tables()"
        f" WHERE table_name = '{table}' AND starts_with(schema_name, '{dataset}') ORDER BY 1, 2"
    )


def test_load_into_an_attached_catalog(
    server: hardcharger.Server, lake: str, make_pipeline: MakePipeline, query: Query
) -> None:
    default_catalog = query("SELECT current_database()")[0][0]
    assert default_catalog != lake
    pipeline = make_pipeline(credentials=server_url(server=server, catalog=lake))
    dataset = pipeline.dataset_name

    @dlt.resource(name="visits", primary_key="id", write_disposition="merge")
    def visits(rows: list[dict]):  # type: ignore[no-untyped-def,type-arg]
        yield rows

    pipeline.run(visits([{"id": 1, "pages": [{"p": "a"}]}, {"id": 2, "pages": []}]))
    pipeline.run(visits([{"id": 2, "pages": [{"p": "b"}]}, {"id": 3, "pages": []}]))

    # Everything is in the catalog of the URL: the tables, dlt's own, and the staging dataset.
    assert _where(query=query, table="visits", dataset=dataset) == [(lake, dataset), (lake, f"{dataset}_staging")]
    assert _where(query=query, table="_dlt_pipeline_state", dataset=dataset) == [(lake, dataset)]
    assert query(f"SELECT id FROM {lake}.{dataset}.visits ORDER BY id") == [(1,), (2,), (3,)]
    assert query(f"SELECT p FROM {lake}.{dataset}.visits__pages ORDER BY p") == [("a",), ("b",)]
    assert query(
        "SELECT count(*) FROM duckdb_schemas()"
        f" WHERE database_name = '{default_catalog}' AND schema_name = '{dataset}'"
    ) == [(0,)]

    # Reads go to the same catalog.
    assert pipeline.dataset().visits.select("id").order_by("id").fetchall() == [(1,), (2,), (3,)]
    with pipeline.destination_client() as client:
        assert client.config.data_location() == f"hardcharger://127.0.0.1:{server.port}/{lake}"


def test_the_same_dataset_name_in_two_catalogs_is_two_datasets(
    server: hardcharger.Server, lake: str, make_pipeline: MakePipeline, query: Query, name: str
) -> None:
    in_default = make_pipeline(pipeline_name=f"{name}_a", dataset_name=name)
    in_lake = make_pipeline(
        pipeline_name=f"{name}_b",
        dataset_name=name,
        credentials=server_url(server=server, catalog=lake),
    )
    in_default.run([{"id": 1}], table_name="t")
    in_lake.run([{"id": 2}], table_name="t")
    default_catalog = query("SELECT current_database()")[0][0]
    assert query(f"SELECT id FROM {default_catalog}.{name}.t") == [(1,)]
    assert query(f"SELECT id FROM {lake}.{name}.t") == [(2,)]
    assert in_default.dataset().t.select("id").fetchall() == [(1,)]
    assert in_lake.dataset().t.select("id").fetchall() == [(2,)]


def test_a_catalog_that_is_not_attached_is_reported(
    server: hardcharger.Server, make_pipeline: MakePipeline
) -> None:
    pipeline = make_pipeline(credentials=server_url(server=server, catalog="no_such_catalog"))
    with pytest.raises(Exception) as raised:
        pipeline.run([{"id": 1}], table_name="t")
    assert "no_such_catalog" in str(raised.value)
