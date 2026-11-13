"""Dropping: refresh modes, the drop helper, the `dlt pipeline drop` command, the whole dataset."""

from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path
from typing import Any

import dlt
import pytest
from dlt.pipeline.helpers import pipeline_drop

from .conftest import Query


@dlt.source
def shop(orders_from: int = 1) -> Any:
    @dlt.resource(primary_key="id", write_disposition="merge")
    def orders(since=dlt.sources.incremental("id")):  # type: ignore[no-untyped-def]
        yield [
            {"id": i, "lines": [{"sku": f"s{i}"}]}
            for i in range(orders_from, orders_from + 3)
            if i > (since.last_value or 0)
        ]

    @dlt.resource(write_disposition="replace")
    def settings() -> Any:
        yield [{"key": "currency", "value": "EUR"}]

    return orders, settings


def _tables(query: Query, dataset: str) -> set[str]:
    return {
        name
        for (name,) in query(
            f"SELECT table_name FROM duckdb_tables() WHERE schema_name = '{dataset}'"
        )
        if not name.startswith("_dlt")
    }


@pytest.fixture
def loaded(pipeline: dlt.Pipeline, query: Query) -> dlt.Pipeline:
    assert not pipeline.run(shop()).has_failed_jobs
    assert _tables(query=query, dataset=pipeline.dataset_name) == {
        "orders",
        "orders__lines",
        "settings",
    }
    return pipeline


def test_refresh_drop_sources(loaded: dlt.Pipeline, query: Query) -> None:
    dataset = loaded.dataset_name
    loaded.run(shop(orders_from=10), refresh="drop_sources")
    # Tables and state went; the run loaded from scratch.
    assert query(f"SELECT id FROM {dataset}.orders ORDER BY id") == [(10,), (11,), (12,)]
    assert query(f"SELECT count(*) FROM {dataset}.orders__lines") == [(3,)]


def test_refresh_drop_resources_drops_only_the_selected(loaded: dlt.Pipeline, query: Query) -> None:
    dataset = loaded.dataset_name
    loaded.run(shop(orders_from=10).with_resources("orders"), refresh="drop_resources")
    assert query(f"SELECT id FROM {dataset}.orders ORDER BY id") == [(10,), (11,), (12,)]
    assert query(f"SELECT value FROM {dataset}.settings") == [("EUR",)]


def test_refresh_drop_data_truncates_and_keeps_the_tables(
    loaded: dlt.Pipeline, query: Query
) -> None:
    dataset = loaded.dataset_name
    loaded.run([{"id": 99, "note": "kept column"}], table_name="orders")
    loaded.run(shop(orders_from=20).with_resources("orders"), refresh="drop_data")
    assert query(f"SELECT id FROM {dataset}.orders ORDER BY id") == [(20,), (21,), (22,)]
    columns = {
        name
        for (name,) in query(
            "SELECT column_name FROM duckdb_columns()"
            f" WHERE schema_name = '{dataset}' AND table_name = 'orders'"
        )
    }
    assert "note" in columns


def test_drop_helper_drops_a_resource_with_its_nested_tables(
    loaded: dlt.Pipeline, query: Query
) -> None:
    dataset = loaded.dataset_name
    drop = pipeline_drop(pipeline=loaded, resources=["orders"])
    assert set(drop.info["tables"]) == {"orders", "orders__lines"}
    drop()
    assert _tables(query=query, dataset=dataset) == {"settings"}
    # The resource's state went with it, so it loads again from the start.
    loaded.run(shop().with_resources("orders"))
    assert query(f"SELECT count(*) FROM {dataset}.orders") == [(3,)]


def test_dlt_pipeline_drop_command(loaded: dlt.Pipeline, query: Query, url: str) -> None:
    """The CLI rebuilds the destination from the pipeline's state: no code, no import."""
    dataset = loaded.dataset_name
    pipelines_dir = loaded.pipelines_dir
    environment = {
        **os.environ,
        "DESTINATION__HARDCHARGER__CREDENTIALS": url,
    }
    done = subprocess.run(
        args=[
            sys.executable,
            "-m",
            "dlt",
            "--yes",
            "pipeline",
            "--pipelines-dir",
            pipelines_dir,
            loaded.pipeline_name,
            "drop",
            "settings",
        ],
        cwd=Path(pipelines_dir).parent,
        env=environment,
        capture_output=True,
        text=True,
        timeout=300,
    )
    assert done.returncode == 0, done.stdout + done.stderr
    assert _tables(query=query, dataset=dataset) == {"orders", "orders__lines"}


def test_dlt_pipeline_sync_command(loaded: dlt.Pipeline, url: str) -> None:
    pipelines_dir = loaded.pipelines_dir
    done = subprocess.run(
        args=[
            sys.executable,
            "-m",
            "dlt",
            "--yes",
            "pipeline",
            "--pipelines-dir",
            pipelines_dir,
            loaded.pipeline_name,
            "sync",
        ],
        cwd=Path(pipelines_dir).parent,
        env={**os.environ, "DESTINATION__HARDCHARGER__CREDENTIALS": url},
        capture_output=True,
        text=True,
        timeout=300,
    )
    assert done.returncode == 0, done.stdout + done.stderr
    restored = dlt.attach(pipeline_name=loaded.pipeline_name, pipelines_dir=pipelines_dir)
    assert "orders" in restored.default_schema.tables


def test_drop_the_whole_dataset(loaded: dlt.Pipeline, query: Query) -> None:
    dataset = loaded.dataset_name
    with loaded.sql_client() as client:
        client.drop_dataset()
    assert query(f"SELECT count(*) FROM duckdb_schemas() WHERE schema_name = '{dataset}'") == [(0,)]
    # And the pipeline can load into it again.
    loaded.run(shop(), refresh="drop_sources")
    assert query(f"SELECT count(*) FROM {dataset}.orders") == [(3,)]
