"""Every write disposition, replace strategy and merge strategy, checked row by row."""

from __future__ import annotations

from typing import Any

import dlt
import pytest
from dlt.pipeline.exceptions import PipelineStepFailed

from dlt_hardcharger.capabilities import MERGE_STRATEGIES, REPLACE_STRATEGIES

from .conftest import MakePipeline, Query, load_formats

# Both ways a file reaches a table: Arrow bulk ingest (the default) and INSERT statements.
FORMATS = ["parquet", "typed-jsonl"]


def _orders(first: int, last: int) -> list[dict[str, Any]]:
    return [
        {
            "id": i,
            "status": "new",
            "lines": [{"sku": f"s{i}", "qty": 1}, {"sku": f"t{i}", "qty": 2}],
        }
        for i in range(first, last + 1)
    ]


def test_capabilities_list_the_strategies(pipeline: dlt.Pipeline) -> None:
    with pipeline.destination_client() as client:
        assert tuple(client.capabilities.supported_merge_strategies) == MERGE_STRATEGIES
        assert tuple(client.capabilities.supported_replace_strategies) == REPLACE_STRATEGIES
        assert client.capabilities.preferred_loader_file_format == "parquet"


@pytest.mark.parametrize("file_format", FORMATS)
def test_append(pipeline: dlt.Pipeline, query: Query, file_format: str) -> None:
    dataset = pipeline.dataset_name
    for first, last in ((1, 3), (4, 5)):
        info = pipeline.run(
            _orders(first=first, last=last),
            table_name="orders",
            write_disposition="append",
            loader_file_format=file_format,  # type: ignore[arg-type]
        )
        assert not info.has_failed_jobs
        assert load_formats(info=info, table_name="orders") == {file_format}
    assert query(f"SELECT id FROM {dataset}.orders ORDER BY id") == [(i,) for i in range(1, 6)]
    assert query(f"SELECT count(*), sum(qty) FROM {dataset}.orders__lines") == [(10, 15)]
    # Every child row hangs off its parent.
    assert query(
        f"SELECT count(*) FROM {dataset}.orders__lines AS l"
        f" JOIN {dataset}.orders AS o ON l._dlt_parent_id = o._dlt_id"
    ) == [(10,)]
    assert query(f"SELECT count(*) FROM {dataset}._dlt_loads WHERE status = 0") == [(2,)]


@pytest.mark.parametrize("file_format", FORMATS)
@pytest.mark.parametrize("strategy", REPLACE_STRATEGIES)
def test_replace(
    make_pipeline: MakePipeline, query: Query, strategy: str, file_format: str
) -> None:
    pipeline = make_pipeline(replace_strategy=strategy)
    dataset = pipeline.dataset_name
    for last in (10, 4):
        info = pipeline.run(
            _orders(first=1, last=last),
            table_name="orders",
            write_disposition="replace",
            loader_file_format=file_format,  # type: ignore[arg-type]
        )
        assert not info.has_failed_jobs
    with pipeline.destination_client() as client:
        assert client.config.replace_strategy == strategy
    assert query(f"SELECT id FROM {dataset}.orders ORDER BY id") == [(i,) for i in range(1, 5)]
    assert query(f"SELECT count(*) FROM {dataset}.orders__lines") == [(8,)]
    staging = query(
        "SELECT table_name FROM information_schema.tables"
        f" WHERE table_schema = '{dataset}_staging' AND table_name = 'orders'"
    )
    # truncate-and-insert writes to the tables themselves; the other two go through staging.
    assert staging == ([] if strategy == "truncate-and-insert" else [("orders",)])


def _merge_resource(strategy: str, **hints: Any) -> Any:
    @dlt.resource(
        name="items",
        write_disposition={"disposition": "merge", "strategy": strategy, **hints.pop("extra", {})},  # type: ignore[arg-type]
        **hints,
    )
    def items(rows: list[dict[str, Any]]) -> Any:
        yield rows

    return items


FIRST = [
    {"id": 1, "v": "a", "tags": [{"t": "a1"}, {"t": "a2"}]},
    {"id": 2, "v": "b", "tags": [{"t": "b1"}]},
]
SECOND = [
    {"id": 2, "v": "B", "tags": [{"t": "B1"}]},
    {"id": 3, "v": "c", "tags": []},
]
EXPECTED_AFTER_SECOND = {
    "delete-insert": ([(1, "a"), (2, "B"), (3, "c")], ["B1", "a1", "a2"]),
    "upsert": ([(1, "a"), (2, "B"), (3, "c")], ["B1", "a1", "a2"]),
    # Rows whose key is already there are left alone.
    "insert-only": ([(1, "a"), (2, "b"), (3, "c")], ["a1", "a2", "b1"]),
    # The second load is the whole truth: id 1 is absent from it, so it is deleted.
    "cdc": ([(2, "B"), (3, "c")], ["B1"]),
}


@pytest.mark.parametrize("file_format", FORMATS)
@pytest.mark.parametrize("strategy", sorted(EXPECTED_AFTER_SECOND))
def test_merge_by_primary_key(
    pipeline: dlt.Pipeline, query: Query, strategy: str, file_format: str
) -> None:
    dataset = pipeline.dataset_name
    items = _merge_resource(strategy=strategy, primary_key="id")
    for rows in (FIRST, SECOND):
        info = pipeline.run(items(rows), loader_file_format=file_format)  # type: ignore[arg-type]
        assert not info.has_failed_jobs
        assert load_formats(info=info, table_name="items") == {file_format}
    expected_rows, expected_tags = EXPECTED_AFTER_SECOND[strategy]
    assert query(f"SELECT id, v FROM {dataset}.items ORDER BY id") == expected_rows
    assert [t for (t,) in query(f"SELECT t FROM {dataset}.items__tags ORDER BY t")] == expected_tags
    # No child row is left without its parent.
    assert query(
        f"SELECT count(*) FROM {dataset}.items__tags AS c"
        f" WHERE c._dlt_root_id NOT IN (SELECT _dlt_id FROM {dataset}.items)"
    ) == [(0,)]


@pytest.mark.parametrize("file_format", FORMATS)
def test_merge_scd2(pipeline: dlt.Pipeline, query: Query, file_format: str) -> None:
    dataset = pipeline.dataset_name
    items = _merge_resource(strategy="scd2")
    loads = (
        [{"id": 1, "v": "a"}, {"id": 2, "v": "b"}],
        [{"id": 1, "v": "a"}, {"id": 2, "v": "B"}, {"id": 3, "v": "c"}],  # 2 changes, 3 is new
        [{"id": 2, "v": "B"}, {"id": 3, "v": "c"}],  # 1 is gone
        [{"id": 2, "v": "B"}, {"id": 3, "v": "c"}],  # nothing changes
    )
    for rows in loads:
        info = pipeline.run(items(rows), loader_file_format=file_format)  # type: ignore[arg-type]
        assert not info.has_failed_jobs
    history = query(
        f"SELECT id, v, _dlt_valid_to IS NULL FROM {dataset}.items ORDER BY id, _dlt_valid_from"
    )
    assert history == [
        (1, "a", False),
        (2, "b", False),
        (2, "B", True),
        (3, "c", True),
    ]
    # A retired version ends exactly where its successor begins.
    assert query(
        f"SELECT count(*) FROM {dataset}.items AS old JOIN {dataset}.items AS new"
        " ON old.id = new.id AND old._dlt_valid_to = new._dlt_valid_from"
    ) == [(1,)]
    assert query(
        f"SELECT count(*) FROM {dataset}.items WHERE _dlt_valid_to <= _dlt_valid_from"
    ) == [(0,)]


def test_merge_scd2_with_an_active_record_timestamp(pipeline: dlt.Pipeline, query: Query) -> None:
    dataset = pipeline.dataset_name
    items = _merge_resource(
        strategy="scd2", extra={"active_record_timestamp": "9999-12-31T00:00:00+00:00"}
    )
    pipeline.run(items([{"id": 1, "v": "a"}]))
    pipeline.run(items([{"id": 1, "v": "A"}]))
    assert query(
        f"SELECT v, _dlt_valid_to = TIMESTAMPTZ '9999-12-31 00:00:00+00'"
        f" FROM {dataset}.items ORDER BY _dlt_valid_from"
    ) == [("a", False), ("A", True)]


def test_merge_delete_insert_by_merge_key_replaces_the_partition(
    pipeline: dlt.Pipeline, query: Query
) -> None:
    dataset = pipeline.dataset_name
    items = _merge_resource(strategy="delete-insert", merge_key="day")
    pipeline.run(items([{"day": "mon", "n": 1}, {"day": "mon", "n": 2}, {"day": "tue", "n": 3}]))
    pipeline.run(items([{"day": "mon", "n": 9}]))
    assert query(f"SELECT day, n FROM {dataset}.items ORDER BY day, n") == [("mon", 9), ("tue", 3)]


def test_merge_delete_insert_dedups_the_batch_and_sorts(
    pipeline: dlt.Pipeline, query: Query
) -> None:
    dataset = pipeline.dataset_name
    items = _merge_resource(
        strategy="delete-insert",
        primary_key="id",
        columns={"seq": {"dedup_sort": "desc"}},
    )
    pipeline.run(items([{"id": 1, "seq": 1, "v": "old"}, {"id": 1, "seq": 3, "v": "newest"}]))
    pipeline.run(items([{"id": 1, "seq": 5, "v": "five"}, {"id": 1, "seq": 4, "v": "four"}]))
    assert query(f"SELECT id, seq, v FROM {dataset}.items") == [(1, 5, "five")]


@pytest.mark.parametrize("strategy", ["delete-insert", "upsert"])
def test_merge_hard_delete(pipeline: dlt.Pipeline, query: Query, strategy: str) -> None:
    dataset = pipeline.dataset_name
    items = _merge_resource(
        strategy=strategy, primary_key="id", columns={"deleted": {"hard_delete": True}}
    )
    pipeline.run(
        items(
            [
                {"id": 1, "v": "a", "deleted": False, "tags": [{"t": "a1"}]},
                {"id": 2, "v": "b", "deleted": False, "tags": [{"t": "b1"}]},
            ]
        )
    )
    pipeline.run(items([{"id": 1, "v": "a", "deleted": True, "tags": []}]))
    assert query(f"SELECT id, v FROM {dataset}.items ORDER BY id") == [(2, "b")]
    assert query(f"SELECT t FROM {dataset}.items__tags") == [("b1",)]


def test_merge_upsert_with_a_composite_key(pipeline: dlt.Pipeline, query: Query) -> None:
    dataset = pipeline.dataset_name
    items = _merge_resource(strategy="upsert", primary_key=["region", "id"])
    pipeline.run(items([{"region": "eu", "id": 1, "v": 1}, {"region": "us", "id": 1, "v": 1}]))
    pipeline.run(items([{"region": "us", "id": 1, "v": 2}, {"region": "us", "id": 2, "v": 2}]))
    assert query(f"SELECT region, id, v FROM {dataset}.items ORDER BY region, id") == [
        ("eu", 1, 1),
        ("us", 1, 2),
        ("us", 2, 2),
    ]


@pytest.mark.parametrize("strategy", sorted(EXPECTED_AFTER_SECOND))
def test_merge_into_tables_with_primary_key_and_unique_constraints(
    make_pipeline: MakePipeline, query: Query, strategy: str
) -> None:
    """Constraints are made with the table, and a merge that rewrites a key doesn't trip them."""
    pipeline = make_pipeline(create_primary_keys=True, create_unique_indexes=True)
    dataset = pipeline.dataset_name
    items = _merge_resource(
        strategy=strategy,
        primary_key="id",
        columns={"code": {"data_type": "text", "unique": True}},
    )
    first = [{"id": 1, "code": "a", "v": "a"}, {"id": 2, "code": "b", "v": "b"}]
    second = [{"id": 2, "code": "b", "v": "B"}, {"id": 3, "code": "c", "v": "c"}]
    for rows in (first, second, second):
        assert not pipeline.run(items(rows)).has_failed_jobs
    assert (
        query(f"SELECT id, v FROM {dataset}.items ORDER BY id")
        == EXPECTED_AFTER_SECOND[strategy][0]
    )
    constraints = query(
        "SELECT constraint_type, constraint_column_names FROM duckdb_constraints()"
        f" WHERE schema_name = '{dataset}' AND table_name = 'items'"
        " AND constraint_type <> 'NOT NULL' ORDER BY 1, 2"
    )
    assert constraints == [
        ("PRIMARY KEY", ["id"]),
        ("UNIQUE", ["_dlt_id"]),
        ("UNIQUE", ["code"]),
    ]


def test_a_duplicate_key_in_a_primary_key_table_fails_the_job_for_good(
    make_pipeline: MakePipeline, query: Query
) -> None:
    pipeline = make_pipeline(create_primary_keys=True)
    dataset = pipeline.dataset_name

    @dlt.resource(name="items", primary_key="id", write_disposition="append")
    def items(rows: list[dict[str, Any]]) -> Any:
        yield rows

    pipeline.run(items([{"id": 1}]))
    with pytest.raises(PipelineStepFailed) as raised:
        pipeline.run(items([{"id": 1}, {"id": 2}]))
    assert 'Duplicate key "id: 1" violates primary key constraint' in str(raised.value)
    # A terminal failure (a transient one is a LoadClientJobRetry after five tries), and
    # nothing of the file was kept.
    assert type(raised.value.__cause__).__name__ == "LoadClientJobTerminalRetry"
    assert query(f"SELECT id FROM {dataset}.items") == [(1,)]


def test_scd2_keeps_versions_that_a_primary_key_on_the_natural_key_forbids(
    make_pipeline: MakePipeline, query: Query
) -> None:
    """scd2 stores several rows per key: with create_primary_keys the second version is refused."""
    pipeline = make_pipeline(create_primary_keys=True)
    items = _merge_resource(strategy="scd2", primary_key="id")
    pipeline.run(items([{"id": 1, "v": "a"}]))
    with pytest.raises(PipelineStepFailed) as raised:
        pipeline.run(items([{"id": 1, "v": "A"}]))
    assert 'Duplicate key "id: 1" violates primary key constraint' in str(raised.value)
    # The merge is one transaction: the first version is still the current one.
    assert query(f"SELECT v, _dlt_valid_to IS NULL FROM {pipeline.dataset_name}.items") == [
        ("a", True)
    ]
