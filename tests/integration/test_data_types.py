"""Every dlt data type, stored as the right DuckDB type and read back as it was sent."""

from __future__ import annotations

import datetime
import decimal
import json
from typing import Any

import dlt
import pyarrow as pa
import pytest
from dlt.common.wei import Wei
from dlt.pipeline.exceptions import PipelineStepFailed

from .conftest import Query, load_formats

FORMATS = ["parquet", "typed-jsonl"]
UTC = datetime.timezone.utc
EST = datetime.timezone(datetime.timedelta(hours=-5))

COLUMNS: dict[str, Any] = {
    "c_text": {"data_type": "text"},
    "c_text_10": {"data_type": "text", "precision": 10},
    "c_double": {"data_type": "double"},
    "c_bool": {"data_type": "bool"},
    "c_ts": {"data_type": "timestamp"},
    "c_ts_tz_p3": {"data_type": "timestamp", "precision": 3},
    "c_ts_ntz": {"data_type": "timestamp", "timezone": False},
    "c_ts_p0": {"data_type": "timestamp", "timezone": False, "precision": 0},
    "c_ts_p3": {"data_type": "timestamp", "timezone": False, "precision": 3},
    "c_ts_p9": {"data_type": "timestamp", "timezone": False, "precision": 9},
    "c_bigint": {"data_type": "bigint"},
    "c_int8": {"data_type": "bigint", "precision": 8},
    "c_int16": {"data_type": "bigint", "precision": 16},
    "c_int32": {"data_type": "bigint", "precision": 32},
    "c_int128": {"data_type": "bigint", "precision": 128},
    "c_binary": {"data_type": "binary"},
    "c_json": {"data_type": "json"},
    "c_decimal": {"data_type": "decimal"},
    "c_decimal_38_18": {"data_type": "decimal", "precision": 38, "scale": 18},
    "c_decimal_10_2": {"data_type": "decimal", "precision": 10, "scale": 2},
    "c_wei": {"data_type": "wei"},
    "c_date": {"data_type": "date"},
    "c_time": {"data_type": "time"},
    "c_time_tz": {"data_type": "time", "timezone": True},
}

SERVER_TYPES = {
    "c_text": "VARCHAR",
    "c_text_10": "VARCHAR",
    "c_double": "DOUBLE",
    "c_bool": "BOOLEAN",
    "c_ts": "TIMESTAMP WITH TIME ZONE",
    "c_ts_tz_p3": "TIMESTAMP WITH TIME ZONE",
    "c_ts_ntz": "TIMESTAMP",
    "c_ts_p0": "TIMESTAMP_S",
    "c_ts_p3": "TIMESTAMP_MS",
    "c_ts_p9": "TIMESTAMP_NS",
    "c_bigint": "BIGINT",
    "c_int8": "TINYINT",
    "c_int16": "SMALLINT",
    "c_int32": "INTEGER",
    "c_int128": "HUGEINT",
    "c_binary": "BLOB",
    "c_json": "JSON",
    "c_decimal": "DECIMAL(38,9)",
    "c_decimal_38_18": "DECIMAL(38,18)",
    "c_decimal_10_2": "DECIMAL(10,2)",
    "c_wei": "DECIMAL(38,0)",
    "c_date": "DATE",
    "c_time": "TIME",
    "c_time_tz": "TIME WITH TIME ZONE",
}

# What dlt reads back from the table. The server keeps a hint only as far as its type does:
# VARCHAR(10) is VARCHAR, a zoned timestamp is always in microseconds.
REFLECTED: dict[str, Any] = {
    "c_text": {"data_type": "text"},
    "c_text_10": {"data_type": "text"},
    "c_double": {"data_type": "double"},
    "c_bool": {"data_type": "bool"},
    "c_ts": {"data_type": "timestamp", "timezone": True},
    "c_ts_tz_p3": {"data_type": "timestamp", "timezone": True},
    "c_ts_ntz": {"data_type": "timestamp", "timezone": False},
    "c_ts_p0": {"data_type": "timestamp", "timezone": False, "precision": 0},
    "c_ts_p3": {"data_type": "timestamp", "timezone": False, "precision": 3},
    "c_ts_p9": {"data_type": "timestamp", "timezone": False, "precision": 9},
    "c_bigint": {"data_type": "bigint"},
    "c_int8": {"data_type": "bigint", "precision": 8},
    "c_int16": {"data_type": "bigint", "precision": 16},
    "c_int32": {"data_type": "bigint", "precision": 32},
    "c_int128": {"data_type": "bigint", "precision": 128},
    "c_binary": {"data_type": "binary"},
    "c_json": {"data_type": "json"},
    "c_decimal": {"data_type": "decimal", "precision": 38, "scale": 9},
    "c_decimal_38_18": {"data_type": "decimal", "precision": 38, "scale": 18},
    "c_decimal_10_2": {"data_type": "decimal", "precision": 10, "scale": 2},
    "c_wei": {"data_type": "wei"},
    "c_date": {"data_type": "date"},
    "c_time": {"data_type": "time"},
    "c_time_tz": {"data_type": "time"},
}

ROW: dict[str, Any] = {
    "id": 1,
    "c_text": "héllo \n 'quoted' \"double\" 🚀",
    "c_text_10": "0123456789",
    "c_double": 1.2345678901234567e100,
    "c_bool": True,
    "c_ts": datetime.datetime(2026, 11, 13, 10, 20, 30, 123456, tzinfo=EST),
    "c_ts_tz_p3": datetime.datetime(2026, 11, 13, 10, 20, 30, 123000, tzinfo=UTC),
    "c_ts_ntz": datetime.datetime(2026, 11, 13, 10, 20, 30, 123456),
    "c_ts_p0": datetime.datetime(2026, 11, 13, 10, 20, 30),
    "c_ts_p3": datetime.datetime(2026, 11, 13, 10, 20, 30, 123000),
    "c_ts_p9": datetime.datetime(2026, 11, 13, 10, 20, 30, 123456),
    "c_bigint": 2**63 - 1,
    "c_int8": -128,
    "c_int16": 32767,
    "c_int32": -(2**31),
    "c_int128": -(2**63),
    "c_binary": b"\x00\x01\xff binary",
    "c_json": {"a": [1, 2, {"b": None}], "s": "x"},
    "c_decimal": decimal.Decimal("12345678901234567890.123456789"),
    "c_decimal_38_18": decimal.Decimal("12345678901234567890.123456789012345678"),
    "c_decimal_10_2": decimal.Decimal("-12345678.91"),
    "c_wei": Wei(2**120),
    "c_date": datetime.date(2026, 11, 13),
    "c_time": datetime.time(13, 45, 59, 123456),
    "c_time_tz": datetime.time(13, 45, 59, 123456, tzinfo=EST),
}
NULLS: dict[str, Any] = {"id": 2, **{column: None for column in COLUMNS}}

# How the server prints the values that have no one Python form, whatever the session's zone.
AS_TEXT = {
    "c_ts AT TIME ZONE 'UTC'": "2026-11-13 15:20:30.123456",
    "c_ts_tz_p3 AT TIME ZONE 'UTC'": "2026-11-13 10:20:30.123",
    "c_ts_ntz": "2026-11-13 10:20:30.123456",
    "c_ts_p0": "2026-11-13 10:20:30",
    "c_ts_p3": "2026-11-13 10:20:30.123",
    "c_ts_p9": "2026-11-13 10:20:30.123456",
    "c_time": "13:45:59.123456",
    "c_time_tz": "13:45:59.123456-05",
    "c_wei": str(2**120),
    "c_int128": str(-(2**63)),
    "c_decimal_38_18": "12345678901234567890.123456789012345678",
    "hex(c_binary)": b"\x00\x01\xff binary".hex().upper(),
}


@pytest.mark.parametrize("file_format", FORMATS)
def test_every_data_type(pipeline: dlt.Pipeline, query: Query, file_format: str) -> None:
    dataset = pipeline.dataset_name
    info = pipeline.run(
        [ROW, NULLS],
        table_name="types",
        columns=COLUMNS,
        loader_file_format=file_format,  # type: ignore[arg-type]
    )
    assert not info.has_failed_jobs
    assert load_formats(info=info, table_name="types") == {file_format}

    stored = dict(
        query(
            "SELECT column_name, data_type FROM duckdb_columns()"
            f" WHERE schema_name = '{dataset}' AND table_name = 'types'"
        )
    )
    assert {column: stored[column] for column in SERVER_TYPES} == SERVER_TYPES
    # No value needed a variant column: every one fitted the type its column was given.
    assert set(stored) == {"id", *COLUMNS, "_dlt_load_id", "_dlt_id"}

    # Read back from the server's catalog, each column is the dlt type it was declared as.
    with pipeline.destination_client() as job_client:
        ((_, reflected),) = job_client.get_storage_tables(table_names=["types"])  # type: ignore[attr-defined]
    hints = {
        column: {
            key: value
            for key, value in reflected[column].items()
            if key not in ("name", "nullable") and value is not None
        }
        for column in COLUMNS
    }
    assert hints == REFLECTED

    with pipeline.sql_client() as client:
        table = client.make_qualified_table_name("types")
        names = ["id", *COLUMNS]
        rows = client.execute_sql(f"SELECT {', '.join(names)} FROM {table} ORDER BY id")
    sent, nulls = (dict(zip(names, row, strict=True)) for row in rows)
    assert nulls == NULLS
    sent["c_json"] = json.loads(sent["c_json"])
    # TIME WITH TIME ZONE has no Arrow type: the driver hands over the time of day. The stored
    # value keeps its offset, which AS_TEXT checks.
    expected = {**ROW, "c_time_tz": ROW["c_time_tz"].replace(tzinfo=None)}
    assert sent == expected

    text = query(
        f"SELECT {', '.join(f'({expression})::VARCHAR' for expression in AS_TEXT)}"
        f" FROM {dataset}.types WHERE id = 1"
    )
    assert dict(zip(AS_TEXT, text[0], strict=True)) == AS_TEXT


ARROW_TYPES = {
    "id": "BIGINT",
    "ts_naive": "TIMESTAMP WITH TIME ZONE",
    "ts_aware": "TIMESTAMP WITH TIME ZONE",
    "ts_ns": "TIMESTAMP WITH TIME ZONE",
    "ts_s": "TIMESTAMP WITH TIME ZONE",
    "t64": "TIME",
    "struct": "JSON",
    "lst": "JSON",
    "bin": "BLOB",
    "dec": "DECIMAL(38,18)",
    "f32": "DOUBLE",
    "i8": "TINYINT",
    "u32": "INTEGER",
    "lstr": "VARCHAR",
    "d32": "DATE",
    "flag": "BOOLEAN",
}


def _arrow_table() -> pa.Table:
    return pa.table(
        {
            "id": pa.array([1, 2], type=pa.int64()),
            # dlt reads an Arrow timestamp without a zone as UTC (on every destination).
            "ts_naive": pa.array(
                [datetime.datetime(2026, 1, 1, 12, 0, 0, 5), None], type=pa.timestamp("us")
            ),
            "ts_aware": pa.array(
                [datetime.datetime(2026, 1, 1, 12, 0, 0, 5, tzinfo=UTC), None],
                type=pa.timestamp("us", tz="UTC"),
            ),
            "ts_ns": pa.array([1_767_268_800_123_456_000, None], type=pa.timestamp("ns")),
            "ts_s": pa.array([1_767_268_800, None], type=pa.timestamp("s")),
            "t64": pa.array([datetime.time(1, 2, 3, 456), None], type=pa.time64("us")),
            "struct": pa.array(
                [{"a": 1, "b": "x"}, None],
                type=pa.struct([("a", pa.int64()), ("b", pa.string())]),
            ),
            "lst": pa.array([[1, 2, 3], None], type=pa.list_(pa.int64())),
            "bin": pa.array([b"\x00\xff", None], type=pa.binary()),
            "dec": pa.array([decimal.Decimal("1.5"), None], type=pa.decimal128(38, 18)),
            "f32": pa.array([1.5, None], type=pa.float32()),
            "i8": pa.array([-7, None], type=pa.int8()),
            "u32": pa.array([2**31 - 1, None], type=pa.uint32()),
            "lstr": pa.array(["large", None], type=pa.large_string()),
            "d32": pa.array([datetime.date(2026, 1, 2), None], type=pa.date32()),
            "flag": pa.array([True, None], type=pa.bool_()),
        }
    )


@pytest.mark.parametrize("file_format", FORMATS)
def test_arrow_table_input(pipeline: dlt.Pipeline, query: Query, file_format: str) -> None:
    dataset = pipeline.dataset_name
    info = pipeline.run(_arrow_table(), table_name="arrow", loader_file_format=file_format)  # type: ignore[arg-type]
    assert not info.has_failed_jobs
    stored = dict(
        query(
            "SELECT column_name, data_type FROM duckdb_columns()"
            f" WHERE schema_name = '{dataset}' AND table_name = 'arrow'"
        )
    )
    assert {column: stored[column] for column in ARROW_TYPES} == ARROW_TYPES
    rows = query(
        "SELECT id, (ts_naive AT TIME ZONE 'UTC')::VARCHAR, (ts_aware AT TIME ZONE 'UTC')::VARCHAR,"
        " (ts_ns AT TIME ZONE 'UTC')::VARCHAR, (ts_s AT TIME ZONE 'UTC')::VARCHAR, t64::VARCHAR,"
        " struct::VARCHAR, lst::VARCHAR, hex(bin), dec::VARCHAR, f32, i8, u32, lstr,"
        f" d32::VARCHAR, flag FROM {dataset}.arrow ORDER BY id"
    )
    first = list(rows[0])
    first[6], first[7] = json.loads(first[6]), json.loads(first[7])
    assert first == [
        1,
        "2026-01-01 12:00:00.000005",
        "2026-01-01 12:00:00.000005",
        "2026-01-01 12:00:00.123456",
        "2026-01-01 12:00:00",
        "01:02:03.000456",
        {"a": 1, "b": "x"},
        [1, 2, 3],
        "00FF",
        "1.500000000000000000",
        1.5,
        -7,
        2**31 - 1,
        "large",
        "2026-01-02",
        True,
    ]
    assert rows[1] == (2, *([None] * 15))


def test_a_value_out_of_range_fails_the_job_for_good(pipeline: dlt.Pipeline, query: Query) -> None:
    """Arrow ingest reports the server's conversion error as terminal: no five retries."""
    table = pa.table({"id": pa.array([1, 2]), "u64": pa.array([5, 2**63 + 5], type=pa.uint64())})
    with pytest.raises(PipelineStepFailed) as raised:
        pipeline.run(table, table_name="big")
    assert type(raised.value.__cause__).__name__ == "LoadClientJobTerminalRetry"
    assert "out of range for the destination type INT64" in str(raised.value)
    # All or nothing: the row that did fit wasn't kept either.
    assert query(f"SELECT count(*) FROM {pipeline.dataset_name}.big") == [(0,)]
