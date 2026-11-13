"""dlt's data types as the DDL the hardcharger dialect writes for them."""

from __future__ import annotations

from typing import Any

import pytest
import sqlalchemy as sa
from dlt.common.exceptions import TerminalValueError
from sqlalchemy.schema import CreateColumn
from sqlalchemy_hardcharger import HardchargerDialect

import dlt_hardcharger
from dlt_hardcharger import HardchargerTypeMapper

DIALECT = HardchargerDialect()
CAPABILITIES = dlt_hardcharger.hardcharger(
    credentials="hardcharger://user:password@db.example.com"
).capabilities()
MAPPER = HardchargerTypeMapper(capabilities=CAPABILITIES, dialect=DIALECT)

DDL = [
    # The dialect writes DuckDB's aliases: TEXT is VARCHAR, NUMERIC is DECIMAL.
    ({"data_type": "text"}, "TEXT"),
    ({"data_type": "text", "precision": 10}, "VARCHAR(10)"),
    ({"data_type": "double"}, "DOUBLE"),
    ({"data_type": "bool"}, "BOOLEAN"),
    ({"data_type": "date"}, "DATE"),
    ({"data_type": "json"}, "JSON"),
    ({"data_type": "binary"}, "BLOB"),
    ({"data_type": "decimal"}, "NUMERIC(38, 9)"),
    ({"data_type": "decimal", "precision": 10, "scale": 2}, "NUMERIC(10, 2)"),
    ({"data_type": "wei"}, "NUMERIC(38, 0)"),
    ({"data_type": "bigint"}, "BIGINT"),
    ({"data_type": "bigint", "precision": 8}, "TINYINT"),
    ({"data_type": "bigint", "precision": 16}, "SMALLINT"),
    ({"data_type": "bigint", "precision": 32}, "INTEGER"),
    ({"data_type": "bigint", "precision": 64}, "BIGINT"),
    ({"data_type": "bigint", "precision": 128}, "HUGEINT"),
    # dlt's timestamp has a time zone unless the column says it has none.
    ({"data_type": "timestamp"}, "TIMESTAMP WITH TIME ZONE"),
    ({"data_type": "timestamp", "timezone": True, "precision": 3}, "TIMESTAMP WITH TIME ZONE"),
    ({"data_type": "timestamp", "timezone": False}, "TIMESTAMP"),
    ({"data_type": "timestamp", "timezone": False, "precision": 0}, "TIMESTAMP_S"),
    ({"data_type": "timestamp", "timezone": False, "precision": 3}, "TIMESTAMP_MS"),
    ({"data_type": "timestamp", "timezone": False, "precision": 6}, "TIMESTAMP"),
    ({"data_type": "timestamp", "timezone": False, "precision": 9}, "TIMESTAMP_NS"),
    # A time of day has no zone unless the column asks for one.
    ({"data_type": "time"}, "TIME"),
    ({"data_type": "time", "timezone": False}, "TIME"),
    ({"data_type": "time", "timezone": True}, "TIME WITH TIME ZONE"),
]


def _ddl(column: dict[str, Any]) -> str:
    sa_type = MAPPER.to_destination_type(column={"name": "c", **column}, table=None)  # type: ignore[arg-type,typeddict-item]
    created = CreateColumn(sa.Column("c", sa_type)).compile(dialect=DIALECT)
    return str(created).removeprefix("c ")


@pytest.mark.parametrize(("column", "expected"), DDL, ids=[str(column) for column, _ in DDL])
def test_ddl_of_each_data_type(column: dict[str, Any], expected: str) -> None:
    assert _ddl(column=column) == expected


@pytest.mark.parametrize(
    ("column", "message"),
    [
        (
            {"data_type": "bigint", "precision": 256},
            "bigint with precision 256 can't be mapped to a Hardcharger integer type",
        ),
        (
            {"data_type": "timestamp", "timezone": False, "precision": 12},
            "timestamp with precision 12 can't be mapped to a Hardcharger type",
        ),
    ],
)
def test_a_precision_no_type_holds_is_refused(column: dict[str, Any], message: str) -> None:
    with pytest.raises(TerminalValueError) as raised:
        _ddl(column=column)
    assert message in str(raised.value)


def test_an_unknown_data_type_is_refused() -> None:
    with pytest.raises(TerminalValueError) as raised:
        _ddl(column={"data_type": "geography"})
    assert "Unsupported data type: `geography`" in str(raised.value)


REFLECTED = [
    ({"data_type": "text"}, {"data_type": "text"}),
    ({"data_type": "double"}, {"data_type": "double"}),
    ({"data_type": "bool"}, {"data_type": "bool"}),
    ({"data_type": "date"}, {"data_type": "date"}),
    ({"data_type": "json"}, {"data_type": "json"}),
    ({"data_type": "time"}, {"data_type": "time"}),
    ({"data_type": "bigint"}, {"data_type": "bigint"}),
    ({"data_type": "bigint", "precision": 8}, {"data_type": "bigint", "precision": 8}),
    ({"data_type": "bigint", "precision": 16}, {"data_type": "bigint", "precision": 16}),
    ({"data_type": "bigint", "precision": 128}, {"data_type": "bigint", "precision": 128}),
    (
        {"data_type": "timestamp", "timezone": False, "precision": 0},
        {"data_type": "timestamp", "timezone": False, "precision": 0},
    ),
    (
        {"data_type": "timestamp", "timezone": False, "precision": 3},
        {"data_type": "timestamp", "timezone": False, "precision": 3},
    ),
    (
        {"data_type": "timestamp", "timezone": False, "precision": 9},
        {"data_type": "timestamp", "timezone": False, "precision": 9},
    ),
    ({"data_type": "binary"}, {"data_type": "binary", "precision": None}),
    ({"data_type": "bigint", "precision": 32}, {"data_type": "bigint", "precision": 32}),
    ({"data_type": "wei"}, {"data_type": "wei"}),
    (
        {"data_type": "decimal", "precision": 10, "scale": 2},
        {"data_type": "decimal", "precision": 10, "scale": 2},
    ),
    ({"data_type": "timestamp"}, {"data_type": "timestamp", "timezone": True}),
    ({"data_type": "timestamp", "timezone": False}, {"data_type": "timestamp", "timezone": False}),
]


@pytest.mark.parametrize(("column", "expected"), REFLECTED, ids=[str(c) for c, _ in REFLECTED])
def test_a_type_maps_back_to_its_data_type(
    column: dict[str, Any], expected: dict[str, Any]
) -> None:
    sa_type = MAPPER.to_destination_type(column={"name": "c", **column}, table=None)  # type: ignore[arg-type,typeddict-item]
    assert MAPPER.from_destination_type(db_type=sa_type) == expected
