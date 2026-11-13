"""What dlt's SQLAlchemy destination needs to know about Hardcharger.

dlt's SQLAlchemy destination has a public hook for dialects it has no built-in knowledge of:
``register_dialect_capabilities(dialect_name, caps_class)``. Importing ``dlt_hardcharger``
registers :class:`HardchargerDialectCapabilities` for the ``hardcharger`` dialect, so both
``dlt_hardcharger.hardcharger(...)`` and a plain ``dlt.destinations.sqlalchemy`` given a
``hardcharger://`` URL use it.
"""

from __future__ import annotations

from typing import Any

import sqlalchemy as sa
from dlt.common.destination.capabilities import DataTypeMapper, DestinationCapabilitiesContext
from dlt.common.destination.typing import PreparedTableSchema
from dlt.common.exceptions import TerminalValueError
from dlt.common.schema.typing import TColumnSchema
from dlt.destinations.impl.sqlalchemy.dialect import (
    DialectCapabilities,
    register_dialect_capabilities,
)
from dlt.destinations.impl.sqlalchemy.type_mapper import SqlalchemyTypeMapper
from sqlalchemy_hardcharger import HUGEINT, TIMESTAMP_MS, TIMESTAMP_NS, TIMESTAMP_S, TINYINT

DIALECT_NAME = "hardcharger"
SQLGLOT_DIALECT = "duckdb"

# Microseconds: what TIMESTAMP and TIMESTAMP WITH TIME ZONE keep.
TIMESTAMP_PRECISION = 6
# Nanoseconds: TIMESTAMP_NS, for a timestamp column without a time zone.
MAX_TIMESTAMP_PRECISION = 9


class HardchargerTypeMapper(SqlalchemyTypeMapper):
    """dlt's data types as DuckDB types, the way dlt's own DuckDB destination maps them."""

    def db_type_from_bigint_type(
        self, column: TColumnSchema, table: PreparedTableSchema
    ) -> sa.types.TypeEngine[Any]:
        precision = column.get("precision")
        if precision is None:
            return sa.BigInteger()
        if precision <= 8:
            return TINYINT()
        if precision <= 16:
            return sa.SmallInteger()
        if precision <= 32:
            return sa.Integer()
        if precision <= 64:
            return sa.BigInteger()
        if precision <= 128:
            return HUGEINT()
        raise TerminalValueError(
            f"bigint with precision {precision} can't be mapped to a Hardcharger integer type"
            " (HUGEINT, 128 bits, is the widest)"
        )

    def db_type_from_timestamp_type(
        self, column: TColumnSchema, table: PreparedTableSchema
    ) -> sa.types.TypeEngine[Any]:
        # dlt's timestamps are time zone aware unless the column says timezone=False.
        timezone = column.get("timezone")
        if timezone is None or timezone:
            # TIMESTAMP WITH TIME ZONE has one precision: microseconds.
            return sa.DateTime(timezone=True)
        precision = column.get("precision")
        if precision is None:
            precision = self.capabilities.timestamp_precision
        if precision == 0:
            return TIMESTAMP_S()
        if precision <= 3:
            return TIMESTAMP_MS()
        if precision <= 6:
            return sa.DateTime(timezone=False)
        if precision <= MAX_TIMESTAMP_PRECISION:
            return TIMESTAMP_NS()
        raise TerminalValueError(
            f"timestamp with precision {precision} can't be mapped to a Hardcharger type"
            " (TIMESTAMP_NS, 9 digits, is the finest)"
        )

    def db_type_from_time_type(
        self, column: TColumnSchema, table: PreparedTableSchema
    ) -> sa.types.TypeEngine[Any]:
        # dlt's "time" is a time of day: TIME. The base mapper asks for TIME WITH TIME ZONE
        # unless the column says timezone=False, and the server then attaches the session's
        # offset to every value.
        return sa.Time(timezone=bool(column.get("timezone", False)))


class HardchargerDialectCapabilities(DialectCapabilities):
    """Hardcharger for dlt's SQLAlchemy destination: DuckDB 2.0 behind the Chargewire protocol."""

    @property
    def sqlglot_dialect(self) -> str:
        # Hardcharger runs DuckDB SQL. sqlglot has no "hardcharger" dialect, and without this
        # every pipeline.dataset() read fails with "Unknown dialect 'hardcharger'".
        return SQLGLOT_DIALECT

    def adjust_capabilities(
        self, caps: DestinationCapabilitiesContext, dialect: sa.engine.interfaces.Dialect
    ) -> None:
        apply_hardcharger_capabilities(caps=caps)

    def type_mapper_class(self) -> type[DataTypeMapper]:
        return HardchargerTypeMapper

    def is_undefined_relation(self, e: Exception) -> bool | None:
        # A Binder Error (an unknown column, say) can carry "not found" or "does not exist" in
        # its text, which the generic patterns read as a missing table. Only a Catalog Error
        # names a missing table or schema.
        if "binder error" in str(e).lower():
            return False
        return super().is_undefined_relation(e)


def apply_hardcharger_capabilities(caps: DestinationCapabilitiesContext) -> None:
    """Set what is true of Hardcharger whatever the connection."""
    caps.sqlglot_dialect = SQLGLOT_DIALECT  # type: ignore[assignment]
    # DuckDB keeps an identifier's case and compares case-insensitively: "Name" and "name"
    # are one column. Telling dlt so makes it refuse a schema with both before any DDL runs.
    caps.has_case_sensitive_identifiers = False
    caps.timestamp_precision = TIMESTAMP_PRECISION
    caps.max_timestamp_precision = MAX_TIMESTAMP_PRECISION


def register() -> None:
    """Register the capabilities with dlt's SQLAlchemy destination (idempotent)."""
    register_dialect_capabilities(
        dialect_name=DIALECT_NAME, caps_class=HardchargerDialectCapabilities
    )
