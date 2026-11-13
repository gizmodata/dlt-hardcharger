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
from dlt.common.schema.typing import (
    TColumnSchema,
    TLoaderMergeStrategy,
    TLoaderReplaceStrategy,
)
from dlt.destinations.impl.sqlalchemy.dialect import (
    DialectCapabilities,
    register_dialect_capabilities,
)
from dlt.destinations.impl.sqlalchemy.type_mapper import SqlalchemyTypeMapper
from sqlalchemy_hardcharger import HUGEINT, TIMESTAMP_MS, TIMESTAMP_NS, TIMESTAMP_S, TINYINT

DIALECT_NAME = "hardcharger"
SQLGLOT_DIALECT = "duckdb"

# The merge strategies dlt's SQLAlchemy destination runs on any dialect.
SQLALCHEMY_MERGE_STRATEGIES: tuple[TLoaderMergeStrategy, ...] = ("delete-insert", "scd2")
# Those, and the ones built on DuckDB 2.0's MERGE INTO, which the hardcharger destination adds.
MERGE_STRATEGIES: tuple[TLoaderMergeStrategy, ...] = (
    "delete-insert",
    "upsert",
    "scd2",
    "insert-only",
    "cdc",
)
REPLACE_STRATEGIES: tuple[TLoaderReplaceStrategy, ...] = (
    "truncate-and-insert",
    "insert-from-staging",
    "staging-optimized",
)

# Microseconds: what TIMESTAMP and TIMESTAMP WITH TIME ZONE keep.
TIMESTAMP_PRECISION = 6
# Nanoseconds: TIMESTAMP_NS, for a timestamp column without a time zone.
MAX_TIMESTAMP_PRECISION = 9


def _unit_digits(precision: int) -> int:
    """Fractional-second digits of the Arrow unit dlt's Parquet writer picks for a precision."""
    if precision == 0:
        return 0
    if precision <= 3:
        return 3
    if precision <= 6:
        return 6
    return 9


class _CutToPrecision:
    """Cut a value's fractional seconds to its column's precision before it is sent.

    dlt's Parquet writer stores a timestamp or time column in the Arrow unit of its precision
    (seconds for 0, milliseconds up to 3) and drops the digits beyond it. An ``INSERT``
    statement sends them all, and the server keeps them (TIMESTAMP WITH TIME ZONE and TIME are
    always in microseconds) or rounds (TIMESTAMP_S, TIMESTAMP_MS): 10:20:30.987654 would be
    10:20:30 from a Parquet file and 10:20:31 from an ``INSERT``. Cut here, both load paths
    store what the Parquet path stores.
    """

    digits: int

    def process_bind_param(self, value: Any, dialect: sa.engine.Dialect) -> Any:
        if value is None or not hasattr(value, "microsecond"):
            return value
        step = 10 ** (6 - self.digits)
        return value.replace(microsecond=value.microsecond // step * step)


class _CutTimestamp(_CutToPrecision, sa.TypeDecorator[Any]):
    impl = sa.DateTime
    cache_ok = True

    def __init__(self, kind: str, digits: int) -> None:
        super().__init__(timezone=kind == "timestamptz")
        self.kind = kind
        self.digits = digits

    def load_dialect_impl(self, dialect: sa.engine.Dialect) -> sa.types.TypeEngine[Any]:
        # The type itself, not dialect.type_descriptor() of it: that would turn TIMESTAMP_S
        # (a DateTime) into the dialect's plain TIMESTAMP in the table's DDL.
        return _TIMESTAMP_KINDS[self.kind]()  # type: ignore[no-any-return]


class _CutTime(_CutToPrecision, sa.TypeDecorator[Any]):
    impl = sa.Time
    cache_ok = True

    def __init__(self, digits: int) -> None:
        super().__init__(timezone=False)
        self.digits = digits


_TIMESTAMP_KINDS: dict[str, Any] = {
    "timestamptz": lambda: sa.DateTime(timezone=True),
    "timestamp_s": TIMESTAMP_S,
    "timestamp_ms": TIMESTAMP_MS,
}


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
        precision = column.get("precision")
        if precision is None:
            precision = self.capabilities.timestamp_precision
        if precision > MAX_TIMESTAMP_PRECISION:
            raise TerminalValueError(
                f"timestamp with precision {precision} can't be mapped to a Hardcharger type"
                " (TIMESTAMP_NS, 9 digits, is the finest)"
            )
        digits = _unit_digits(precision)
        if timezone is None or timezone:
            # TIMESTAMP WITH TIME ZONE has one precision: microseconds.
            if digits < 6:
                return _CutTimestamp(kind="timestamptz", digits=digits)
            return sa.DateTime(timezone=True)
        if digits == 0:
            return _CutTimestamp(kind="timestamp_s", digits=0)
        if digits == 3:
            return _CutTimestamp(kind="timestamp_ms", digits=3)
        if digits == 6:
            return sa.DateTime(timezone=False)
        return TIMESTAMP_NS()

    def db_type_from_time_type(
        self, column: TColumnSchema, table: PreparedTableSchema
    ) -> sa.types.TypeEngine[Any]:
        # Always TIME, whatever the column's `timezone` hint (which dlt defaults to true): a time
        # of day has no zone here, as on dlt's own DuckDB destination. TIME WITH TIME ZONE can't
        # be loaded reliably. Parquet and Arrow have no such type, so a value's offset is gone
        # before the file reaches the server, and the server then attaches its session's offset
        # to what arrives without one: the same pipeline would store 13:45-05 on one server and
        # 13:45+00 on another. Into TIME, a value's clock reading is stored and its offset
        # dropped, on both load paths and in every zone.
        precision = column.get("precision")
        if precision is None:
            precision = self.capabilities.timestamp_precision
        digits = _unit_digits(precision)
        if digits < 6:
            return _CutTime(digits=digits)
        return sa.Time(timezone=False)

    def _from_db_integer_type(self, db_type: sa.Integer) -> TColumnSchema:
        # BIGINT first: SQLAlchemy's BigInteger is an Integer, which the base mapper reads as
        # 32 bits.
        if isinstance(db_type, HUGEINT):
            return {"data_type": "bigint", "precision": 128}
        if isinstance(db_type, sa.BigInteger):
            return {"data_type": "bigint"}
        if isinstance(db_type, TINYINT):
            return {"data_type": "bigint", "precision": 8}
        if isinstance(db_type, sa.SmallInteger):
            return {"data_type": "bigint", "precision": 16}
        return {"data_type": "bigint", "precision": 32}

    def from_destination_type(  # type: ignore[override]
        self,
        db_type: sa.types.TypeEngine[Any],
        precision: int | None = None,
        scale: int | None = None,
    ) -> TColumnSchema:
        if isinstance(db_type, sa.TypeDecorator):
            db_type = db_type.load_dialect_impl(self.dialect)
        column = super().from_destination_type(db_type=db_type, precision=precision, scale=scale)
        for timestamp_type, digits in _TIMESTAMP_DIGITS.items():
            if isinstance(db_type, timestamp_type):
                column["precision"] = digits
        return column


_TIMESTAMP_DIGITS: dict[type, int] = {TIMESTAMP_S: 0, TIMESTAMP_MS: 3, TIMESTAMP_NS: 9}


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
        # Merge strategies stay as the destination set them: a plain dlt.destinations.sqlalchemy
        # runs dlt's SQLAlchemy merge job (delete-insert, scd2); the hardcharger destination
        # has its own job client and lists MERGE_STRATEGIES (dlt_hardcharger.destinations).
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
    # staging-optimized has no faster form here (no table clone or swap): dlt's SQLAlchemy
    # client runs it as insert-from-staging, which is the fallback dlt documents. Listing it
    # means a pipeline configured for it loads, where an unlisted strategy is an AssertionError.
    caps.supported_replace_strategies = list(REPLACE_STRATEGIES)


def register() -> None:
    """Register the capabilities with dlt's SQLAlchemy destination (idempotent)."""
    register_dialect_capabilities(
        dialect_name=DIALECT_NAME, caps_class=HardchargerDialectCapabilities
    )
