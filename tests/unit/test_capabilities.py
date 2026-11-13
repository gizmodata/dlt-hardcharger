"""What the destination tells dlt about Hardcharger, with no server."""

from __future__ import annotations

import dlt
import pytest
from dlt.common.destination import Destination
from dlt.destinations.impl.sqlalchemy.dialect import DIALECT_CAPS_REGISTRY, get_dialect_capabilities

import dlt_hardcharger
from dlt_hardcharger import HardchargerDialectCapabilities, HardchargerTypeMapper
from dlt_hardcharger.capabilities import MERGE_STRATEGIES, REPLACE_STRATEGIES
from dlt_hardcharger.job_client import HardchargerJobClient

URL = "hardcharger://user:password@db.example.com/lake"


def test_importing_the_package_registers_the_dialect_capabilities() -> None:
    assert DIALECT_CAPS_REGISTRY["hardcharger"] is HardchargerDialectCapabilities
    capabilities = get_dialect_capabilities("hardcharger")
    assert isinstance(capabilities, HardchargerDialectCapabilities)
    assert capabilities.sqlglot_dialect == "duckdb"
    assert capabilities.type_mapper_class() is HardchargerTypeMapper


@pytest.mark.parametrize("credentials", [None, URL], ids=["no credentials yet", "with a URL"])
def test_hardcharger_destination_capabilities(credentials: str | None) -> None:
    destination = dlt_hardcharger.hardcharger(credentials=credentials)
    capabilities = destination.capabilities()
    assert capabilities.sqlglot_dialect == "duckdb"
    assert capabilities.has_case_sensitive_identifiers is False
    assert capabilities.casefold_identifier is str  # case is kept
    assert capabilities.preferred_loader_file_format == "parquet"
    assert list(capabilities.supported_loader_file_formats) == ["typed-jsonl", "parquet", "model"]
    assert tuple(capabilities.supported_merge_strategies) == MERGE_STRATEGIES
    assert tuple(capabilities.supported_replace_strategies) == REPLACE_STRATEGIES
    assert capabilities.timestamp_precision == 6
    assert capabilities.max_timestamp_precision == 9
    assert capabilities.decimal_precision == (38, 9)
    assert capabilities.wei_precision == (38, 0)
    assert capabilities.type_mapper is HardchargerTypeMapper
    assert capabilities.supports_ddl_transactions is True
    assert destination.client_class is HardchargerJobClient


def test_identifier_length_comes_from_the_dialect() -> None:
    capabilities = dlt_hardcharger.hardcharger(credentials=URL).capabilities()
    assert capabilities.max_identifier_length == 1024
    assert capabilities.max_column_identifier_length == 1024
    assert isinstance(capabilities.dialect_capabilities, HardchargerDialectCapabilities)


def test_plain_sqlalchemy_destination_with_a_hardcharger_url() -> None:
    """dlt's own destination runs its own merge job: the MERGE INTO strategies aren't offered."""
    capabilities = dlt.destinations.sqlalchemy(credentials=URL).capabilities()
    assert isinstance(capabilities.dialect_capabilities, HardchargerDialectCapabilities)
    assert capabilities.sqlglot_dialect == "duckdb"
    assert capabilities.has_case_sensitive_identifiers is False
    assert capabilities.type_mapper is HardchargerTypeMapper
    assert list(capabilities.supported_merge_strategies) == ["delete-insert", "scd2"]
    assert tuple(capabilities.supported_replace_strategies) == REPLACE_STRATEGIES
    assert capabilities.preferred_loader_file_format == "typed-jsonl"


def test_explicit_capabilities_win() -> None:
    destination = dlt_hardcharger.hardcharger(
        credentials=URL, preferred_loader_file_format="typed-jsonl", max_rows_per_insert=500
    )
    capabilities = destination.capabilities()
    assert capabilities.preferred_loader_file_format == "typed-jsonl"
    assert capabilities.max_rows_per_insert == 500


# The server's own messages, as the driver hands them to SQLAlchemy.
MISSING_TABLE = (
    "(adbc_driver_manager.ProgrammingError) NOT_FOUND: [chargewire] ExecuteQuery: Catalog Error:"
    " Table with name no_such does not exist!"
)
MISSING_SCHEMA = (
    "(adbc_driver_manager.ProgrammingError) NOT_FOUND: [chargewire] ExecuteQuery: Catalog Error:"
    ' Table with name "nope.t" does not exist because schema "nope" does not exist.'
)
MISSING_COLUMN = (
    "(adbc_driver_manager.ProgrammingError) INVALID_ARGUMENT: [chargewire] ExecuteQuery: Binder"
    ' Error: Referenced column "nope" not found in FROM clause!'
)
SYNTAX_ERROR = (
    "(adbc_driver_manager.ProgrammingError) INVALID_ARGUMENT: [chargewire] ExecuteQuery: Parser"
    ' Error: syntax error at or near "SELEC"'
)


@pytest.mark.parametrize(
    ("message", "expected"),
    [(MISSING_TABLE, True), (MISSING_SCHEMA, True), (MISSING_COLUMN, False), (SYNTAX_ERROR, None)],
    ids=["missing table", "missing schema", "missing column", "syntax error"],
)
def test_is_undefined_relation(message: str, expected: bool | None) -> None:
    capabilities = HardchargerDialectCapabilities(backend_name="hardcharger")
    assert capabilities.is_undefined_relation(Exception(message)) is expected


def test_destination_is_found_by_name_and_by_module_path() -> None:
    by_name = Destination.from_reference("hardcharger", credentials=URL)
    by_path = Destination.from_reference("dlt_hardcharger.hardcharger", credentials=URL)
    assert type(by_name) is type(by_path) is dlt_hardcharger.hardcharger
    assert by_name.destination_type == "dlt_hardcharger.destinations.hardcharger"
    assert by_name.destination_name == "hardcharger"
