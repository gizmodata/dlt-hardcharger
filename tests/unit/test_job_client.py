"""The job client's decisions that need no server."""

from __future__ import annotations

import pytest
from adbc_driver_manager import AdbcStatusCode
from adbc_driver_manager import Error as AdbcError
from dlt.destinations.exceptions import (
    DatabaseTerminalException,
    DatabaseTransientException,
    DatabaseUndefinedRelation,
)

from dlt_hardcharger.job_client import ingest_exception


@pytest.mark.parametrize(
    ("status", "expected"),
    [
        (AdbcStatusCode.INVALID_ARGUMENT, DatabaseTerminalException),
        (AdbcStatusCode.INTEGRITY, DatabaseTerminalException),
        (AdbcStatusCode.UNAUTHORIZED, DatabaseTerminalException),
        (AdbcStatusCode.NOT_FOUND, DatabaseUndefinedRelation),
        (AdbcStatusCode.IO, DatabaseTransientException),
        (AdbcStatusCode.TIMEOUT, DatabaseTransientException),
        (AdbcStatusCode.UNKNOWN, DatabaseTransientException),
    ],
)
def test_ingest_errors_are_classified_by_adbc_status(
    status: AdbcStatusCode, expected: type[Exception]
) -> None:
    error = AdbcError("the driver's message", status_code=status)
    classified = ingest_exception(error=error)
    assert type(classified) is expected
    assert "the driver's message" in str(classified)
