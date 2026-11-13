"""Fixtures for the integration tests: a real Hardcharger server.

The ``hardcharger`` package downloads the server binary for this platform on first use and runs
it on a free port of 127.0.0.1 (``HARDCHARGER_SERVER_BINARY`` names another build to test). The
server is a requirement, not an option: if it can't start, every test here errors. Nothing skips.
"""

from __future__ import annotations

import itertools
import os
import subprocess
import sys
import textwrap
from collections.abc import Callable, Iterator
from pathlib import Path
from typing import Any

import dlt
import hardcharger
import pytest
from adbc_driver_hardcharger import dbapi

import dlt_hardcharger

USERNAME = "dlt_test"
PASSWORD = "dlt-test-password"

_names = itertools.count()


@pytest.hookimpl(hookwrapper=True)
def pytest_runtest_makereport(item: pytest.Item, call: pytest.CallInfo[None]) -> Iterator[None]:
    """A skipped integration test is a failed one: these tests exist to run against a server."""
    outcome = yield
    report = outcome.get_result()
    if report.skipped:
        report.outcome = "failed"
        report.longrepr = f"integration tests never skip, but this one did: {report.longrepr}"


@pytest.fixture(scope="session")
def server() -> Iterator[hardcharger.Server]:
    with hardcharger.Server(username=USERNAME, password=PASSWORD) as running:
        assert running.host == "127.0.0.1"
        yield running


def server_url(server: hardcharger.Server, catalog: str | None = None) -> str:
    """The ``hardcharger://`` URL of the test server, in a catalog or the server's default."""
    path = f"/{catalog}" if catalog else ""
    return f"hardcharger://{USERNAME}:{PASSWORD}@{server.host}:{server.port}{path}?tls=false"


@pytest.fixture(scope="session")
def url(server: hardcharger.Server) -> str:
    return server_url(server=server)


Query = Callable[[str], list[tuple[Any, ...]]]


@pytest.fixture(scope="session")
def query(server: hardcharger.Server) -> Iterator[Query]:
    """Run SQL through the plain driver (not dlt, not SQLAlchemy) and return the rows."""
    connection = dbapi.connect(
        uri=f"hardcharger://{server.host}:{server.port}?tls=false",
        username=USERNAME,
        password=PASSWORD,
    )

    def run(sql: str) -> list[tuple[Any, ...]]:
        with connection.cursor() as cursor:
            cursor.execute(operation=sql)
            if cursor.description is None:
                return []
            return [tuple(row) for row in cursor.fetchall()]

    yield run
    connection.close()


@pytest.fixture
def name() -> str:
    """A pipeline and dataset name no other test uses."""
    return f"it_{next(_names)}"


MakePipeline = Callable[..., dlt.Pipeline]


@pytest.fixture
def make_pipeline(url: str, name: str, tmp_path: Path) -> MakePipeline:
    """Make a pipeline into the test server; keyword arguments go to the destination factory."""

    def make(
        pipeline_name: str | None = None,
        dataset_name: str | None = None,
        credentials: Any = None,
        pipelines_dir: Path | None = None,
        **destination_kwargs: Any,
    ) -> dlt.Pipeline:
        return dlt.pipeline(
            pipeline_name=pipeline_name or name,
            destination=dlt_hardcharger.hardcharger(
                credentials=credentials or url, **destination_kwargs
            ),
            dataset_name=dataset_name or pipeline_name or name,
            pipelines_dir=str(pipelines_dir or tmp_path / "pipelines"),
        )

    return make


@pytest.fixture
def pipeline(make_pipeline: MakePipeline) -> dlt.Pipeline:
    return make_pipeline()


RunScript = Callable[..., subprocess.CompletedProcess[str]]


@pytest.fixture
def run_script(tmp_path: Path) -> RunScript:
    """Run Python code in a new interpreter, in a dlt project directory of its own.

    The registration tests need this: whether dlt finds the destination without an import can
    only be seen in a process that has not imported it.
    """

    def run(
        code: str, env: dict[str, str] | None = None, secrets: str | None = None
    ) -> subprocess.CompletedProcess[str]:
        project = tmp_path / "project"
        (project / ".dlt").mkdir(parents=True, exist_ok=True)
        if secrets is not None:
            (project / ".dlt" / "secrets.toml").write_text(secrets, encoding="utf-8")
        script = project / "script.py"
        script.write_text(textwrap.dedent(code), encoding="utf-8")
        environment = {
            name: value
            for name, value in os.environ.items()
            if not name.startswith(("DESTINATION__", "SCHEMA__"))
        }
        environment.update(
            {
                "DLT_PROJECT_DIR": str(project),
                "DLT_DATA_DIR": str(tmp_path / "data"),
                "RUNTIME__DLTHUB_TELEMETRY": "false",
                **(env or {}),
            }
        )
        return subprocess.run(
            args=[sys.executable, str(script)],
            cwd=project,
            env=environment,
            capture_output=True,
            text=True,
            timeout=300,
        )

    return run


def load_formats(info: Any, table_name: str) -> set[str]:
    """The file formats of one table's completed data jobs (not its follow-up SQL jobs)."""
    return {
        job.job_file_info.file_format
        for package in info.load_packages
        for job in package.jobs["completed_jobs"]
        if job.job_file_info.table_name == table_name and job.job_file_info.file_format != "sql"
    }
