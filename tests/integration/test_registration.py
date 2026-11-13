"""How dlt finds the destination and its credentials."""

from __future__ import annotations

import dlt
import hardcharger
import pytest
from dlt.common.configuration.exceptions import ConfigurationValueError
from dlt.pipeline.exceptions import PipelineStepFailed
from sqlalchemy import create_engine

import dlt_hardcharger
from dlt_hardcharger.job_client import HardchargerJobClient

from .conftest import PASSWORD, USERNAME, MakePipeline, Query, RunScript

LOAD_AND_REPORT = """
    import sys
    import dlt

    assert "dlt_hardcharger" not in sys.modules, "the test must not import the package itself"
    pipeline = dlt.pipeline(pipeline_name="{name}", destination="{destination}", dataset_name="{name}")
    info = pipeline.run([{{"id": 1}}, {{"id": 2}}], table_name="numbers")
    assert not info.has_failed_jobs
    with pipeline.destination_client() as client:
        capabilities = client.capabilities
        print("client", type(client).__name__)
        print("sqlglot", capabilities.sqlglot_dialect)
        print("case_sensitive", capabilities.has_case_sensitive_identifiers)
        print("dialect_capabilities", type(capabilities.dialect_capabilities).__name__)
    print("destination_type", pipeline.destination.destination_type)
    print("state_destination_type", pipeline.state["destination_type"])
    print("rows", pipeline.dataset().numbers.select("id").order_by("id").fetchall())
"""


def _report(done) -> dict[str, str]:  # type: ignore[no-untyped-def]
    assert done.returncode == 0, done.stdout + done.stderr
    return dict(line.split(" ", 1) for line in done.stdout.splitlines() if " " in line)


def test_destination_by_name_needs_no_import(
    url: str, name: str, run_script: RunScript, query: Query
) -> None:
    """``destination="hardcharger"`` in a process that never imports ``dlt_hardcharger``."""
    done = run_script(
        code=LOAD_AND_REPORT.format(name=name, destination="hardcharger"),
        env={"DESTINATION__HARDCHARGER__CREDENTIALS": url},
    )
    report = _report(done)
    assert report["client"] == "HardchargerJobClient"
    assert report["sqlglot"] == "duckdb"
    assert report["case_sensitive"] == "False"
    assert report["destination_type"] == "dlt_hardcharger.destinations.hardcharger"
    assert report["state_destination_type"] == "dlt_hardcharger.destinations.hardcharger"
    assert report["rows"] == "[(1,), (2,)]"
    assert query(f"SELECT id FROM {name}.numbers ORDER BY id") == [(1,), (2,)]


def test_plain_sqlalchemy_destination_gets_the_capabilities_without_import(
    url: str, name: str, run_script: RunScript
) -> None:
    """dlt's own ``sqlalchemy`` destination, given a ``hardcharger://`` URL with a catalog."""
    done = run_script(
        code=LOAD_AND_REPORT.format(name=name, destination="sqlalchemy"),
        env={"DESTINATION__SQLALCHEMY__CREDENTIALS": url.replace("?", "/memory?")},
    )
    report = _report(done)
    assert report["client"] == "SqlalchemyJobClient"
    assert report["dialect_capabilities"] == "HardchargerDialectCapabilities"
    assert report["sqlglot"] == "duckdb"
    assert report["case_sensitive"] == "False"
    assert report["rows"] == "[(1,), (2,)]"


def test_credentials_from_secrets_toml_as_a_url(url: str, name: str, run_script: RunScript) -> None:
    done = run_script(
        code=LOAD_AND_REPORT.format(name=name, destination="hardcharger"),
        secrets=f'[destination.hardcharger]\ncredentials = "{url}"\n',
    )
    assert _report(done)["rows"] == "[(1,), (2,)]"


def test_credentials_from_secrets_toml_in_parts(
    server: hardcharger.Server, name: str, run_script: RunScript
) -> None:
    secrets = f"""
        [destination.hardcharger.credentials]
        host = "{server.host}"
        port = {server.port}
        username = "{USERNAME}"
        password = "{PASSWORD}"

        [destination.hardcharger.credentials.query]
        tls = "false"
    """
    done = run_script(
        code=LOAD_AND_REPORT.format(name=name, destination="hardcharger"),
        secrets="\n".join(line.strip() for line in secrets.splitlines()),
    )
    assert _report(done)["rows"] == "[(1,), (2,)]"


def test_missing_credentials_name_the_configuration_keys(name: str, run_script: RunScript) -> None:
    done = run_script(code=LOAD_AND_REPORT.format(name=name, destination="hardcharger"))
    assert done.returncode != 0
    assert "The hardcharger destination has no server to connect to" in done.stderr
    assert "DESTINATION__HARDCHARGER__CREDENTIALS" in done.stderr


def test_factory_with_credentials(pipeline: dlt.Pipeline, query: Query) -> None:
    info = pipeline.run([{"id": 1}], table_name="numbers")
    assert not info.has_failed_jobs
    assert pipeline.destination.destination_name == "hardcharger"
    with pipeline.destination_client() as client:
        assert type(client) is HardchargerJobClient
    assert query(f"SELECT id FROM {pipeline.dataset_name}.numbers") == [(1,)]


def test_module_path_reference(url: str, name: str, tmp_path, monkeypatch, query: Query) -> None:  # type: ignore[no-untyped-def]
    monkeypatch.setenv("DESTINATION__HARDCHARGER__CREDENTIALS", url)
    pipeline = dlt.pipeline(
        pipeline_name=name,
        destination="dlt_hardcharger.hardcharger",
        dataset_name=name,
        pipelines_dir=str(tmp_path),
    )
    pipeline.run([{"id": 1}], table_name="numbers")
    assert pipeline.destination.destination_type == "dlt_hardcharger.destinations.hardcharger"
    assert query(f"SELECT id FROM {name}.numbers") == [(1,)]


def test_named_destination_reads_its_own_section(
    url: str, name: str, tmp_path, monkeypatch, query: Query
) -> None:  # type: ignore[no-untyped-def]
    monkeypatch.setenv("DESTINATION__WAREHOUSE__CREDENTIALS", url)
    pipeline = dlt.pipeline(
        pipeline_name=name,
        destination=dlt_hardcharger.hardcharger(destination_name="warehouse"),
        dataset_name=name,
        pipelines_dir=str(tmp_path),
    )
    pipeline.run([{"id": 1}], table_name="numbers")
    assert query(f"SELECT id FROM {name}.numbers") == [(1,)]


def test_engine_as_credentials(url: str, make_pipeline: MakePipeline, query: Query) -> None:
    engine = create_engine(url)
    try:
        pipeline = make_pipeline(credentials=engine)
        pipeline.run([{"id": 1}], table_name="numbers")
        pipeline.run([{"id": 2}], table_name="numbers")
        assert query(f"SELECT id FROM {pipeline.dataset_name}.numbers ORDER BY id") == [(1,), (2,)]
    finally:
        engine.dispose()


def test_another_database_url_is_refused(make_pipeline: MakePipeline) -> None:
    pipeline = make_pipeline(credentials="postgresql://user:password@db.example.com/app")
    with pytest.raises(PipelineStepFailed) as raised:
        pipeline.run([{"id": 1}], table_name="numbers")
    cause = raised.value.__cause__
    while cause is not None and not isinstance(cause, ConfigurationValueError):
        cause = cause.__cause__
    assert cause is not None
    assert "takes a hardcharger:// URL, not `postgresql://`" in str(cause)


def test_wrong_password_is_an_authentication_error(
    url: str, make_pipeline: MakePipeline
) -> None:
    pipeline = make_pipeline(credentials=url.replace(PASSWORD, "not-the-password"))
    with pytest.raises(PipelineStepFailed) as raised:
        pipeline.run([{"id": 1}], table_name="numbers")
    assert "UNAUTHENTICATED" in str(raised.value)
