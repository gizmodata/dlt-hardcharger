"""Explicit profile configuration loads a real pipeline without a profile-bearing URL."""

from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

import hardcharger
import pytest

from .conftest import MakePipeline


def test_a_profile_from_connect_args_loads_and_reads_a_pipeline(
    server: hardcharger.Server,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    make_pipeline: MakePipeline,
) -> None:
    drivers = tmp_path / "drivers"
    profiles = tmp_path / "profiles"
    profiles.mkdir()
    subprocess.run(
        args=[sys.executable, "-m", "adbc_driver_hardcharger.manifest", "--dir", str(drivers)],
        check=True,
        capture_output=True,
    )
    monkeypatch.setenv(name="ADBC_DRIVER_PATH", value=str(drivers))
    monkeypatch.setenv(name="ADBC_PROFILE_PATH", value=str(profiles))
    (profiles / "warehouse.toml").write_text(
        data='profile_version = 1\ndriver = "hardcharger"\n\n[Options]\n'
        f"uri = {json.dumps(obj=server.uri)}\n"
        f"username = {json.dumps(obj=server.username)}\n"
        f"password = {json.dumps(obj=server.password)}\n",
        encoding="utf-8",
    )
    pipeline = make_pipeline(
        credentials="hardcharger://",
        engine_kwargs={"connect_args": {"profile": "warehouse"}},
    )
    info = pipeline.run(data=[{"id": 1}], table_name="profile_rows")
    assert not info.has_failed_jobs
    assert pipeline.dataset().profile_rows.select("id").fetchall() == [(1,)]
