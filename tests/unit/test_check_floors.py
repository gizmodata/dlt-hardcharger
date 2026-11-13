"""CI's lowest-versions gate (scripts/check_floors.py) fails when it should."""

from __future__ import annotations

import importlib.util
import sys
from importlib.metadata import version
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parent.parent.parent
_spec = importlib.util.spec_from_file_location("check_floors", ROOT / "scripts" / "check_floors.py")
assert _spec is not None and _spec.loader is not None
check_floors = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(check_floors)


def test_python_floor_of_this_projects_pyproject() -> None:
    pyproject = (ROOT / "pyproject.toml").read_text(encoding="utf-8")
    assert check_floors.python_floor(pyproject=pyproject) == "3.10"


@pytest.mark.parametrize("line", ['requires-python = ">=3.10,<3.15"', 'requires-python = ">=3.10"'])
def test_python_floor_with_and_without_an_upper_bound(line: str) -> None:
    assert check_floors.python_floor(pyproject=f"[project]\n{line}\n") == "3.10"


def test_no_python_floor_is_an_error() -> None:
    with pytest.raises(SystemExit, match="no requires-python"):
        check_floors.python_floor(pyproject='[project]\nname = "x"\n')


def test_floors_of_built_metadata() -> None:
    requirements = [
        "dlt[parquet,sqlalchemy]<2,>=1.32.0",
        "sqlalchemy-hardcharger<2,>=1.1.0",
        'pytest>=9.2.0; extra == "dev"',
    ]
    assert check_floors.floors(requirements=requirements) == {
        "dlt": "1.32.0",
        "sqlalchemy-hardcharger": "1.1.0",
    }


def test_a_requirement_without_a_floor_is_an_error() -> None:
    with pytest.raises(SystemExit, match="without a >= floor"):
        check_floors.floors(requirements=["dlt<2"])


def test_problems() -> None:
    wanted = {"dlt": "1.32.0", "sqlalchemy-hardcharger": "1.1.0"}
    at_floor = check_floors.problems(python="3.10", running="3.10", wanted=wanted, installed=wanted)
    assert at_floor == []
    newer = {**wanted, "dlt": "1.33.0"}
    assert check_floors.problems(python="3.10", running="3.14", wanted=wanted, installed=newer) == [
        "requires-python's floor is 3.10, but this is Python 3.14",
        "dlt: floor 1.32.0, installed 1.33.0",
    ]


def test_the_script_fails_in_this_environment_unless_it_is_the_lowest() -> None:
    """On the installed package: Python 3.10 with the floors passes, anything else fails."""

    at_floors = sys.version_info[:2] == (3, 10) and all(
        version(name) == floor
        for name, floor in check_floors.floors(
            requirements=check_floors.requires(check_floors.DISTRIBUTION)
        ).items()
    )
    assert check_floors.main() == (0 if at_floors else 1)
