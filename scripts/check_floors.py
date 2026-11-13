"""Fail unless this environment holds exactly the lowest versions pyproject.toml allows.

    python scripts/check_floors.py

CI's lowest-versions job installs with ``uv pip install --resolution lowest-direct`` and then
runs this: the Python must be ``requires-python``'s floor, and every requirement of the package
itself (not of its extras) must be installed at its ``>=`` version. Without the check, a job
that quietly resolved something newer would test nothing a floor promises.
"""

from __future__ import annotations

import re
import sys
from importlib.metadata import requires, version
from pathlib import Path

PYPROJECT = Path(__file__).resolve().parent.parent / "pyproject.toml"
DISTRIBUTION = "dlt-hardcharger"


def python_floor(pyproject: str) -> str:
    """The lowest Python of ``requires-python`` (">=3.10,<3.15" and ">=3.10" alike)."""
    found = re.search(r'^requires-python = ">=([0-9.]+)', pyproject, flags=re.MULTILINE)
    if found is None:
        raise SystemExit("pyproject.toml has no requires-python with a >= floor")
    return found.group(1)


def floors(requirements: list[str]) -> dict[str, str]:
    """Name and ``>=`` version of each requirement that isn't an extra's."""
    result: dict[str, str] = {}
    for requirement in requirements:
        if "extra ==" in requirement:
            continue
        # Built metadata orders the specifiers its own way: "dlt[parquet,sqlalchemy]<2,>=1.32.0".
        name = re.match(r"[A-Za-z0-9_.-]+", requirement)
        floor = re.search(r">=([0-9][0-9A-Za-z.]*)", requirement)
        if name is None or floor is None:
            raise SystemExit(f"requirement without a >= floor: {requirement!r}")
        result[name.group(0)] = floor.group(1)
    return result


def problems(
    python: str, running: str, wanted: dict[str, str], installed: dict[str, str]
) -> list[str]:
    found = []
    if running != python:
        found.append(f"requires-python's floor is {python}, but this is Python {running}")
    for name, floor in wanted.items():
        if installed[name] != floor:
            found.append(f"{name}: floor {floor}, installed {installed[name]}")
    return found


def main() -> int:
    python = python_floor(pyproject=PYPROJECT.read_text(encoding="utf-8"))
    running = f"{sys.version_info[0]}.{sys.version_info[1]}"
    wanted = floors(requirements=requires(DISTRIBUTION) or [])
    if not wanted:
        raise SystemExit(f"{DISTRIBUTION} declares no requirements: nothing would be checked")
    installed = {name: version(name) for name in wanted}
    print(f"python: floor {python}, running {running}")
    for name, floor in wanted.items():
        print(f"{name}: floor {floor}, installed {installed[name]}")
    found = problems(python=python, running=running, wanted=wanted, installed=installed)
    for problem in found:
        print(f"NOT AT ITS FLOOR: {problem}")
    return 1 if found else 0


if __name__ == "__main__":
    sys.exit(main())
