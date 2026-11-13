"""Run every Python example in README.md against a real Hardcharger server.

    python scripts/run_readme_examples.py

Each ```python block runs as written, in its own interpreter and its own dlt project directory,
against a server started here with the user and password the examples use (only
``localhost:31337`` is replaced by the server's real address). A block followed by a ```text
block must print exactly that text.

An example that takes its credentials from dlt's configuration has a setup block before it,
marked ``<!-- setup: .dlt/secrets.toml -->`` (a ```toml block, written to that file) or
``<!-- setup: environment -->`` (a ```bash block of ``export NAME="value"`` lines). A block that
can't run unattended is marked ``<!-- not-run: reason -->``. The script fails if the number of
blocks it ran, set up or left out changes without this file being told, so an example can't drop
out of the check unnoticed.
"""

from __future__ import annotations

import os
import re
import subprocess
import sys
import tempfile
from pathlib import Path

import hardcharger

README = Path(__file__).resolve().parent.parent / "README.md"
ADDRESS_IN_EXAMPLES = "localhost:31337"
ADDRESS_IN_PARTS = 'host = "localhost"\nport = 31337'
USERNAME, PASSWORD = "hardcharger", "change-me"
EXPECTED_RUN = 9
EXPECTED_SETUP = 3
EXPECTED_NOT_RUN = 0

BLOCK = re.compile(
    r"(?P<marker><!-- (?P<kind>not-run|setup): (?P<detail>[^>]*?) -->\n)?"
    r"```(?P<language>python|toml|bash)\n(?P<code>.*?)```\n"
    r"(?:\n```text\n(?P<output>.*?)```\n)?",
    flags=re.DOTALL,
)
EXPORT = re.compile(r'^export (?P<name>[A-Z0-9_]+)="(?P<value>[^"]*)"$')


def _environment(directory: Path, extra: dict[str, str]) -> dict[str, str]:
    """The examples' environment: dlt's files in ``directory``, no settings from outside."""
    environment = {
        name: value
        for name, value in os.environ.items()
        if not name.startswith(("DESTINATION__", "SCHEMA__", "LOAD__", "NORMALIZE__"))
    }
    environment.update(
        {
            "DLT_PROJECT_DIR": str(directory),
            "DLT_DATA_DIR": str(directory / "data"),
            "RUNTIME__DLTHUB_TELEMETRY": "false",
            **extra,
        }
    )
    return environment


def main() -> int:
    text = README.read_text(encoding="utf-8")
    ran = set_up = not_run = failures = 0
    secrets: str | None = None
    exports: dict[str, str] = {}

    with hardcharger.Server(username=USERNAME, password=PASSWORD) as server:
        address = f"{server.host}:{server.port}"
        for block in BLOCK.finditer(text):
            line = text.count("\n", 0, block.start("code")) + 1
            code = block.group("code")
            kind, language = block.group("kind"), block.group("language")
            if kind == "not-run":
                not_run += 1
                continue
            if kind == "setup":
                set_up += 1
                if ADDRESS_IN_EXAMPLES not in code and ADDRESS_IN_PARTS not in code:
                    print(f"FAIL README.md:{line}: the setup doesn't name {ADDRESS_IN_EXAMPLES}")
                    failures += 1
                code = code.replace(ADDRESS_IN_EXAMPLES, address).replace(
                    ADDRESS_IN_PARTS, f'host = "{server.host}"\nport = {server.port}'
                )
                if language == "toml":
                    secrets = code
                else:
                    exports = {
                        match.group("name"): match.group("value")
                        for match in map(EXPORT.match, code.splitlines())
                        if match
                    }
                    if not exports:
                        print(f'FAIL README.md:{line}: no export NAME="value" line to set')
                        failures += 1
                continue
            if language != "python":
                continue  # shell commands for the reader (pip install, make)

            ran += 1
            configured = secrets is not None or bool(exports)
            if not configured and ADDRESS_IN_EXAMPLES not in code:
                print(
                    f"FAIL README.md:{line}: the example doesn't connect to {ADDRESS_IN_EXAMPLES}"
                )
                failures += 1
                continue
            with tempfile.TemporaryDirectory() as temporary:
                directory = Path(temporary)
                (directory / ".dlt").mkdir()
                if secrets is not None:
                    (directory / ".dlt" / "secrets.toml").write_text(secrets, encoding="utf-8")
                script = directory / f"example_{ran}.py"
                script.write_text(code.replace(ADDRESS_IN_EXAMPLES, address), encoding="utf-8")
                done = subprocess.run(
                    args=[sys.executable, str(script)],
                    capture_output=True,
                    text=True,
                    timeout=300,
                    cwd=directory,
                    env=_environment(directory=directory, extra=exports),
                )
            secrets, exports = None, {}
            expected = block.group("output")
            if done.returncode != 0:
                print(f"FAIL README.md:{line}: exit {done.returncode}\n{done.stdout}{done.stderr}")
                failures += 1
            elif expected is not None and done.stdout.strip() != expected.strip():
                print(
                    f"FAIL README.md:{line}: output differs\n--- README\n{expected.strip()}\n"
                    f"--- actual\n{done.stdout.strip()}"
                )
                failures += 1
            else:
                checked = "output matches" if expected is not None else "ran"
                print(f"ok   README.md:{line}: {checked}")

    counts = (ran, set_up, not_run)
    if counts != (EXPECTED_RUN, EXPECTED_SETUP, EXPECTED_NOT_RUN):
        print(
            f"FAIL README.md has {ran} runnable examples, {set_up} setup blocks and {not_run}"
            f" not-run examples; this script expects {EXPECTED_RUN}, {EXPECTED_SETUP} and"
            f" {EXPECTED_NOT_RUN}. Update the numbers at the top of this script with the README."
        )
        failures += 1
    print(f"{ran} examples run, {failures} failure(s)")
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())
