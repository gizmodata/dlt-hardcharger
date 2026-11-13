.PHONY: install lint format unit integration test readme all build clean

PYTHON ?= .venv/bin/python

# --python: without it uv installs into the shell's active virtualenv, not the one made here.
install:
	uv venv
	uv pip install --python .venv/bin/python -e ".[dev]"

lint:
	$(PYTHON) -m ruff check .
	$(PYTHON) -m ruff format --check .
	$(PYTHON) -m mypy dlt_hardcharger

format:
	$(PYTHON) -m ruff check --fix .
	$(PYTHON) -m ruff format .

unit:
	$(PYTHON) -m pytest tests/unit

# Starts a real Hardcharger server (the `hardcharger` package); fails if it can't.
integration:
	$(PYTHON) -m pytest tests/integration

test:
	$(PYTHON) -m pytest

# Every Python example in README.md, run as written against a real server.
readme:
	$(PYTHON) scripts/run_readme_examples.py

all: lint test readme

build:
	rm -rf dist
	$(PYTHON) -m build --sdist --wheel --outdir dist
	$(PYTHON) -m twine check --strict dist/*

clean:
	rm -rf dist build *.egg-info .pytest_cache .mypy_cache .ruff_cache
