"""The installed distribution: its version, its dlt entry point, its marker files."""

from __future__ import annotations

from importlib import metadata, resources

import dlt_hardcharger


def test_version_is_the_distribution_version() -> None:
    assert dlt_hardcharger.__version__ == metadata.version("dlt-hardcharger")


def test_dlt_entry_point() -> None:
    """dlt loads every ``dlt`` entry point and looks in ``<module>.destinations`` for names."""
    (entry_point,) = (
        entry_point
        for entry_point in metadata.distribution("dlt-hardcharger").entry_points
        if entry_point.group == "dlt"
    )
    assert entry_point.value == "dlt_hardcharger"
    module = entry_point.load()
    assert module is dlt_hardcharger
    assert module.destinations.hardcharger is dlt_hardcharger.hardcharger


def test_typed_marker_is_packaged() -> None:
    assert resources.files("dlt_hardcharger").joinpath("py.typed").is_file()
