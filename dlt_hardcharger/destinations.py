"""The ``hardcharger`` destination factory.

dlt resolves ``destination="hardcharger"`` to ``<plugin module>.destinations.hardcharger`` for
every installed package with a ``dlt`` entry point, which is how this module is found without
an import in the user's code.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from dlt.common.destination import Destination, DestinationCapabilitiesContext
from dlt.destinations.impl.sqlalchemy.factory import sqlalchemy as _sqlalchemy

from .capabilities import (
    MERGE_STRATEGIES,
    HardchargerTypeMapper,
    apply_hardcharger_capabilities,
)
from .configuration import HardchargerClientConfiguration, HardchargerCredentials

if TYPE_CHECKING:
    from sqlalchemy.engine import Engine

    from .job_client import HardchargerJobClient


class hardcharger(_sqlalchemy):  # noqa: N801 (dlt names destination factories in lower case)
    """dlt destination for Hardcharger, GizmoData's DuckDB 2.0 server."""

    spec = HardchargerClientConfiguration  # type: ignore[assignment]

    def _raw_capabilities(self) -> DestinationCapabilitiesContext:
        caps = super()._raw_capabilities()
        apply_hardcharger_capabilities(caps=caps)
        # With HardchargerJobClient, which runs the MERGE INTO strategies as well.
        caps.supported_merge_strategies = list(MERGE_STRATEGIES)  # type: ignore[arg-type]
        caps.type_mapper = HardchargerTypeMapper
        # Parquet files are bulk-loaded as Arrow (HardchargerArrowIngestJob); typed-jsonl files
        # are sent as INSERT statements, about seventy times slower.
        caps.preferred_loader_file_format = "parquet"
        return caps

    @property
    def client_class(self) -> type[HardchargerJobClient]:  # type: ignore[override]
        from .job_client import HardchargerJobClient

        return HardchargerJobClient

    def __init__(
        self,
        credentials: HardchargerCredentials | dict[str, Any] | str | Engine | None = None,
        create_unique_indexes: bool = False,
        create_primary_keys: bool = False,
        destination_name: str | None = None,
        environment: str | None = None,
        engine_kwargs: dict[str, Any] | None = None,
        **kwargs: Any,
    ) -> None:
        """Configure the Hardcharger destination of a pipeline.

        Arguments given here take precedence over dlt's other configuration sources
        (``secrets.toml``, environment variables).

        Args:
            credentials: A ``hardcharger://user:password@host[:port][/catalog][?option=value]``
                URL, its parts as a dict, or a SQLAlchemy ``Engine`` made from such a URL.
                Left out, dlt reads ``destination.hardcharger.credentials`` from its
                configuration.
            create_unique_indexes: Create ``UNIQUE`` constraints for columns hinted ``unique``.
            create_primary_keys: Create ``PRIMARY KEY`` constraints for ``primary_key`` columns.
            destination_name: The destination's name, which is also its configuration section
                (default ``hardcharger``).
            environment: The destination's environment.
            engine_kwargs: Keyword arguments for ``sqlalchemy.create_engine``.
            **kwargs: Other destination settings and capabilities.
        """
        # Destination.__init__, not the SQLAlchemy factory's: that one also passes its
        # deprecated engine_args, which would reach the configuration as an explicit None.
        Destination.__init__(
            self,
            credentials=credentials,
            create_unique_indexes=create_unique_indexes,
            create_primary_keys=create_primary_keys,
            destination_name=destination_name,
            environment=environment,
            engine_kwargs=engine_kwargs,
            **kwargs,
        )


hardcharger.register()

__all__ = ["hardcharger"]
