"""Configuration of the ``hardcharger`` destination: where the server is and how to sign in."""

from __future__ import annotations

import dataclasses
from typing import TYPE_CHECKING, Final

from dlt.common.configuration import configspec
from dlt.common.configuration.exceptions import ConfigurationValueError
from dlt.destinations.impl.sqlalchemy.configuration import (
    SqlalchemyClientConfiguration,
    SqlalchemyCredentials,
)

from .capabilities import DIALECT_NAME

if TYPE_CHECKING:
    from sqlalchemy.engine import Dialect


@configspec(init=False)
class HardchargerCredentials(SqlalchemyCredentials):
    """A ``hardcharger://user:password@host[:port][/catalog][?option=value]`` URL, or its parts.

    The catalog is ``database``; dlt's dataset is a schema in it. Without one, the pipeline
    writes to the server's default catalog. URL options (``tls=false``, ``token=...``) are
    ``query``.
    """

    drivername: str = dataclasses.field(default=DIALECT_NAME, init=False, repr=False, compare=False)

    def get_dialect_class(self) -> type[Dialect]:
        # Known without an engine: dlt asks for capabilities before it has credentials, and an
        # engine can't be made from a URL that has no host yet.
        if self.is_external_engine:
            return type(self.engine.dialect)
        from sqlalchemy_hardcharger import HardchargerDialect

        return HardchargerDialect

    def on_resolved(self) -> None:
        super().on_resolved()
        backend = self.get_backend_name()
        if backend != DIALECT_NAME:
            raise ConfigurationValueError(
                f"The hardcharger destination takes a hardcharger:// URL, not `{backend}://`."
                " Use dlt.destinations.sqlalchemy for other databases."
            )
        if not self.host and "profile" not in (self.query or {}):
            raise ConfigurationValueError(
                "The hardcharger destination has no server to connect to. Give it a"
                " hardcharger://user:password@host[:port][/catalog] URL: as"
                " credentials=... to dlt_hardcharger.hardcharger(), in the environment variable"
                " DESTINATION__HARDCHARGER__CREDENTIALS, or as credentials under"
                " [destination.hardcharger] in .dlt/secrets.toml."
            )


@configspec
class HardchargerClientConfiguration(SqlalchemyClientConfiguration):
    destination_type: Final[str] = dataclasses.field(  # type: ignore[misc]
        default=DIALECT_NAME, init=False, repr=False, compare=False
    )
    credentials: HardchargerCredentials = None

    def get_dialect_class(self) -> type[Dialect]:
        # dlt asks for capabilities before the credentials are resolved (they may still be the
        # URL text, or absent): the dialect is known either way.
        if isinstance(self.credentials, HardchargerCredentials):
            return self.credentials.get_dialect_class()
        from sqlalchemy_hardcharger import HardchargerDialect

        return HardchargerDialect

    def get_backend_name(self) -> str:
        return DIALECT_NAME

    def data_location(self) -> str:
        """The server, and the catalog when the URL names one.

        dlt's SQLAlchemy destination asks an unknown dialect's URL for a database and fails
        the run's trace without one. A Hardcharger URL without a catalog is complete: the
        session works in the server's default catalog.
        """
        server = self._server_location()
        if not self.credentials or not server:
            self._no_data_location("the configuration has no host")
        location = f"{DIALECT_NAME}://{server}"
        if self.credentials.database:
            location = f"{location}/{self.credentials.database}"
        return location
