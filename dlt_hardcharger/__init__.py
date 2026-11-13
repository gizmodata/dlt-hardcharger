"""dlt destination for Hardcharger, GizmoData's DuckDB 2.0 server.

    import dlt

    pipeline = dlt.pipeline(
        pipeline_name="events",
        destination="hardcharger",
        dataset_name="raw",
    )

Installing the package is enough: dlt finds the ``hardcharger`` destination through the
package's ``dlt`` entry point. Credentials come from dlt's configuration
(``destination.hardcharger.credentials``) or from ``dlt_hardcharger.hardcharger(credentials=...)``.
"""

from ._version import __version__
from .capabilities import HardchargerDialectCapabilities, HardchargerTypeMapper, register
from .configuration import HardchargerClientConfiguration, HardchargerCredentials
from .destinations import hardcharger

# A plain dlt.destinations.sqlalchemy given a hardcharger:// URL gets the same capabilities.
register()

__all__ = [
    "HardchargerClientConfiguration",
    "HardchargerCredentials",
    "HardchargerDialectCapabilities",
    "HardchargerTypeMapper",
    "__version__",
    "hardcharger",
]
