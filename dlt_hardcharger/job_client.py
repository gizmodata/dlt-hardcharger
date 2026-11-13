"""The job client: dlt's SQLAlchemy client, with Parquet files bulk-loaded as Arrow."""

from __future__ import annotations

import time
from collections.abc import Sequence
from typing import TYPE_CHECKING, Any

import sqlalchemy as sa
from adbc_driver_manager import AdbcStatusCode
from adbc_driver_manager import Error as AdbcError
from dlt.common import logger
from dlt.common.destination.client import (
    FollowupJobRequest,
    HasFollowupJobs,
    LoadJob,
    RunnableLoadJob,
)
from dlt.common.destination.typing import PreparedTableSchema
from dlt.common.destination.utils import resolve_merge_strategy
from dlt.destinations.exceptions import (
    DatabaseTerminalException,
    DatabaseTransientException,
    DatabaseUndefinedRelation,
)
from dlt.destinations.impl.sqlalchemy.load_jobs import SqlalchemyParquetInsertJob
from dlt.destinations.impl.sqlalchemy.sqlalchemy_job_client import SqlalchemyJobClient
from dlt.destinations.sql_jobs import SqlMergeFollowupJob

from .capabilities import SQLALCHEMY_MERGE_STRATEGIES

if TYPE_CHECKING:
    import pyarrow as pa

# What a retry can't change: the data or the table is wrong, or the user may not write it.
_TERMINAL_STATUSES = frozenset(
    {
        AdbcStatusCode.INVALID_ARGUMENT,
        AdbcStatusCode.INVALID_DATA,
        AdbcStatusCode.INTEGRITY,
        AdbcStatusCode.ALREADY_EXISTS,
        AdbcStatusCode.NOT_IMPLEMENTED,
        AdbcStatusCode.UNAUTHENTICATED,
        AdbcStatusCode.UNAUTHORIZED,
    }
)


def ingest_exception(error: AdbcError) -> Exception:
    """The dlt exception for a failed bulk ingest, told by the driver's ADBC status.

    dlt runs a job again after a transient failure and fails it for good after a terminal one.
    A value that doesn't convert or a broken constraint is terminal; a dropped connection or a
    timeout is worth another try.
    """
    status = getattr(error, "status_code", None)
    if status == AdbcStatusCode.NOT_FOUND:
        return DatabaseUndefinedRelation(error)
    if status in _TERMINAL_STATUSES:
        return DatabaseTerminalException(error)
    return DatabaseTransientException(error)


class HardchargerArrowIngestJob(RunnableLoadJob, HasFollowupJobs):
    """Load one Parquet file with the Hardcharger ADBC driver's Arrow bulk ingest.

    dlt's SQLAlchemy destination reads a Parquet file back into Python rows and sends them as
    ``INSERT`` statements. The driver under the ``hardcharger://`` dialect appends Arrow batches
    to a table directly (``cursor.adbc_ingest``), over several connections at once. The ingest
    runs in a transaction of the job's own connection, as dlt's ``INSERT`` jobs do: the file is
    in the table whole, or not at all. Columns are matched to the table's by name, and a column
    the file lacks takes its default.
    """

    def __init__(self, file_path: str, table: sa.Table) -> None:
        super().__init__(file_path)
        self._job_client: HardchargerJobClient = None  # type: ignore[assignment]
        self.table = table

    def run(self) -> None:
        import pyarrow as pa
        import pyarrow.parquet as pq

        sql_client = self._job_client.sql_client
        # The table in the dataset this job loads into: the staging dataset for a merge.
        table = sql_client.to_dataset_table(table_obj=self.table)
        started = time.monotonic()
        with pq.ParquetFile(self._file_path) as parquet, sql_client.begin_transaction():
            reader = pa.RecordBatchReader.from_batches(
                schema=parquet.schema_arrow, batches=parquet.iter_batches()
            )
            try:
                rows = self._ingest(
                    connection=sql_client.native_connection, table=table, data=reader
                )
            except AdbcError as error:
                raise ingest_exception(error=error) from error
        logger.info(
            f"{rows} rows of {self._file_name} ingested into {table.schema}.{table.name}"
            f" in {time.monotonic() - started:.3f} s"
        )

    @staticmethod
    def _ingest(
        connection: sa.engine.Connection, table: sa.Table, data: pa.RecordBatchReader
    ) -> int:
        # The Hardcharger ADBC connection under SQLAlchemy's one: the dialect documents
        # connection.connection.driver_connection as the way to its Arrow interface.
        driver_connection: Any = connection.connection.driver_connection
        with driver_connection.cursor() as cursor:
            return int(
                cursor.adbc_ingest(
                    table_name=table.name,
                    data=data,
                    mode="append",
                    db_schema_name=table.schema,
                )
            )


class HardchargerJobClient(SqlalchemyJobClient):
    """dlt's SQLAlchemy job client, with Arrow ingest of Parquet files and MERGE strategies."""

    def _create_merge_followup_jobs(
        self, table_chain: Sequence[PreparedTableSchema]
    ) -> list[FollowupJobRequest]:
        root_table = table_chain[0]
        strategy = resolve_merge_strategy(
            {root_table["name"]: root_table},  # type: ignore[dict-item]
            root_table,  # type: ignore[arg-type]
            self.capabilities,
            is_prepared=True,
        )
        if strategy in SQLALCHEMY_MERGE_STRATEGIES:
            return super()._create_merge_followup_jobs(table_chain)
        # upsert, insert-only and cdc are built on MERGE INTO, which dlt's SQLAlchemy merge
        # job doesn't write. dlt's SQL merge job does: it is the job dlt's own DuckDB
        # destination runs, so Hardcharger gets the same DuckDB statements.
        return [SqlMergeFollowupJob.from_table_chain(table_chain, self.sql_client)]

    def create_load_job(
        self, table: PreparedTableSchema, file_path: str, load_id: str, restore: bool = False
    ) -> LoadJob:
        job = super().create_load_job(table, file_path, load_id, restore)
        if isinstance(job, SqlalchemyParquetInsertJob):
            # The base client made the job that sends the file's rows as INSERT statements.
            return HardchargerArrowIngestJob(file_path=file_path, table=job.table)
        return job
