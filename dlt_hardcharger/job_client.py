"""The job client: dlt's SQLAlchemy client, with Parquet files bulk-loaded as Arrow."""

from __future__ import annotations

import time
from typing import TYPE_CHECKING, Any

import sqlalchemy as sa
from dlt.common import logger
from dlt.common.destination.client import HasFollowupJobs, LoadJob, RunnableLoadJob
from dlt.common.destination.typing import PreparedTableSchema
from dlt.destinations.impl.sqlalchemy.load_jobs import SqlalchemyParquetInsertJob
from dlt.destinations.impl.sqlalchemy.sqlalchemy_job_client import SqlalchemyJobClient

if TYPE_CHECKING:
    import pyarrow as pa


class HardchargerArrowIngestJob(RunnableLoadJob, HasFollowupJobs):
    """Load one Parquet file with the Hardcharger ADBC driver's Arrow bulk ingest.

    dlt's SQLAlchemy destination reads a Parquet file back into Python rows and sends them as
    ``INSERT`` statements. The driver under the ``hardcharger://`` dialect can append Arrow
    batches to a table directly (``cursor.adbc_ingest``), over several connections at once and
    committed as one: the file is in the table whole, or not at all. Columns are matched to the
    table's by name, and a column the file lacks takes its default.
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
        with pq.ParquetFile(self._file_path) as parquet:
            reader = pa.RecordBatchReader.from_batches(
                schema=parquet.schema_arrow, batches=parquet.iter_batches()
            )
            rows = self._ingest(connection=sql_client.native_connection, table=table, data=reader)
        logger.info(
            f"{rows} rows of {self._file_name} ingested into {table.schema}.{table.name}"
            f" in {time.monotonic() - started:.3f} s"
        )

    @staticmethod
    def _ingest(connection: sa.engine.Connection, table: sa.Table, data: pa.RecordBatchReader) -> int:
        # The Hardcharger ADBC connection under SQLAlchemy's (documented by the dialect).
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
    def create_load_job(
        self, table: PreparedTableSchema, file_path: str, load_id: str, restore: bool = False
    ) -> LoadJob:
        job = super().create_load_job(table, file_path, load_id, restore)
        if isinstance(job, SqlalchemyParquetInsertJob):
            return HardchargerArrowIngestJob(file_path=file_path, table=job.table)
        return job
