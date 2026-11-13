# Changelog

All notable changes to this project are documented here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and the project follows
[Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [1.0.0] - 2026-11-13

### Added

- The `hardcharger` destination for [dlt](https://dlthub.com/docs), loading into Hardcharger™,
  GizmoData's DuckDB 2.0 server, through the `hardcharger://` SQLAlchemy dialect
  (`sqlalchemy-hardcharger>=1.1.0`) and the Hardcharger ADBC driver over Chargewire HTTP(S).
  dlt 1.32.0 or later (below 2), Python 3.10 to 3.14.
- Found by name with no import: `dlt.pipeline(destination="hardcharger")` works once the package
  is installed (a `dlt` entry point). `dlt_hardcharger.hardcharger(credentials=...)` and
  `destination="dlt_hardcharger.hardcharger"` name it explicitly. Credentials come from the
  factory, from `secrets.toml` (`[destination.hardcharger]`, as a URL or in parts) or from the
  environment (`DESTINATION__HARDCHARGER__CREDENTIALS`); a SQLAlchemy `Engine` is accepted too.
- Arrow bulk ingest: Parquet is the destination's default file format, and each file is appended
  to its table with the driver's `adbc_ingest`, in one transaction, columns matched by name. One
  million rows of eight columns load in about 0.2 s on a laptop against a local server; the same
  rows as `INSERT` statements take about 140 s. `loader_file_format="typed-jsonl"` selects the
  `INSERT` path.
- Every write disposition: `append`; `replace` with `truncate-and-insert`, `insert-from-staging`
  and `staging-optimized` (which runs as `insert-from-staging`); `merge` with `delete-insert`,
  `scd2`, and the `MERGE INTO` strategies `upsert`, `insert-only` and `cdc`. Primary and merge
  keys, hard deletes, deduplication, nested tables, and optional `PRIMARY KEY` and `UNIQUE`
  constraints (`create_primary_keys`, `create_unique_indexes`).
- Type mapping to DuckDB types: `bigint` precision to `TINYINT`…`HUGEINT`, timestamps with a zone
  to `TIMESTAMP WITH TIME ZONE` and without one to `TIMESTAMP_S`, `TIMESTAMP_MS`, `TIMESTAMP` or
  `TIMESTAMP_NS` by precision, `time` to `TIME`, `json` to `JSON`, `wei` to `DECIMAL(38,0)`.
- Nothing stored depends on a time zone, the client's or the server's, and both load paths store
  the same value. A `time` column is always `TIME` (its `timezone` hint has no effect: Parquet
  and Arrow can't carry a time's offset, and the server would attach its session's): a value's
  clock reading is stored, its offset dropped. Fractional seconds beyond a timestamp or time
  column's `precision` are cut, not rounded, by `INSERT` statements as by Parquet files. Tested
  from clients and into servers in UTC, New York and Kolkata.
- The catalog is the URL's path (`hardcharger://host/lake`), the dataset a schema in it; a URL
  without a catalog works in the server's default one.
- Capabilities that match the server: DuckDB SQL for `pipeline.dataset()` reads (sqlglot dialect
  `duckdb`), case-insensitive identifiers (a schema with `Name` and `name` is refused by dlt
  before any DDL), a missing column is never taken for a missing table, and a failed bulk load
  is retried only when a retry can help (a value out of range or a broken constraint fails at
  once).
- dlt's built-in `sqlalchemy` destination, given a `hardcharger://` URL, gets the same type
  mapping, identifier rules and read dialect once this package is installed.
- Tests against a real Hardcharger server: unit and integration tests on every supported Python,
  and every README example run as written.
