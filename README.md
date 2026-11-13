<p align="center">
  <a href="https://github.com/gizmodata/hardcharger">
    <picture>
      <source media="(prefers-color-scheme: dark)" srcset="https://raw.githubusercontent.com/gizmodata/dlt-hardcharger/main/docs/assets/hardcharger-banner-dark.png">
      <img src="https://raw.githubusercontent.com/gizmodata/dlt-hardcharger/main/docs/assets/hardcharger-banner-light.png" alt="Hardcharger™" width="400">
    </picture>
  </a>
</p>

<h1 align="center">Hardcharger™ dlt Destination</h1>

<p align="center">
  <a href="https://github.com/gizmodata/dlt-hardcharger/actions/workflows/ci.yml"><img src="https://github.com/gizmodata/dlt-hardcharger/actions/workflows/ci.yml/badge.svg" alt="CI: full suite on Python 3.10–3.14"></a>
  <a href="https://pypi.org/project/dlt-hardcharger/"><img src="https://img.shields.io/pypi/v/dlt-hardcharger?logo=pypi" alt="PyPI version"></a>
  <a href="https://pypi.org/project/dlt-hardcharger/"><img src="https://img.shields.io/badge/Python-3.10–3.14-3776AB?logo=python&amp;logoColor=white" alt="Python 3.10–3.14"></a>
  <a href="https://pypi.org/project/dlt-hardcharger/"><img src="https://img.shields.io/pepy/dt/dlt-hardcharger.svg" alt="PyPI downloads"></a>
  <a href="https://dlthub.com/docs"><img src="https://img.shields.io/badge/dlt-1.32+-59C1D5.svg" alt="dlt 1.32 and later"></a>
  <a href="https://github.com/gizmodata/sqlalchemy-hardcharger"><img src="https://img.shields.io/badge/dialect-Hardcharger%20SQLAlchemy-f15a24.svg" alt="Hardcharger SQLAlchemy dialect"></a>
  <a href="https://github.com/gizmodata/hardcharger-adbc"><img src="https://img.shields.io/badge/driver-Hardcharger%20ADBC-f15a24.svg" alt="Hardcharger ADBC driver"></a>
  <a href="https://github.com/gizmodata/chargewire"><img src="https://img.shields.io/badge/protocol-Chargewire-31a8db.svg" alt="Chargewire protocol"></a>
  <a href="https://github.com/gizmodata/hardcharger"><img src="https://img.shields.io/badge/server-Hardcharger-f15a24.svg" alt="Hardcharger"></a>
  <a href="https://github.com/gizmodata/dlt-hardcharger/blob/main/LICENSE"><img src="https://img.shields.io/badge/license-Apache%202.0-brightgreen.svg" alt="Apache 2.0 license"></a>
  <a href="https://gizmodata.com"><img src="https://img.shields.io/badge/by-GizmoData%E2%84%A2-f15a24.svg" alt="GizmoData™"></a>
</p>

<h3 align="center">Load anything dlt can extract into Hardcharger: remote DuckDB 2.0, over Chargewire, as Arrow.</h3>

---

`dlt-hardcharger` is the [dlt](https://dlthub.com/docs) (data load tool) destination for
[Hardcharger™](https://gizmodata.com/hardcharger), GizmoData's DuckDB 2.0 server. Install it and
`destination="hardcharger"` works in any dlt pipeline: every write disposition, schema evolution,
nested tables, state kept at the destination, and `pipeline.dataset()` to read the data back.

Data files are bulk-loaded as **Arrow** through the
[Hardcharger ADBC driver](https://github.com/gizmodata/hardcharger-adbc) over **Chargewire HTTP(S)**:
a million rows land in a fraction of a second, where `INSERT` statements take minutes
([measured below](#speed)).

## Install

```bash
pip install dlt-hardcharger
```

Python **3.10 to 3.14** (dlt's range). Installs `dlt[sqlalchemy,parquet]>=1.32.0` and
[`sqlalchemy-hardcharger`](https://github.com/gizmodata/sqlalchemy-hardcharger)`>=1.1.0`, which
brings the driver.

## Quick start

```python
import dlt
import dlt_hardcharger

pipeline = dlt.pipeline(
    pipeline_name="quick_start",
    destination=dlt_hardcharger.hardcharger(
        credentials="hardcharger://hardcharger:change-me@localhost:31337?tls=false"
    ),
    dataset_name="weather",
)

readings = [
    {"city": "Lima", "temp_c": 19.0, "hours": [{"hour": 6, "temp_c": 17.5}]},
    {"city": "Pune", "temp_c": 31.5, "hours": [{"hour": 6, "temp_c": 24.0}]},
]
info = pipeline.run(readings, table_name="readings")
print(info.has_failed_jobs)

dataset = pipeline.dataset()
print(dataset.readings.select("city", "temp_c").order_by("city").fetchall())
print(dataset.readings__hours.select("hour", "temp_c").order_by("temp_c").fetchall())
```

```text
False
[('Lima', 19.0), ('Pune', 31.5)]
[(6, 17.5), (6, 24.0)]
```

The dataset is a schema on the server (`weather`), each resource a table in it, and each nested
list a child table (`readings__hours`). TLS is **on** unless the URL says `?tls=false` (a local
development server). The port defaults to 31337, and to 443 for a
[GizmoData Cloud](https://gizmodata.com/cloud) host.

## Credentials

The URL is the one
[`sqlalchemy-hardcharger`](https://github.com/gizmodata/sqlalchemy-hardcharger#the-url) takes:

```
hardcharger://user:password@host[:port][/catalog][?option=value&...]
```

Give it to the destination in code, as above, or let dlt find it in its
[configuration](https://dlthub.com/docs/general-usage/credentials/): then the pipeline names the
destination and nothing else, and the package needs no import. dlt finds the destination through
the package's `dlt` entry point.

In `.dlt/secrets.toml`:

<!-- setup: .dlt/secrets.toml -->
```toml
[destination.hardcharger]
credentials = "hardcharger://hardcharger:change-me@localhost:31337?tls=false"
```

```python
import dlt

pipeline = dlt.pipeline(pipeline_name="from_secrets", destination="hardcharger", dataset_name="crm")
pipeline.run([{"id": 1, "name": "Ada"}], table_name="contacts")
print(pipeline.dataset().contacts.select("id", "name").fetchall())
```

```text
[(1, 'Ada')]
```

The URL can also be given in parts (`query` holds the URL's options):

<!-- setup: .dlt/secrets.toml -->
```toml
[destination.hardcharger.credentials]
host = "localhost"
port = 31337
username = "hardcharger"
password = "change-me"
database = "memory"  # the catalog; leave it out for the server's default

[destination.hardcharger.credentials.query]
tls = "false"
```

```python
import dlt

pipeline = dlt.pipeline(
    pipeline_name="from_parts", destination="hardcharger", dataset_name="crm_parts"
)
pipeline.run([{"id": 2, "name": "Grace"}], table_name="contacts")
print(pipeline.dataset().contacts.select("id", "name").fetchall())
```

```text
[(2, 'Grace')]
```

Or in the environment:

<!-- setup: environment -->
```bash
export DESTINATION__HARDCHARGER__CREDENTIALS="hardcharger://hardcharger:change-me@localhost:31337?tls=false"
```

```python
import dlt

pipeline = dlt.pipeline(pipeline_name="from_env", destination="hardcharger", dataset_name="crm_env")
pipeline.run([{"id": 3, "name": "Edsger"}], table_name="contacts")
print(pipeline.dataset().contacts.select("id", "name").fetchall())
```

```text
[(3, 'Edsger')]
```

`dlt_hardcharger.hardcharger(...)` also takes a SQLAlchemy `Engine` made from such a URL,
`engine_kwargs` for `sqlalchemy.create_engine`, and `destination_name="warehouse"` to read
`destination.warehouse.credentials` instead. Token and browser (SSO) sign-in, client certificates
and the other URL options are the dialect's: see
[its README](https://github.com/gizmodata/sqlalchemy-hardcharger#the-url).

## Write dispositions

All three, with every strategy dlt has for them:

| Disposition | Strategies | On Hardcharger |
| --- | --- | --- |
| `append` | | Arrow bulk ingest into the table |
| `replace` | `truncate-and-insert` (default) | Empties the table, then loads into it |
| | `insert-from-staging` | Loads into the staging dataset, then swaps the rows in one transaction |
| | `staging-optimized` | The same as `insert-from-staging` (DuckDB has no faster table swap) |
| `merge` | `delete-insert` (default) | By `primary_key` and/or `merge_key`, with `hard_delete` and `dedup_sort` |
| | `upsert` | One `MERGE INTO` by `primary_key`, with `hard_delete` |
| | `insert-only` | `MERGE INTO` that only inserts keys not yet there |
| | `cdc` | An upsert that also deletes what the load no longer has |
| | `scd2` | Slowly changing dimension: versions with `_dlt_valid_from` / `_dlt_valid_to` |

```python
import dlt
import dlt_hardcharger

pipeline = dlt.pipeline(
    pipeline_name="merge_example",
    destination=dlt_hardcharger.hardcharger(
        credentials="hardcharger://hardcharger:change-me@localhost:31337?tls=false"
    ),
    dataset_name="shop",
)


@dlt.resource(primary_key="id", write_disposition={"disposition": "merge", "strategy": "upsert"})
def orders(rows):
    yield rows


pipeline.run(orders([{"id": 1, "status": "new"}, {"id": 2, "status": "new"}]))
pipeline.run(orders([{"id": 2, "status": "shipped"}, {"id": 3, "status": "new"}]))
print(pipeline.dataset().orders.select("id", "status").order_by("id").fetchall())
```

```text
[(1, 'new'), (2, 'shipped'), (3, 'new')]
```

Merges and the staged replaces load into a staging dataset first (`shop_staging`, a second
schema in the same catalog) and move the rows in one transaction.

`create_primary_keys=True` and `create_unique_indexes=True` make dlt create `PRIMARY KEY` and
`UNIQUE` constraints from its hints. They are off by default, as on dlt's other destinations: a
constraint slows loading and dlt's merges don't need one.

## Speed

A Parquet file is appended to its table with the driver's Arrow bulk ingest
(`cursor.adbc_ingest`): Arrow batches sent over several connections at once, inside the load
job's transaction, so a file is in the table whole or not at all. Parquet is this destination's
default file format. Asked for `loader_file_format="typed-jsonl"`, it sends `INSERT` statements,
as dlt's SQLAlchemy destination does for every database.

One million rows of eight columns (integers, a float, two strings, a timestamp, a boolean), from
a laptop (Apple M-series) into a Hardcharger 2.12.0 server on the same machine, dlt 1.32.0:

| Source | File format | Load step | Whole run |
| --- | --- | --- | --- |
| Arrow table | Parquet → Arrow ingest | 0.14–0.20 s | 0.3 s |
| Python dicts | Parquet → Arrow ingest | 0.19 s | 12.9 s |
| Python dicts | typed-jsonl → `INSERT` statements | 143 s | 158 s |

The load step is the part this package decides. With Python dicts, dlt's own extract and
normalize steps take the other twelve seconds; hand dlt Arrow tables or pandas frames where a
source has them and they pass through untouched.

```python
import dlt
import dlt_hardcharger
import pyarrow as pa

pipeline = dlt.pipeline(
    pipeline_name="arrow_example",
    destination=dlt_hardcharger.hardcharger(
        credentials="hardcharger://hardcharger:change-me@localhost:31337?tls=false"
    ),
    dataset_name="metrics",
)

rows = 500_000
table = pa.table({"id": pa.array(range(rows)), "squared": pa.array(i * i for i in range(rows))})
pipeline.run(table, table_name="squares")
print(pipeline.dataset().squares.select("squared").max().fetchscalar())
```

```text
249999000001
```

## Catalogs

The catalog is the URL's path and the dataset a schema in it: `hardcharger://host/lake` with
`dataset_name="raw"` loads into `lake.raw`. Without a path, the pipeline works in the server's
default catalog. dlt's own tables and the staging dataset are in the same catalog.

```python
import dlt
import dlt_hardcharger
from sqlalchemy import create_engine, text

# For the example, a second catalog on the server: an attached DuckLake or database file in real use.
engine = create_engine("hardcharger://hardcharger:change-me@localhost:31337?tls=false")
with engine.connect() as conn:
    conn.execute(text("ATTACH IF NOT EXISTS ':memory:' AS lake"))
    conn.commit()

pipeline = dlt.pipeline(
    pipeline_name="catalog_example",
    destination=dlt_hardcharger.hardcharger(
        credentials="hardcharger://hardcharger:change-me@localhost:31337/lake?tls=false"
    ),
    dataset_name="raw",
)
pipeline.run([{"id": 1}, {"id": 2}], table_name="events")

with engine.connect() as conn:
    print(conn.execute(text("SELECT count(*) FROM lake.raw.events")).scalar())
```

```text
2
```

## Data types

| dlt type | Hardcharger type |
| --- | --- |
| `text` | `VARCHAR` |
| `double` | `DOUBLE` |
| `bool` | `BOOLEAN` |
| `bigint` | `BIGINT`; with `precision` 8, 16, 32, 64 or 128 bits: `TINYINT`, `SMALLINT`, `INTEGER`, `BIGINT`, `HUGEINT` |
| `decimal` | `DECIMAL(38,9)`, or the column's `precision` and `scale` |
| `wei` | `DECIMAL(38,0)` |
| `timestamp` | `TIMESTAMP WITH TIME ZONE` (microseconds): an instant. A value without an offset is read as UTC |
| `timestamp` with `timezone=False` | `TIMESTAMP`; with `precision` 0, 3 or 9: `TIMESTAMP_S`, `TIMESTAMP_MS`, `TIMESTAMP_NS`. A value with an offset is stored as its UTC clock reading |
| `date` | `DATE` |
| `time` | `TIME`, whatever the `timezone` hint: a value's clock reading is stored and its UTC offset dropped (`13:45-05:00` is `13:45`) |
| `binary` | `BLOB` |
| `json` | `JSON` |

Each is tested both ways (Arrow ingest and `INSERT` statements): the value read back is the value
sent, and both ways store the same value.

**Nothing stored depends on a time zone**, the client's or the server's. A Hardcharger session
works in the server's time zone, and DuckDB gives a value that arrives without an offset that
zone's offset wherever a zoned type is expected. The destination never lets that happen:

- A `timestamp` is always sent as an instant, so it is the same instant on every server. Read it
  back and you get that instant; as text (`::VARCHAR`), DuckDB prints it in the session's zone.
- A `time` is never `TIME WITH TIME ZONE`. Parquet and Arrow have no time-of-day type with an
  offset, so the offset of a value is gone before a file reaches the server, and the server would
  attach its own: the same pipeline would store `13:45-05` on one server and `13:45+00` on
  another. dlt's `timezone` hint therefore has no effect on a `time` column, as on dlt's own
  DuckDB destination. If the offset matters, store an instant (`timestamp`) or keep the offset in
  a column of its own.
- Fractional seconds beyond a column's `precision` are cut, never rounded, by both load paths:
  `10:20:30.987654` in a `precision=0` column is `10:20:30`.

The test suite loads every kind of timestamp, time and date from clients and into servers in UTC,
New York and Kolkata (a half-hour offset), in the same zone and in different ones, over both load
paths, and requires the same stored values every time.

## Reading the data

`pipeline.dataset()` gives dlt's relations (`.fetchall()`, `.arrow()`, `.df()`), and takes
DuckDB SQL: dlt parses it with sqlglot's DuckDB dialect. Table names in it need no schema.

```python
import dlt
import dlt_hardcharger

pipeline = dlt.pipeline(
    pipeline_name="reading_example",
    destination=dlt_hardcharger.hardcharger(
        credentials="hardcharger://hardcharger:change-me@localhost:31337?tls=false"
    ),
    dataset_name="trips",
)
pipeline.run(
    [
        {"city": "Lima", "km": 4.5},
        {"city": "Lima", "km": 12.0},
        {"city": "Pune", "km": 7.25},
    ],
    table_name="rides",
)

dataset = pipeline.dataset()
print(
    dataset(
        "SELECT city, sum(km) AS km, count(*) AS rides FROM rides GROUP BY ALL ORDER BY city"
    ).fetchall()
)
print(dataset.rides.select("city", "km").order_by("km").arrow().to_pydict())

with pipeline.sql_client() as client:
    rides = client.make_qualified_table_name("rides")
    print(client.execute_sql(f"SELECT max(km) FROM {rides} WHERE city = %s", "Lima"))
```

```text
[('Lima', 16.5, 2), ('Pune', 7.25, 1)]
{'city': ['Lima', 'Pune', 'Lima'], 'km': [4.5, 7.25, 12.0]}
[(12.0,)]
```

With `pipeline.sql_client()`, qualify table names (`client.make_qualified_table_name("rides")`):
the client doesn't change the session's schema.

## With dlt's own SQLAlchemy destination

Installing this package also teaches dlt's built-in `sqlalchemy` destination about Hardcharger,
for a pipeline that is already written against it. The URL then needs its catalog:

```python
import dlt

pipeline = dlt.pipeline(
    pipeline_name="plain_sqlalchemy",
    destination=dlt.destinations.sqlalchemy(
        credentials="hardcharger://hardcharger:change-me@localhost:31337/memory?tls=false"
    ),
    dataset_name="plain",
)
pipeline.run([{"id": 1}], table_name="numbers")
print(pipeline.dataset().numbers.select("id").fetchall())
```

```text
[(1,)]
```

It gets the same type mapping, identifier rules and DuckDB SQL for reads. It does not get the
Arrow bulk ingest (rows go as `INSERT` statements) or the `MERGE INTO` strategies (`upsert`,
`insert-only`, `cdc`): those are the `hardcharger` destination's.

## Good to know

- **Names are case-insensitive.** DuckDB keeps an identifier's case and compares without it. dlt's
  default naming makes everything `snake_case`, so nothing collides. With a case-sensitive naming
  convention (`sql_cs_v1`) the case is kept on the server, and a table with both `Name` and `name`
  is refused by dlt before anything is created ("Destination is case insensitive").
- **dlt's files are loaded one transaction each**, twenty at a time by default (`load.workers`),
  each on its own connection: the destination lets SQLAlchemy's pool open as many as dlt asks for
  (`max_overflow=-1`; pass `engine_kwargs` to choose otherwise). A failed file leaves nothing behind. A value the table can't hold (a number out of range, a
  broken constraint) fails its job at once; a dropped connection is retried.
- **Unsigned Arrow integers** take the signed type of the same width (dlt's mapping): a `uint32`
  column is an `INTEGER`, and a value above 2,147,483,647 in it is refused. Cast such a column to
  the next wider type before loading.
- **`scd2` and `create_primary_keys=True`** don't go together when the key is the natural key:
  scd2 keeps several versions of a key, which a `PRIMARY KEY` on it forbids.
- **One transaction writes to one catalog** in DuckDB. A pipeline works in one catalog (tables,
  dlt's own tables and the staging dataset), so loads are not affected.
- **dlt 1.32.0 or later, below 2.** The destination builds on dlt's SQLAlchemy destination; each
  release is tested against the newest dlt and against 1.32.0.

## Development and testing

```bash
make install         # a virtualenv in .venv with the package and its test tools
make lint            # ruff and mypy
make test            # unit and integration tests
make readme          # every example in this file, run as written
```

The integration tests and the README examples run against a real Hardcharger server: the
[`hardcharger`](https://pypi.org/project/hardcharger/) package downloads the server binary and
starts it on a free port. A test fails if the server can't start; none is skipped.

## License

Apache License 2.0. See [LICENSE](LICENSE) and [NOTICE](NOTICE).

Hardcharger™ and GizmoData™ are trademarks of GizmoData LLC. dlt is a project of dltHub Inc.; this
package is not affiliated with or endorsed by dltHub.
