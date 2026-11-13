"""What is stored never depends on a time zone: not the client's, not the server's.

A Hardcharger session works in the server process's time zone, and a value that reaches the
server without an offset where a zoned type is expected gets that zone's offset. So every
temporal column kind is loaded here from clients and into servers in several zones (one with a
half-hour offset), over both load paths, and must come out the same everywhere.

The first release candidate stored ``time`` columns hinted ``timezone=True`` as TIME WITH TIME
ZONE: 13:45-05:00 became 13:45+00 on a UTC server and 13:45+05:30 on one in Kolkata, and the
test for it passed only where it was written, on a machine whose offset was the value's.
"""

from __future__ import annotations

import json
from collections.abc import Iterator

import hardcharger
import pytest

from .conftest import PASSWORD, USERNAME, RunScript, server_url

ZONES = ["UTC", "America/New_York", "Asia/Kolkata"]
# (client zone, server zone): the same zone on both sides, then each side somewhere else.
PAIRS = [
    ("UTC", "UTC"),
    ("America/New_York", "America/New_York"),
    ("Asia/Kolkata", "Asia/Kolkata"),
    ("UTC", "Asia/Kolkata"),
    ("America/New_York", "UTC"),
    ("Asia/Kolkata", "America/New_York"),
]
# The client's offset from UTC on 2026-07-04, in seconds: proof that the script ran in its zone.
JULY_OFFSETS = {"UTC": 0, "America/New_York": -4 * 3600, "Asia/Kolkata": 5 * 3600 + 1800}


@pytest.fixture(scope="module")
def zoned_servers() -> Iterator[dict[str, hardcharger.Server]]:
    """One server per zone: the server's TZ is its sessions' time zone."""
    servers: dict[str, hardcharger.Server] = {}
    try:
        for zone in ZONES:
            server = hardcharger.Server(username=USERNAME, password=PASSWORD, env={"TZ": zone})
            server.start()
            servers[zone] = server
        yield servers
    finally:
        for server in servers.values():
            server.stop()


LOAD_AND_REPORT = """
import datetime
import json
import time

import dlt
import pyarrow as pa

import dlt_hardcharger

EST = datetime.timezone(datetime.timedelta(hours=-5))
UTC = datetime.timezone.utc
AWARE = datetime.datetime(2026, 7, 4, 10, 20, 30, 987654, tzinfo=EST)
NAIVE = datetime.datetime(2026, 7, 4, 10, 20, 30, 987654)
HINTS = {{
    "ts": {{"data_type": "timestamp"}},
    "ts_p3": {{"data_type": "timestamp", "precision": 3}},
    "ntz": {{"data_type": "timestamp", "timezone": False}},
    "ntz_p0": {{"data_type": "timestamp", "timezone": False, "precision": 0}},
    "ntz_p3": {{"data_type": "timestamp", "timezone": False, "precision": 3}},
    "ntz_p9": {{"data_type": "timestamp", "timezone": False, "precision": 9}},
    "t": {{"data_type": "time"}},
    "t_p3": {{"data_type": "time", "precision": 3}},
    "t_tz": {{"data_type": "time", "timezone": True}},
}}
columns, row = {{"d": {{"data_type": "date"}}}}, {{"id": 1, "d": datetime.date(2026, 7, 4)}}
for name, hint in HINTS.items():
    is_time = hint["data_type"] == "time"
    for kind, value in (("aware", AWARE), ("naive", NAIVE)):
        columns[f"{{name}}_{{kind}}"] = dict(hint)
        row[f"{{name}}_{{kind}}"] = value.timetz() if is_time else value
arrow = pa.table(
    {{
        "id": pa.array([1]),
        "a_naive": pa.array([NAIVE], type=pa.timestamp("us")),
        "a_utc": pa.array([AWARE], type=pa.timestamp("us", tz="UTC")),
        "a_kolkata": pa.array([AWARE], type=pa.timestamp("us", tz="Asia/Kolkata")),
        "a_time": pa.array([NAIVE.time()], type=pa.time64("us")),
        "a_date": pa.array([datetime.date(2026, 7, 4)], type=pa.date32()),
    }}
)


@dlt.resource(name="versions", write_disposition={{"disposition": "merge", "strategy": "scd2"}})
def versions(rows):
    yield rows


local = datetime.datetime(2026, 7, 4, 12).astimezone()
report = {{"client_offset": local.utcoffset().total_seconds(), "stored": {{}}}}
for file_format in ("parquet", "typed-jsonl"):
    dataset_name = "{name}_" + file_format.replace("-", "_")
    pipeline = dlt.pipeline(
        pipeline_name=dataset_name,
        destination=dlt_hardcharger.hardcharger(credentials="{url}"),
        dataset_name=dataset_name,
    )
    pipeline.run([row], table_name="hinted", columns=columns, loader_file_format=file_format)
    pipeline.run(arrow, table_name="arrow", loader_file_format=file_format)
    pipeline.run(versions([{{"id": 1, "v": "a"}}]), loader_file_format=file_format)
    pipeline.run(versions([{{"id": 1, "v": "b"}}]), loader_file_format=file_format)
    stored = {{}}
    with pipeline.sql_client() as client:
        report["session_zone"] = client.execute_sql("SELECT current_setting('TimeZone')")[0][0]
        for table in ("hinted", "arrow"):
            qualified = client.make_qualified_table_name(table)
            types = client.execute_sql(
                "SELECT column_name, data_type FROM duckdb_columns() WHERE schema_name = %s"
                " AND table_name = %s AND column_name NOT IN ('id', '_dlt_id', '_dlt_load_id')"
                " ORDER BY column_index",
                dataset_name,
                table,
            )
            # A zoned type as its UTC clock reading, every other type as its own text: neither
            # depends on the session's zone.
            expressions = [
                f"({{column}} AT TIME ZONE 'UTC')::VARCHAR"
                if data_type == "TIMESTAMP WITH TIME ZONE"
                else f"{{column}}::VARCHAR"
                for column, data_type in types
            ]
            values = client.execute_sql(f"SELECT {{', '.join(expressions)}} FROM {{qualified}}")[0]
            for (column, data_type), value in zip(types, values):
                stored[column] = [data_type, value]
        # dlt's own timestamps are instants too: all of them within minutes of the server's now.
        for table, column in (
            ("_dlt_loads", "inserted_at"),
            ("_dlt_version", "inserted_at"),
            ("_dlt_pipeline_state", "created_at"),
            ("versions", "_dlt_valid_from"),
        ):
            qualified = client.make_qualified_table_name(table)
            stored[f"{{table}}.{{column}} is now"] = client.execute_sql(
                f"SELECT bool_and(abs(epoch({{column}}) - epoch(now())) < 600) FROM {{qualified}}"
            )[0][0]
        qualified = client.make_qualified_table_name("versions")
        stored["scd2 history"] = [
            list(found)
            for found in client.execute_sql(
                f"SELECT v, _dlt_valid_to IS NULL, _dlt_valid_to > _dlt_valid_from"
                f" FROM {{qualified}} ORDER BY _dlt_valid_from"
            )
        ]
        # Read back as Python values through the SQL client ...
        qualified = client.make_qualified_table_name("hinted")
        found = client.execute_sql(f"SELECT ts_aware, ntz_naive, t_naive, d FROM {{qualified}}")[0]
        stored["read sql_client"] = [found[0] == AWARE, found[1] == NAIVE, *map(str, found[2:])]
    # ... and through dlt's dataset, as rows and as Arrow.
    relation = pipeline.dataset().hinted.select("ts_aware", "ntz_naive", "t_naive", "d")
    found = relation.fetchall()[0]
    stored["read dataset"] = [found[0] == AWARE, found[1] == NAIVE, *map(str, found[2:])]
    table = relation.arrow()
    stored["read arrow"] = [
        table.column("ts_aware")[0].as_py() == AWARE,
        table.column("ntz_naive")[0].as_py() == NAIVE,
    ]
    report["stored"][file_format] = stored
print("REPORT " + json.dumps(report))
"""

# The same in every zone and from both load paths. Sent: 2026-07-04 10:20:30.987654, once at
# -05:00 ("aware") and once without an offset ("naive"), and the time of day 10:20:30.987654
# likewise.
STORED = {
    "d": ["DATE", "2026-07-04"],
    # A zoned timestamp is an instant: -05:00 is 15:20 UTC. Without an offset dlt reads UTC.
    "ts_aware": ["TIMESTAMP WITH TIME ZONE", "2026-07-04 15:20:30.987654"],
    "ts_naive": ["TIMESTAMP WITH TIME ZONE", "2026-07-04 10:20:30.987654"],
    # Digits beyond the column's precision are cut, never rounded, on both paths.
    "ts_p3_aware": ["TIMESTAMP WITH TIME ZONE", "2026-07-04 15:20:30.987"],
    "ts_p3_naive": ["TIMESTAMP WITH TIME ZONE", "2026-07-04 10:20:30.987"],
    # Into a timestamp without a zone, dlt stores an instant's UTC clock reading.
    "ntz_aware": ["TIMESTAMP", "2026-07-04 15:20:30.987654"],
    "ntz_naive": ["TIMESTAMP", "2026-07-04 10:20:30.987654"],
    "ntz_p0_aware": ["TIMESTAMP_S", "2026-07-04 15:20:30"],
    "ntz_p0_naive": ["TIMESTAMP_S", "2026-07-04 10:20:30"],
    "ntz_p3_aware": ["TIMESTAMP_MS", "2026-07-04 15:20:30.987"],
    "ntz_p3_naive": ["TIMESTAMP_MS", "2026-07-04 10:20:30.987"],
    "ntz_p9_aware": ["TIMESTAMP_NS", "2026-07-04 15:20:30.987654"],
    "ntz_p9_naive": ["TIMESTAMP_NS", "2026-07-04 10:20:30.987654"],
    # A time of day has no zone, whatever the hint: the clock reading, its offset dropped.
    "t_aware": ["TIME", "10:20:30.987654"],
    "t_naive": ["TIME", "10:20:30.987654"],
    "t_p3_aware": ["TIME", "10:20:30.987"],
    "t_p3_naive": ["TIME", "10:20:30.987"],
    "t_tz_aware": ["TIME", "10:20:30.987654"],
    "t_tz_naive": ["TIME", "10:20:30.987654"],
    # Arrow input: a timestamp without a zone is read as UTC, one in any zone is its instant.
    "a_naive": ["TIMESTAMP WITH TIME ZONE", "2026-07-04 10:20:30.987654"],
    "a_utc": ["TIMESTAMP WITH TIME ZONE", "2026-07-04 15:20:30.987654"],
    "a_kolkata": ["TIMESTAMP WITH TIME ZONE", "2026-07-04 15:20:30.987654"],
    "a_time": ["TIME", "10:20:30.987654"],
    "a_date": ["DATE", "2026-07-04"],
    "_dlt_loads.inserted_at is now": True,
    "_dlt_version.inserted_at is now": True,
    "_dlt_pipeline_state.created_at is now": True,
    "versions._dlt_valid_from is now": True,
    "scd2 history": [["a", False, True], ["b", True, None]],
    "read sql_client": [True, True, "10:20:30.987654", "2026-07-04"],
    "read dataset": [True, True, "10:20:30.987654", "2026-07-04"],
    "read arrow": [True, True],
}


@pytest.mark.parametrize("zone", ZONES)
def test_a_servers_sessions_work_in_its_zone(
    zoned_servers: dict[str, hardcharger.Server], zone: str
) -> None:
    """The premise of this module: without it, every test below would prove nothing."""
    with zoned_servers[zone].connect() as connection, connection.cursor() as cursor:
        cursor.execute("SELECT current_setting('TimeZone'), TIME '12:00:00'::TIMETZ::VARCHAR")
        session_zone, attached = cursor.fetchone()
    assert session_zone == zone
    # What made stored values depend on the zone: an offset attached to a value without one.
    assert (
        attached
        == {
            "UTC": "12:00:00+00",
            "America/New_York": "12:00:00-05",
            "Asia/Kolkata": "12:00:00+05:30",
        }[zone]
    )


@pytest.mark.parametrize(("client_zone", "server_zone"), PAIRS)
def test_stored_values_are_the_same_in_every_zone_and_from_both_load_paths(
    zoned_servers: dict[str, hardcharger.Server],
    run_script: RunScript,
    name: str,
    client_zone: str,
    server_zone: str,
) -> None:
    url = server_url(server=zoned_servers[server_zone])
    done = run_script(code=LOAD_AND_REPORT.format(name=name, url=url), env={"TZ": client_zone})
    assert done.returncode == 0, done.stdout + done.stderr
    (line,) = (line for line in done.stdout.splitlines() if line.startswith("REPORT "))
    report = json.loads(line.removeprefix("REPORT "))
    # The script did run in its zone, against a session in the server's.
    assert report["client_offset"] == JULY_OFFSETS[client_zone]
    assert report["session_zone"] == server_zone
    assert report["stored"]["parquet"] == STORED
    assert report["stored"]["typed-jsonl"] == STORED
