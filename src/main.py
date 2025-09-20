import argparse
import json
from pathlib import Path

import polars as pl
import trio
from duckdb import DuckDBPyConnection, connect

from runner import Runner
from server import ClientManager
from scheduler import TrioScheduler
from sql.file import iter_sql_statements
from task import TaskManager

PROPERTIES_SCHEMA = json.loads(
    open(Path("src/properties.schema.json"), "rb").read().decode("utf-8")
)


async def main():
    pl.Config.set_fmt_str_lengths(900)  # TODO: expose as configuration available in SET
    parser = argparse.ArgumentParser("Run a SQL file")
    parser.add_argument("file")
    args = parser.parse_args()
    sql_filepath = Path(args.file)

    conn: DuckDBPyConnection = connect(database=":memory:")
    scheduler = TrioScheduler()
    task_manager = TaskManager(conn, scheduler)
    client_manager = ClientManager(conn)
    runner = Runner(conn, PROPERTIES_SCHEMA, task_manager, client_manager)

    await runner.build()
    async with trio.open_nursery() as nursery:
        nursery.start_soon(runner.run)
        for sql in iter_sql_statements(sql_filepath):
            await runner.submit(sql)


if __name__ == "__main__":
    trio.run(main)
