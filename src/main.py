import argparse
import json
import trio
import polars as pl

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from duckdb import connect, DuckDBPyConnection
from pathlib import Path

from server import ClientManager
from task import TaskManager
from runner import Runner
from sql.file import iter_sql_statements


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
    scheduler = AsyncIOScheduler()
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
