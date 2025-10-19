import argparse
import json
import polars as pl
import trio

from duckdb import DuckDBPyConnection, connect
from loguru import logger
from pathlib import Path

from app import App
from scheduler import TrioScheduler
from server import ClientManager
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

    registry_conn: DuckDBPyConnection = connect(database=":memory:")

    scheduler = TrioScheduler()
    task_manager = TaskManager(registry_conn)
    client_manager = ClientManager(registry_conn)
    app = App(registry_conn, PROPERTIES_SCHEMA)

    # TODO: Not the most elegant, baby steps
    # towards full actor model to polish

    # Connect ClientManager to App
    app.add_dependency(client_manager)
    app.connect_client_manager(client_manager)

    # Connect TaskManager to App
    app.add_dependency(task_manager)
    app.connect_task_manager(task_manager)

    # Connect Scheduler to TaskManager
    task_manager.add_dependency(scheduler)
    task_manager.connect_scheduler(scheduler)

    async with trio.open_nursery() as nursery:
        nursery.start_soon(app.start, nursery)
        for sql in iter_sql_statements(sql_filepath):
            await app.submit(sql)

        try:
            await app.wait_until_stopped()
        except KeyboardInterrupt:
            logger.warning(
                "KeyboardInterrupt received! Stopping services gracefully..."
            )
            # This will set `_stopped` and trigger orderly shutdown
            await app.stop()

            # Now wait for all dependencies to shut down
            await app._shutdown.wait()
            logger.success("All services shut down gracefully!")


if __name__ == "__main__":
    trio.run(main)
