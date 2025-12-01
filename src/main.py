import argparse
import json
import polars as pl
import trio

from duckdb import DuckDBPyConnection, connect
from loguru import logger
from pathlib import Path

from app import App
from entity.entity_manager import EntityManager
from scheduler.scheduler import TrioScheduler
from server import ClientManager
from sql.file import iter_sql_statements
from task.manager import TaskManager

PROPERTIES_SCHEMA = json.loads(
    open(Path("src/properties.schema.json"), "rb").read().decode("utf-8")
)


async def main():
    # TODO: expose as configuration available in SET
    pl.Config.set_fmt_str_lengths(900)
    parser = argparse.ArgumentParser("Run a SQL file")
    parser.add_argument("file")
    args = parser.parse_args()
    sql_filepath = Path(args.file)

    # For now we manage state with the backend
    # To be ultimately changed for obvious reasons
    backend_conn: DuckDBPyConnection = connect(database=":memory:")
    transform_conn: DuckDBPyConnection = backend_conn

    app = App(backend_conn, PROPERTIES_SCHEMA)

    # Connect ClientManager to App
    client_manager = ClientManager(backend_conn)
    app.add_dependency(client_manager)

    # Connect App to EntityManager
    entity_manager = EntityManager(backend_conn)
    app.add_dependency(entity_manager)
    app.connect_entity_manager(entity_manager)

    # Connect TaskManager to EntityManager
    task_manager = TaskManager(backend_conn, transform_conn)
    entity_manager.add_dependency(task_manager)
    entity_manager.connect_task_manager(task_manager)

    # Connect Scheduler to TaskManager
    scheduler = TrioScheduler()
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
            # Set App._stopped and trigger orderly shutdown
            await app.stop()

            # Now wait for all dependencies for full shut down
            await app._shutdown.wait()
            logger.success("All services shut down gracefully!")


if __name__ == "__main__":
    trio.run(main)
