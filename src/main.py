import argparse
import json
import polars as pl
import trio

from duckdb import DuckDBPyConnection, connect
from loguru import logger
from pathlib import Path

from app import App
from channel.broker import _get_channel_broker
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
    parser.add_argument("--entrypoint", dest="entrypoint", default=None, required=False)
    args = parser.parse_args()
    entrypoint = Path(args.entrypoint) if args.entrypoint else None

    # For now we manage state with the backend
    # To be ultimately changed for obvious reasons
    backend_conn: DuckDBPyConnection = connect(database=":memory:")
    transform_conn: DuckDBPyConnection = backend_conn

    app = App(backend_conn, PROPERTIES_SCHEMA)
    client_manager = ClientManager(backend_conn)
    entity_manager = EntityManager(backend_conn)
    task_manager = TaskManager(backend_conn, transform_conn)
    scheduler = TrioScheduler()

    app.add_dependency(client_manager)
    app.add_dependency(entity_manager)
    entity_manager.add_dependency(task_manager)
    task_manager.add_dependency(scheduler)

    channel_broker = _get_channel_broker()

    async with trio.open_nursery() as nursery:
        nursery.start_soon(app.start, nursery)

        for sql in iter_sql_statements(entrypoint):
            await channel_broker.publish("client.sql.requests", (app.name, sql))

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
