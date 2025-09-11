import trio

from commons.utils import Channel
from context.context_manager import ContextManager
from context.context import EvalContext, TaskContext, InvalidContext
from server import ClientManager
from metadata import init_metadata_store
from sql.file.reader import iter_sql_statements
from task import TaskManager

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from duckdb import DuckDBPyConnection, connect
from loguru import logger


class Runner:
    def __init__(
        self,
        conn: DuckDBPyConnection,
        context_manager: ContextManager,
        task_manager: TaskManager,
        client_manager: ClientManager,
    ):
        self.conn = conn
        self.context_manager = context_manager
        self.task_manager = task_manager
        self.client_manager = client_manager

        self._sql_channel = Channel[tuple[str, str]](10)
        self._evalctx_channel = Channel[tuple[str, EvalContext | InvalidContext]](10)
        self._taskctx_channel = Channel[TaskContext](10)

        self._nursery = None
        self._running = False

    async def build(self):
        # SQL channel
        await self.context_manager.add_sql_channel(self._sql_channel)
        await self.client_manager.add_sql_channel(self._sql_channel)

        # Eval Ctx channel (to execute)
        await self.context_manager.add_evalctx_channel(self._evalctx_channel)
        await self.client_manager.add_evalctx_channel(self._evalctx_channel)

        # Task Ctx channel (to schedule)
        await self.context_manager.add_taskctx_channel(self._taskctx_channel)
        await self.task_manager.add_taskctx_channel(self._taskctx_channel)

    async def submit(self, sql: str) -> None:
        # main is main app client_id
        await self._sql_channel.send(("main", sql))

    async def run(self):
        init_metadata_store(self.conn)
        self._running = True
        async with trio.open_nursery() as nursery:
            self._nursery = nursery

            nursery.start_soon(self.context_manager.run)
            nursery.start_soon(self.task_manager.run)
            nursery.start_soon(self.client_manager.run)

            nursery.start_soon(self._watch_for_shutdown)
            logger.info("[Runner] Started.")
            await trio.sleep_forever()

        logger.info("[Runner] Stopped.")

    async def _watch_for_shutdown(self):
        try:
            while self._running:
                await trio.sleep(1)
        finally:
            logger.info("[Runner] Shutdown initiated.")
            self._running = False


if __name__ == "__main__":
    import json
    from pathlib import Path

    with open(Path("src/properties.schema.json"), "rb") as fo:
        properties_schema = json.loads(fo.read().decode("utf-8"))
    conn: DuckDBPyConnection = connect(database=":memory:")
    scheduler = AsyncIOScheduler()
    context_manager = ContextManager(conn, properties_schema)
    task_manager = TaskManager(conn, scheduler)
    client_manager = ClientManager(conn)
    executors = {}
    runner = Runner(conn, context_manager, task_manager, client_manager)

    async def main():
        await runner.build()
        # preload file SQLs
        for sql in iter_sql_statements("examples/basic.sql"):
            await runner.submit(sql)
        async with trio.open_nursery() as nursery:
            nursery.start_soon(runner.run)

            # simulate console input
            await trio.sleep(1)
            await runner.submit("SELECT * FROM my_table;")

            await trio.sleep(5)

        logger.info(task_manager)

    trio.run(main)
