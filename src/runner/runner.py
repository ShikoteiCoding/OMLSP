import trio

from commons.utils import Channel
from context.context_manager import ContextManager
from context.context import EvalContext, TaskContext
from sql.file.reader import iter_sql_statements

from apscheduler import AsyncScheduler
from duckdb import DuckDBPyConnection, connect
from loguru import logger
from task.task_manager import TaskManager


class Runner:
    def __init__(self, 
                 context_manager: ContextManager,
                 task_manager: TaskManager):
        self.context_manager = context_manager
        self.task_manager = task_manager
        
        self._sql_channel = Channel[str](10)
        self._evalctx_channel = Channel[EvalContext](10)
        self._taskctx_channel = Channel[TaskContext](10)
        self._nursery = None
        self._running = False
    
    async def build(self):
        await self.context_manager.add_sql_channel(self._sql_channel)
        await self.context_manager.add_evalctx_channel(self._evalctx_channel)
        await self.context_manager.add_taskctx_channel(self._taskctx_channel)
        await self.task_manager.add_taskctx_channel(self._taskctx_channel)
    
    # TODO: replace with terminal channel
    async def submit(self, sql: str) -> None:
        await self._sql_channel.send(sql)

    async def run(self):
        self._running = True
        async with trio.open_nursery() as nursery:
            self._nursery = nursery

            nursery.start_soon(self.context_manager.run)
            nursery.start_soon(self.task_manager.run)

            nursery.start_soon(self._watch_for_shutdown)
            logger.info("[Runner] Started and running...")
            await trio.sleep_forever()

        logger.info("[Runner] Stopped.")
        
    async def _watch_for_shutdown(self):
        try:
            while self._running:
                await trio.sleep(1)
        finally:
            logger.info("[Runner] Shutdown initiated.")
            self._running = False


#    async def run(self) -> None:
#        """Main entrypoint: start background consumers + DAG scheduler."""
#        async with trio.open_nursery() as nursery:
#            self._nursery = nursery
#            self.task_manager.appoint(nursery)
#            nursery.start_soon(self._sql_consummer)
#            nursery.start_soon(self._ctx_consummer)

    async def shutdown(self) -> None:
        """Stop the runner."""
        self._running = False
        if self._nursery is not None:
            self._nursery.cancel_scope.cancel()


if __name__ == "__main__":
    import json
    from pathlib import Path

    with open(Path("src/properties.schema.json"), "rb") as fo:
        properties_schema = json.loads(fo.read().decode("utf-8"))
    conn: DuckDBPyConnection = connect(database=":memory:")
    scheduler = AsyncScheduler()
    context_manager = ContextManager(properties_schema)
    task_manager = TaskManager(conn, scheduler)
    executors = {}
    runner = Runner(context_manager, task_manager)

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
            await runner.shutdown()
        
        logger.info(task_manager)

    trio.run(main)
