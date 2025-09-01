import trio

from typing import Any

from sql.file.reader import read_sql_statements
from dag.channel import Channel
from dag.dag_manager import DagManager
from context.context_manager import ContextManager

from loguru import logger


class Runner:
    def __init__(self, context_manager: ContextManager, dag_manager: DagManager, executors: dict[str, Any]):
        self.context_manager = context_manager
        self.dag_manager = dag_manager
        self.executors = executors

        
        self._sql_channel = Channel(10) # Channels for SQL submissions
        self._ctx_channel = Channel(10) # Channels for Params
        self._nursery = None
        self._running = False
        
    async def submit(self, sql: str) -> None:
        await self._sql_channel.send(sql)

    async def _consume_sql(self) -> None:
        """Consume SQL statements, parse, and update DAG dynamically."""
        async for sql in self._sql_channel:
            ctx = self.context_manager.parse(sql)
            self.dag_manager.register(ctx)
            logger.info(f"[Runner] Registered SQL: {sql}")

    async def _schedule_dag(self) -> None:
        """Continuously run tasks in the evolving DAG."""
        while self._running:
            #await self.dag_manager.run_once(self.executors)
            await trio.sleep(0.1)  # prevent busy loop

    async def run(self) -> None:
        """Main entrypoint: start background consumers + DAG scheduler."""
        self._running = True
        async with trio.open_nursery() as nursery:
            self._nursery = nursery
            nursery.start_soon(self._consume_sql)
            nursery.start_soon(self._schedule_dag)

    async def shutdown(self) -> None:
        """Stop the runner."""
        self._running = False
        if self._nursery is not None:
            self._nursery.cancel_scope.cancel()


if __name__ == "__main__":
    context_manager = ContextManager()
    dag_manager = DagManager()
    executors = {}
    runner = Runner(context_manager, dag_manager, executors)

    async def main():
        # preload file SQLs
        for sql in read_sql_statements("examples/basic.sql"):
            await runner.submit(sql)
        logger.info("here")
        async with trio.open_nursery() as nursery:
            nursery.start_soon(runner.run)

            # simulate console input
            await trio.sleep(1)
            await runner.submit("SELECT * FROM my_table;")

            await trio.sleep(5)
            await runner.shutdown()

    trio.run(main)
