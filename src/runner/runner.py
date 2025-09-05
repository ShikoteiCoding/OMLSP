import trio

from typing import Any

from sql.file.reader import iter_sql_statements
from common.utils import Channel
from dag.dag_manager import DagManager
from context.context_manager import ContextManager
from context.context import InvalidContext, QueryContext

from loguru import logger


class Runner:
    def __init__(self, context_manager: ContextManager, dag_manager: DagManager, executors: dict[str, Any], properties_schema: dict[str, Any]):
        self.context_manager = context_manager
        self.dag_manager = dag_manager
        self.properties_schema = properties_schema
        self.executors = executors

        
        self._sql_channel = Channel[str](10) # Channels for SQL submissions
        self._ctx_channel = Channel[QueryContext](10) # Channels for Contexts
        self._nursery = None
        self._running = False
        
    async def submit(self, sql: str) -> None:
        await self._sql_channel.send(sql)

    async def _sql_consummer(self) -> None:
        """Consume SQL statements, parse, and update DAG dynamically."""
        async for sql in self._sql_channel:
            ctx = self.context_manager.parse(sql, self.properties_schema)

            if isinstance(ctx, InvalidContext):
                raise Exception(str(ctx.reason))

            await self._ctx_channel.send(ctx)
            logger.info(f"[Runner] Registered SQL: {sql}")

    async def _ctx_manager(self, nursery: trio.Nursery) -> None:
        """Continuously run tasks in the evolving DAG."""
        async for ctx in self._ctx_channel:
            self.dag_manager.update(ctx, nursery)

    async def run(self) -> None:
        """Main entrypoint: start background consumers + DAG scheduler."""
        self._running = True
        async with trio.open_nursery() as nursery:
            self._nursery = nursery
            # TODO: invert dependency
            # Makes CM & DM be long lived task 
            # with nursury management
            nursery.start_soon(self._sql_consummer)
            nursery.start_soon(self._ctx_manager, nursery)

    async def shutdown(self) -> None:
        """Stop the runner."""
        self._running = False
        if self._nursery is not None:
            self._nursery.cancel_scope.cancel()


if __name__ == "__main__":
    import json
    from pathlib import Path

    context_manager = ContextManager()
    dag_manager = DagManager()
    executors = {}
    with open(Path("src/properties.schema.json"), "rb") as fo:
        properties_schema = json.loads(fo.read().decode("utf-8"))
    runner = Runner(context_manager, dag_manager, executors, properties_schema)

    async def main():
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

    trio.run(main)
