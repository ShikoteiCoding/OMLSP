import trio

from commons.utils import Channel
from context.context import (
    EvalContext,
    QueryContext,
    TaskContext,
    InvalidContext,
)
from sql.sqlparser.parser import extract_one_query_context
from typing import Any

from loguru import logger


class ContextManager:
    _running = False
    _sql_channel: Channel[tuple[str, str]]
    _evalctx_channel: Channel[tuple[str, EvalContext | InvalidContext]]
    _taskctx_channel: Channel[TaskContext]

    def __init__(self, properties_schema: dict[str, Any]):
        self.properties_schema = properties_schema

    async def add_sql_channel(self, channel: Channel[tuple[str, str]]):
        self._sql_channel = channel

    async def add_evalctx_channel(
        self, channel: Channel[tuple[str, EvalContext | InvalidContext]]
    ):
        self._evalctx_channel = channel

    async def add_taskctx_channel(self, channel: Channel[TaskContext]):
        self._taskctx_channel = channel

    async def run(self):
        self._running = True
        async with trio.open_nursery() as nursery:
            self._nursery = nursery
            nursery.start_soon(self._process)
            nursery.start_soon(self._watch_for_shutdown)
            logger.info("[ContextManager] Started.")
            await trio.sleep_forever()
        logger.info("[ContextManager] Stopped.")

    def _parse(self, sql: str) -> QueryContext | InvalidContext:
        return extract_one_query_context(sql, self.properties_schema)

    async def _process(self):
        async for client_id, sql in self._sql_channel:
            ctx = self._parse(sql)
            # TODO: rework this for clearer workflow
            # Ideally channel should be consumer agnostic
            # It is to be avoided to multi type channels

            if isinstance(ctx, InvalidContext):
                logger.warning(f"[ContextManager] {str(ctx.reason)}")

            # Keep TaskContext first
            if isinstance(ctx, (TaskContext)):
                await self._taskctx_channel.send(ctx)
            elif isinstance(ctx, (EvalContext, InvalidContext)):
                await self._evalctx_channel.send((client_id, ctx))
            logger.info(f"[ContextManager] Processed SQL: {sql}")
        logger.error("exited context manager process")

    async def _watch_for_shutdown(self):
        try:
            while self._running:
                await trio.sleep(1)
        finally:
            logger.info("[ContextManager] Shutdown initiated.")
            self._running = False
