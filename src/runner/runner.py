import trio

from duckdb import DuckDBPyConnection, connect
from loguru import logger
from typing import Any

from channel import Channel
from context.context import (
    CommandContext,
    CreateLookupTableContext,
    CreateSinkContext,
    CreateTableContext,
    CreateViewContext,
    EvaluableContext,
    InvalidContext,
    SelectContext,
    SetContext,
    TaskContext,
)
from engine.engine import duckdb_to_pl, pre_hook_select_statements
from metadata import (
    create_sink,
    create_table,
    create_view,
    get_lookup_tables,
    get_tables,
    init_metadata_store,
)
from server import ClientManager
from sql.file.reader import iter_sql_statements
from sql.sqlparser.parser import extract_one_query_context
from scheduler import TrioScheduler
from task import TaskManager

ClientId = str


class Runner:
    _sql_channel = Channel[tuple[str, str]](10)
    _client_channel = Channel[tuple[str, str]](10)
    _taskctx_channel = Channel[TaskContext](10)
    _nursery = None
    _running = False
    _internal_ref = "__runner"

    def __init__(
        self,
        conn: DuckDBPyConnection,
        properties_schema: dict[str, Any],
        task_manager: TaskManager,
        client_manager: ClientManager,
    ):
        self.conn = conn
        self.properties_schema = properties_schema
        self.task_manager = task_manager
        self.client_manager = client_manager

    async def build(self):
        # SQL channel
        await self.client_manager.add_sql_channel(self._sql_channel)

        # Str channel (already executed)
        await self.client_manager.add_response_channel(self._client_channel)

        # Task Ctx channel (to schedule)
        await self.task_manager.add_taskctx_channel(self._taskctx_channel)

    async def submit(self, sql: str) -> None:
        """
        Convenient method for sql submit. Syntactic sugar.
        """
        # main is main app client_id
        await self._sql_channel.send((self._internal_ref, sql))

    async def run(self):
        init_metadata_store(self.conn)
        self._running = True
        async with trio.open_nursery() as nursery:
            self._nursery = nursery

            nursery.start_soon(self.task_manager.run)
            nursery.start_soon(self.client_manager.run)

            nursery.start_soon(self._process)
            nursery.start_soon(self._watch_for_shutdown)
            logger.info("[Runner] Started.")
            await trio.sleep_forever()

        logger.info("[Runner] Stopped.")

    async def _process(self):
        async for client_id, sql in self._sql_channel:
            ctx = extract_one_query_context(sql, self.properties_schema)

            # execute eval
            if isinstance(ctx, EvaluableContext):
                result = self._eval_ctx(client_id, ctx)

            elif isinstance(ctx, InvalidContext):
                logger.warning(
                    "[Runner] - attempted to parse invalid sql: {} - reason: {}",
                    sql,
                    str(ctx.reason),
                )
                result = str(ctx.reason)

            if client_id != self._internal_ref:
                await self._client_channel.send((client_id, result))

            # dispatch to task manager
            if isinstance(ctx, TaskContext):
                await self._taskctx_channel.send(ctx)

    # TODO: return ValidEval / InvalidEval objects to handle dynamic errors before registering
    def _eval_ctx(self, client_id: ClientId, ctx: EvaluableContext) -> str:
        if isinstance(ctx, (CreateTableContext, CreateLookupTableContext)):
            return create_table(self.conn, ctx)

        elif isinstance(ctx, CreateViewContext):
            return create_view(self.conn, ctx)

        elif isinstance(ctx, CreateSinkContext):
            return create_sink(self.conn, ctx)

        elif isinstance(ctx, CommandContext):
            return str(duckdb_to_pl(self.conn, ctx.query))

        elif isinstance(ctx, SetContext):
            self.conn.sql(ctx.query)
            return "SET"

        # TODO: find more elegant way to avoid Select from file submit ?
        elif isinstance(ctx, SelectContext) and client_id != self._internal_ref:
            table_name = ctx.table
            lookup_tables = get_lookup_tables(self.conn)
            tables = get_tables(self.conn)

            if table_name in lookup_tables:
                return f"{table_name} is a lookup table, you cannot use it in FROM."

            duckdb_sql = pre_hook_select_statements(self.conn, ctx, tables)
            return str(duckdb_to_pl(self.conn, duckdb_sql))

        return ""

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
    scheduler = TrioScheduler()
    task_manager = TaskManager(conn, scheduler)
    client_manager = ClientManager(conn)
    executors = {}
    runner = Runner(conn, properties_schema, task_manager, client_manager)

    async def main():
        await runner.build()
        # preload file mSQLs
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
