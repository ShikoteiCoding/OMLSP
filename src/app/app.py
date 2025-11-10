import trio
from typing import Any, Callable, Type
from duckdb import DuckDBPyConnection, connect
from loguru import logger
from channel import Channel
from context.context import (
    CreateContext,
    CreateHTTPTableContext,
    CreateWSTableContext,
    CreateHTTPLookupTableContext,
    CommandContext,
    CreateSecretContext,
    CreateSinkContext,
    CreateHTTPSourceContext,
    CreateWSSourceContext,
    CreateViewContext,
    DropContext,
    EvaluableContext,
    InvalidContext,
    SelectContext,
    SetContext,
    ShowContext,
    TaskContext,
    OnStartContext,
)
from engine.engine import eval_select, duckdb_to_dicts, duckdb_to_pl
from metadata import (
    create_secret,
    create_sink,
    create_source,
    create_table,
    create_view,
    init_metadata_store,
)
from server import ClientManager
from sql.file.reader import iter_sql_statements
from sql.parser import extract_one_query_context
from scheduler import TrioScheduler
from task import TaskManager
from services import Service

ClientId = str
Evaled = str
SQL = str
ClientSQL = tuple[ClientId, SQL]
EvaledSQL = tuple[ClientId, Evaled]

__all__ = ["App"]


CREATE_QUERY_DISPATCH: dict[Type[CreateContext], Callable] = {
    CreateHTTPLookupTableContext: create_table,
    CreateHTTPTableContext: create_table,
    CreateWSTableContext: create_table,
    CreateHTTPSourceContext: create_source,
    CreateWSSourceContext: create_source,
    CreateViewContext: create_view,
    CreateSinkContext: create_sink,
    CreateSecretContext: create_secret,
}


class App(Service):
    """
    App that orchestrates client and task managers, processes SQL,
    and dispatches tasks, built as derived Service.
    """

    #: Duckdb connection
    _conn: DuckDBPyConnection

    #: SQL properties json schema for properties validation
    _properties_schema: dict[str, Any]

    #: Internal reference for when sql doesn't come from TCP
    _internal_ref = "__runner"

    #: Incoming SQL from self.submit() or TCP client
    _sql_to_eval: Channel[ClientSQL]

    #: Outgoing SQL result to TCP client
    _evaled_sql: Channel[EvaledSQL]

    #: Outgoing Task context to be orchestrated by TaskManager
    _tasks_to_deploy: Channel[TaskContext]

    _tasks_to_cancel: Channel[DropContext]

    def __init__(
        self,
        conn: DuckDBPyConnection,
        properties_schema: dict[str, Any],
    ):
        super().__init__(name="App")
        self._conn = conn
        self._properties_schema = properties_schema

        # Channels are created and own by App.
        # Could be injected later when architrecture
        # becomes more mature.
        self._sql_to_eval = Channel[ClientSQL](100)
        self._evaled_sql = Channel[EvaledSQL](100)
        self._tasks_to_deploy = Channel[TaskContext](100)
        self._tasks_to_cancel = Channel[DropContext](100)


    def connect_client_manager(self, client_manager: ClientManager) -> None:
        """
        Connect App and ClientManager through Channels.

        Channels:
            - sql Channel (incoming client SQL)
            - evaled Channel (outgoing evaled SQL)

        See channel.py for Channel implementation.
        """
        client_manager.add_sql_channel(self._sql_to_eval)
        client_manager.add_response_channel(self._evaled_sql)

    def connect_task_manager(self, task_manager: TaskManager) -> None:
        """
        Connect App and TaskManager through one Channel.

        Channel:
            - Task Channel (incoming client SQL)

        See channel.py for Channel implementation.
        """
        task_manager.add_taskctx_channel(self._tasks_to_deploy)
        task_manager.add_task_cancel_channel(self._tasks_to_cancel)

    async def on_start(self):
        # Init metastore backend
        init_metadata_store(self._conn)

        # Start sql handling
        self._nursery.start_soon(self._handle_messages)

    async def on_stop(self):
        logger.success("[App] stopping.")
        await self._sql_to_eval.aclose()
        await self._evaled_sql.aclose()
        await self._tasks_to_deploy.aclose()

    async def submit(self, sql: str) -> None:
        """
        Convenient method to submit SQL to the app.

        This can be used to provide SQL file.

        TODO: move to entrypoint from path on __init__ + on_start
        """
        await self._sql_to_eval.send((self._internal_ref, sql))

    async def _handle_messages(self) -> None:
        # Process SQL commands from clients, evaluate them, and dispatch results.
        # SQL comes from TCP clients or internal sql file entrypoint

        # Each SQL keeps reference of a client_id for dispatch
        async for client_id, sql in self._sql_to_eval:
            # Convert SQL to "OMLSP" interpretable Context
            ctx = extract_one_query_context(sql, self._properties_schema)

            # Handle context with on_start eval conditions
            if isinstance(ctx, OnStartContext) and ctx.on_start_query != "":
                on_start_result = duckdb_to_dicts(self._conn, ctx.on_start_query)
                if len(on_start_result) == 0:
                    # Override context to bypass next
                    ctx = InvalidContext(
                        reason=f"Response from '{ctx.on_start_query}' is empty. Cannot proceed."
                    )

            # Evaluable Context are simple statements which
            # can be executed and simply return a result.
            if isinstance(ctx, EvaluableContext):
                result = self._eval_ctx(client_id, ctx)

            # Warn of invalid context for tracing.
            elif isinstance(ctx, InvalidContext):
                logger.warning(
                    "[App] Invalid SQL received: {} - reason: {}",
                    sql,
                    str(ctx.reason),
                )
                result = str(ctx.reason)
            else:
                result = ""

            # Send back to client (unless internal query)
            if client_id != self._internal_ref:
                await self._evaled_sql.send((client_id, result))

            # Dispatch TaskContext to task manager
            if isinstance(ctx, TaskContext):
                await self._tasks_to_deploy.send(ctx)

            # Dispatch DropContext to task manager
            if isinstance(ctx, DropContext):
                await self._tasks_to_cancel.send(ctx)

        logger.debug("[App] _handle_messages exited cleanly (input channel closed).")
        return

    # TODO: Run eval_ctx in background to avoid thread blocking.
    # This is currently a blocking operation in _handle_messages.
    # Client terminal gets "blocked" till response is received,
    # so it is safe to assume we can queue per client and defer
    # results to keep ordering per client
    def _eval_ctx(self, client_id: str, ctx: EvaluableContext) -> str:
        try:
            # Query with Create Context statements to eval before
            # Task manager instanciation
            if isinstance(ctx, CreateContext):
                CREATE_QUERY_DISPATCH[type(ctx)](self._conn, ctx)
            elif isinstance(ctx, CommandContext):
                return str(duckdb_to_pl(self._conn, ctx.query))
            elif isinstance(ctx, SetContext):
                self._conn.sql(ctx.query)
                return "SET"
            elif isinstance(ctx, SelectContext) and client_id != self._internal_ref:
                return eval_select(self._conn, ctx)
            elif isinstance(ctx, ShowContext):
                return str(duckdb_to_pl(self._conn, ctx.query))
        except Exception as e:
            return f"fail to run sql: '{ctx}': {e}"

        return ""


if __name__ == "__main__":
    import json
    from pathlib import Path

    with open(Path("src/properties.schema.json"), "rb") as fo:
        properties_schema = json.loads(fo.read().decode("utf-8"))
    backend_conn: DuckDBPyConnection = connect(database=":memory:")
    transform_conn: DuckDBPyConnection = backend_conn
    scheduler = TrioScheduler()
    task_manager = TaskManager(backend_conn, transform_conn)
    client_manager = ClientManager(backend_conn)
    runner = App(backend_conn, properties_schema)

    async def main():
        # preload file mSQLs
        for sql in iter_sql_statements("examples/basic.sql"):
            await runner.submit(sql)
        async with trio.open_nursery() as nursery:
            nursery.start_soon(runner.start, nursery)

            # simulate console input
            await trio.sleep(1)
            await runner.submit("SELECT * FROM my_table;")

            await trio.sleep(5)

        logger.info(task_manager)

    trio.run(main)
