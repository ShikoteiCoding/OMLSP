from typing import Any, NoReturn
import trio
from duckdb import DuckDBPyConnection, connect
from loguru import logger
from channel import Channel
from context.context import (
    CommandContext,
    CreateSecretContext,
    CreateSinkContext,
    CreateSourceContext,
    CreateTableContext,
    CreateViewContext,
    EvaluableContext,
    InvalidContext,
    SelectContext,
    SetContext,
    ShowContext,
    TaskContext,
    OnStartContext,
)
from engine.engine import duckdb_to_dicts, duckdb_to_pl, substitute_sql_template
from metadata import (
    create_secret,
    create_sink,
    create_source,
    create_table,
    create_view,
    get_lookup_tables,
    get_duckdb_tables,
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

    async def on_start(self):
        # Init metastore backend
        init_metadata_store(self._conn)

        # Start sql handling
        self._nursery.start_soon(self._handle_messages)

    async def submit(self, sql: str) -> None:
        """
        Convenient method to submit SQL to the app.

        This can be used to provide SQL file.

        TODO: move to entrypoint from path on __init__ + on_start
        """
        await self._sql_to_eval.send((self._internal_ref, sql))

    async def _handle_messages(self) -> NoReturn:
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

        raise RuntimeError("SQL Handle message loop has exited.")

    # TODO: Run eval_ctx in background to avoid thread blocking.
    # This is currently a blocking operation in _handle_messages.
    # Client terminal gets "blocked" till response is received,
    # so it is safe to assume we can queue per client and defer
    # results to keep ordering per client
    def _eval_ctx(self, client_id: str, ctx: EvaluableContext) -> str:
        # Evaluate and execute various SQL context types.
        # TODO: to be eventually replaced by visitor pattern ?
        try:
            if isinstance(ctx, CreateTableContext):
                return create_table(self._conn, ctx)
            elif isinstance(ctx, CreateSourceContext):
                return create_source(self._conn, ctx)
            elif isinstance(ctx, CreateViewContext):
                return create_view(self._conn, ctx)
            elif isinstance(ctx, CreateSinkContext):
                return create_sink(self._conn, ctx)
            elif isinstance(ctx, CreateSecretContext):
                return create_secret(self._conn, ctx)
            elif isinstance(ctx, CommandContext):
                return str(duckdb_to_pl(self._conn, ctx.query))
            elif isinstance(ctx, SetContext):
                self._conn.sql(ctx.query)
                return "SET"
            elif isinstance(ctx, SelectContext) and client_id != self._internal_ref:
                return self._eval_select_ctx(ctx)
            elif isinstance(ctx, ShowContext):
                return str(duckdb_to_pl(self._conn, ctx.query))
        except Exception as e:
            return f"fail to run sql: '{ctx}': {e}"

        return ""

    def _eval_select_ctx(self, ctx):
        table_name = ctx.table
        lookup_tables = get_lookup_tables(self._conn)
        duckdb_tables = get_duckdb_tables(self._conn)

        # add internal tables here for easier dev time
        duckdb_tables.append("duckdb_tables")

        substitute_mapping = dict(zip(duckdb_tables, duckdb_tables))

        if table_name in lookup_tables:
            return f"'{table_name}' is a lookup table, you cannot use it in FROM."

        if table_name not in duckdb_tables:
            return f"'{table_name}' doesn't exist"

        duckdb_sql = substitute_sql_template(self._conn, ctx, substitute_mapping)
        return str(duckdb_to_pl(self._conn, duckdb_sql))


if __name__ == "__main__":
    import json
    from pathlib import Path

    with open(Path("src/properties.schema.json"), "rb") as fo:
        properties_schema = json.loads(fo.read().decode("utf-8"))
    db_conn: DuckDBPyConnection = connect(database=":memory:")
    exec_conn: DuckDBPyConnection = connect(database=":memory:")
    scheduler = TrioScheduler()
    task_manager = TaskManager(db_conn, exec_conn)
    client_manager = ClientManager(db_conn)
    runner = App(db_conn, properties_schema)

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
