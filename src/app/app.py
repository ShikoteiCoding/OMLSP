from __future__ import annotations

from typing import Any, TYPE_CHECKING
from duckdb import DuckDBPyConnection
from loguru import logger

from context.context import (
    CreateContext,
    EvaluableContext,
    InvalidContext,
    CreateWSTableContext,
    DropContext,
)
from engine.engine import duckdb_to_dicts, EVALUABLE_QUERY_DISPATCH
from services import Service
from sql.parser import extract_one_query_context
from store import (
    init_metadata_store,
)

if TYPE_CHECKING:
    from app.types import ClientId, SQL

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

    def __init__(
        self,
        conn: DuckDBPyConnection,
        properties_schema: dict[str, Any],
    ):
        super().__init__(name="App")
        self._conn = conn
        self._properties_schema = properties_schema

    async def on_start(self):
        """
        Callaback for parent Service class during :meth:`App.start`.
        """
        # Init metastore backend
        init_metadata_store(self._conn)

    async def on_stop(self):
        """
        Callaback for parent Service class during :meth:`App.stop`.
        """
        logger.success("[{}] stopping.", self.name)

    async def on_receive(self, client_id: ClientId, sql: SQL) -> None:
        # Process SQL commands from clients, evaluate them, and dispatch results.
        # SQL comes from TCP clients or internal sql file entrypoint

        # Each SQL keeps reference of a client_id for dispatch
        # Convert SQL to "OMLSP" interpretable Context
        ctx = extract_one_query_context(sql, self._properties_schema)

        # Handle context with on_start eval conditions
        if isinstance(ctx, CreateWSTableContext) and ctx.on_start_query:
            on_start_result = duckdb_to_dicts(self._conn, ctx.on_start_query)
            if len(on_start_result) == 0:
                # Override context to bypass next
                ctx = InvalidContext(
                    reason=f"Response from '{ctx.on_start_query}' is empty. Cannot proceed."
                )

        # Evaluable Context are simple statements which
        # can be executed and simply return a result.
        if isinstance(ctx, EvaluableContext):
            result = await self._eval_ctx(ctx)

        # Warn of invalid context for tracing.
        elif isinstance(ctx, InvalidContext):
            logger.warning(
                "[{}] Invalid SQL received: {} - reason: {}",
                self.name,
                sql,
                str(ctx.reason),
            )
            result = str(ctx.reason)
        else:
            result = ""

        # Dispatch CreateContext and DropContext to task manager
        if isinstance(ctx, CreateContext | DropContext):
            await self.channel_registry.publish("EntityManager", ctx)

        # Send back reply to client
        if client_id != self.name:
            await self.channel_registry.publish(client_id, result)

    async def _eval_ctx(self, ctx: EvaluableContext) -> str:
        """
        TODO: Run eval_ctx in background to avoid thread blocking.
        This is currently a blocking operation in _handle_messages.
        Client terminal gets "blocked" till response is received,
        so it is safe to assume we can queue per client and defer
        results to keep ordering per client
        """
        try:
            return await EVALUABLE_QUERY_DISPATCH[type(ctx)](self._conn, ctx)
        except Exception as e:
            logger.error("Error evaluating context type '{}': {}", type(ctx), ctx)
            return f"Error evaluating context type '{ctx}': {e}"
