from typing import Any
from duckdb import DuckDBPyConnection
from loguru import logger

from channel.channel import Channel
from context.context import (
    CreateContext,
    EvaluableContext,
    InvalidContext,
    CreateWSTableContext,
    DropContext,
)
from engine.engine import duckdb_to_dicts, EVALUABLE_QUERY_DISPATCH
from entity.entity_manager import EntityManager
from channel.channel_broker import _get_event_bus, Consumer, ChannelBroker
from services import Service
from sql.parser import extract_one_query_context
from store import (
    init_metadata_store,
)


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

    #: Outgoing Task context to be orchestrated by TaskManager
    _command_entity: Channel[CreateContext | DropContext]

    #: EventBus ref
    _event_bus: ChannelBroker

    #: Consumer for client sql requests from ClientManager
    _client_sql_request_consumer: Consumer

    def __init__(
        self,
        conn: DuckDBPyConnection,
        properties_schema: dict[str, Any],
    ):
        super().__init__(name="App")
        self._conn = conn
        self._properties_schema = properties_schema
        self._event_bus = _get_event_bus()
        self._command_entity = Channel[CreateContext | DropContext](100)
        # Create Client SQL request consumer & producer
        self._client_sql_request_consumer = self._event_bus.consumer(
            "client.sql.requests"
        )

    def connect_entity_manager(self, entity_manager: EntityManager) -> None:
        """
        Connect App and EntityManager through one Channel.

        Channel:
            - Context Channel (incoming client SQL)

        See channel.py for Channel implementation.
        """
        entity_manager.add_ctx_channel(self._command_entity)

    async def on_start(self):
        """
        Callaback for parent Service class during :meth:`App.start`.
        """
        # Init metastore backend
        init_metadata_store(self._conn)

        # Start sql handling
        self._nursery.start_soon(self._handle_messages)

    async def on_stop(self):
        """
        Callaback for parent Service class during :meth:`App.stop`.
        """
        logger.success("[App] stopping.")
        await self._command_entity.aclose()

    async def submit(self, sql: str) -> None:
        """
        Convenient method to submit SQL to the app.

        This can be used to provide SQL file.

        TODO: move to entrypoint from path on __init__ + on_start
        """
        await self._event_bus.publish("client.sql.requests", (self._internal_ref, sql))

    async def _handle_messages(self) -> None:
        # Process SQL commands from clients, evaluate them, and dispatch results.
        # SQL comes from TCP clients or internal sql file entrypoint

        # Each SQL keeps reference of a client_id for dispatch
        async for client_id, sql in self._client_sql_request_consumer.channel:
            # Convert SQL to "OMLSP" interpretable Context
            ctx = extract_one_query_context(sql, self._properties_schema)

            # Handle context with on_start eval conditions
            if isinstance(ctx, CreateWSTableContext) and ctx.on_start_query != "":
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
                    "[App] Invalid SQL received: {} - reason: {}",
                    sql,
                    str(ctx.reason),
                )
                result = str(ctx.reason)
            else:
                result = ""

            # Send back to client (unless internal query)
            # Anonymous publish: i.e no internal ref to producer
            if client_id != self._internal_ref:
                await self._event_bus.publish(client_id, result)

            # Dispatch CreateContext and DropContext to task manager
            if isinstance(ctx, CreateContext | DropContext):
                await self._command_entity.send(ctx)

        logger.debug("[App] _handle_messages exited cleanly (input channel closed).")
        return

    # TODO: Run eval_ctx in background to avoid thread blocking.
    # This is currently a blocking operation in _handle_messages.
    # Client terminal gets "blocked" till response is received,
    # so it is safe to assume we can queue per client and defer
    # results to keep ordering per client
    async def _eval_ctx(self, ctx: EvaluableContext) -> str:
        try:
            return await EVALUABLE_QUERY_DISPATCH[type(ctx)](self._conn, ctx)
        except Exception as e:
            return f"Fail to run sql: '{ctx}': {e}"
