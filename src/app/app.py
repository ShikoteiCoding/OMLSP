from typing import Any
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
from channel.broker import _get_channel_broker, ChannelBroker
from channel.consumer import Consumer
from channel.producer import Producer
from channel.types import ValidResponse, InvalidResponse
from services import Service
from sql.parser import extract_one_query_context
from store import (
    init_metadata_store,
)
from graph.dependency_graph import dependency_grah


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

    #: ChannelBroker ref
    _channel_broker: ChannelBroker

    #: Consumer for client sql requests from ClientManager
    _client_sql_request_consumer: Consumer

    #: Producer for entity commands to EntityManager
    _entity_commands_producer: Producer

    def __init__(
        self,
        conn: DuckDBPyConnection,
        properties_schema: dict[str, Any],
    ):
        super().__init__(name="App")
        self._conn = conn
        self._properties_schema = properties_schema
        self._channel_broker = _get_channel_broker()

        self._client_sql_request_consumer = self._channel_broker.consumer(
            "client.sql.requests"
        )
        self._entity_commands_producer = self._channel_broker.producer(
            "entity.commands"
        )

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

    async def submit(self, sql: str) -> None:
        """
        Convenient method to submit SQL to the app.

        This can be used to provide SQL file.

        TODO: move to entrypoint from path on __init__ + on_start
        """
        await self._channel_broker.send("client.sql.requests", sql)

    async def _handle_messages(self) -> None:
        # Process SQL commands from clients, evaluate them, and dispatch results.
        # SQL comes from TCP clients or internal sql file entrypoint

        # Each SQL keeps reference of a client_id for dispatch
        async for sql, promise in self._client_sql_request_consumer.channel:
            # Convert SQL to "OMLSP" interpretable Context
            ctx = extract_one_query_context(sql, self._properties_schema)

            if isinstance(ctx, InvalidContext):
                logger.warning(
                    "[App] Invalid SQL received: {} - reason: {}",
                    sql,
                    str(ctx.reason),
                )
                result = str(ctx.reason)
                promise.set(InvalidResponse(result))

            # Check and dispatch CreateContext to manager
            elif isinstance(ctx, CreateContext):
                # Pyton check, faster than Duckdb
                if dependency_grah.exist(ctx.name):
                    promise.set(
                        InvalidResponse(
                            f"Entity '{ctx.name}' already exists. DROP it first."
                        )
                    )
                    continue

                # Handle context with on_start eval conditions
                if isinstance(ctx, CreateWSTableContext) and ctx.on_start_query:
                    on_start_result = duckdb_to_dicts(self._conn, ctx.on_start_query)
                    if len(on_start_result) == 0:
                        promise.set(
                            InvalidResponse(
                                f"Pre-check failed: '{ctx.on_start_query}' returned empty."
                            )
                        )
                        continue

                await EVALUABLE_QUERY_DISPATCH[type(ctx)](self._conn, ctx)
                # Handover to Entity Manager
                await self._entity_commands_producer.produce((ctx, promise))

            elif isinstance(ctx, DropContext):
                if not dependency_grah.exist(ctx.name):
                    promise.set(
                        InvalidResponse(
                            f"Entity '{ctx.name}' does not exist. CREATE it first."
                        )
                    )
                    continue
                # Handover to Entity Manager
                await self._entity_commands_producer.produce((ctx, promise))

            elif isinstance(ctx, EvaluableContext):
                try:
                    result = await EVALUABLE_QUERY_DISPATCH[type(ctx)](self._conn, ctx)
                    promise.set(ValidResponse(result))
                except Exception as e:
                    logger.error(f"Error evaluating context type '{type(ctx)}': {e}")
                    promise.set(
                        InvalidResponse(f"Error evaluating query '{ctx.query}': {e}")
                    )

        logger.debug("[App] _handle_messages exited cleanly (input channel closed).")
        return
