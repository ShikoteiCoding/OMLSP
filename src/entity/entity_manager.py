"""
Entity Manager managing registration and dependancies of Context.
"""

from duckdb import DuckDBPyConnection

from channel.broker import ChannelBroker, _get_channel_broker
from channel.consumer import Consumer
from channel.producer import Producer
from channel.promise import Promise
from channel.types import ValidResponse, InvalidResponse

from store.db import (
    METADATA_TABLE_TABLE_NAME,
    METADATA_VIEW_TABLE_NAME,
    METADATA_SINK_TABLE_NAME,
    METADATA_SOURCE_TABLE_NAME,
    METADATA_SECRET_TABLE_NAME,
    delete_metadata,
)
from context.context import (
    CreateSinkContext,
    CreateHTTPSourceContext,
    CreateHTTPTableContext,
    CreateWSSourceContext,
    CreateWSTableContext,
    CreateViewContext,
    CreateHTTPLookupTableContext,
    CreateSecretContext,
    CreateContext,
    DropContext,
    DropSimpleContext,
    DropCascadeContext,
)
from graph.dependency_graph import dependency_grah
from task.types import TaskManagerCommand
from store.lookup import callback_store

from services import Service

from loguru import logger

__all__ = ["EntityManager"]


class EntityManager(Service):
    CTX_METADATA_MAP: dict[type, tuple[str, str]] = {
        CreateHTTPTableContext: (METADATA_TABLE_TABLE_NAME, "table_name"),
        CreateWSTableContext: (METADATA_TABLE_TABLE_NAME, "table_name"),
        CreateHTTPLookupTableContext: (METADATA_TABLE_TABLE_NAME, "table_name"),
        CreateViewContext: (METADATA_VIEW_TABLE_NAME, "view_name"),
        CreateSinkContext: (METADATA_SINK_TABLE_NAME, "sink_name"),
        CreateHTTPSourceContext: (METADATA_SOURCE_TABLE_NAME, "source_name"),
        CreateWSSourceContext: (METADATA_SOURCE_TABLE_NAME, "source_name"),
        CreateSecretContext: (METADATA_SECRET_TABLE_NAME, "secret_name"),
    }
    #: Duckdb connections for backend metadata
    backend_conn: DuckDBPyConnection

    #: Reference to all tasks by task id
    _name_to_context: dict[str, CreateContext] = {}

    #: ChannelBroker ref
    _channel_broker: ChannelBroker

    #: Consumer for entity commands from App
    _entity_commands_consumer: Consumer

    #: Producer for tasks commands to TaskManager
    _task_commands_producer: Producer

    def __init__(self, backend_conn: DuckDBPyConnection):
        super().__init__(name="EntityManager")
        self.backend_conn = backend_conn
        self._channel_broker = _get_channel_broker()
        self._entity_commands_consumer = self._channel_broker.consumer(
            "entity.commands"
        )
        self._task_commands_producer = self._channel_broker.producer("task.commands")

    async def on_start(self):
        """Main loop for the EntityManager, runs forever."""
        self._nursery.start_soon(self._process)

    async def _process(self):
        # NOTE: We intentionally operate on the original CreateContext rather
        # than converting to a dedicated Entity type.
        # This keeps task lifecycle logic simple until the future Entity
        # abstraction is introduced

        # Commands supported:
        # CREATE
        # DROP
        async for ctx, promise in self._entity_commands_consumer.channel:
            if isinstance(ctx, CreateContext):
                await self._register_entity(ctx, promise)
            elif isinstance(ctx, DropContext):
                await self._delete_entity(ctx, promise)

    async def _register_entity(self, ctx: CreateContext, promise: Promise):
        dependency_grah.ensure_vertex(ctx.name)

        # Register dependencies in the graph
        match ctx:
            # Receiver tasks can have multiple upstream, each of
            # the upstream correspond to a vertex
            case CreateSinkContext() | CreateViewContext():
                for parent in ctx.upstreams:
                    dependency_grah.add_vertex(parent, ctx.name)

            # NOTE: For now secrets are only supported in Http
            # properties
            case (
                CreateHTTPTableContext()
                | CreateHTTPSourceContext()
                | CreateHTTPLookupTableContext()
            ):
                for _, secret_name in ctx.properties.secrets:
                    dependency_grah.add_vertex(secret_name, ctx.name)

        await self._task_commands_producer.produce((TaskManagerCommand.CREATE, ctx))
        self._name_to_context[ctx.name] = ctx
        promise.set(ValidResponse(f"Successfully created '{ctx.name}"))
        logger.success(f"[EntityManager] registered context '{ctx.name}'")

    async def _delete_entity(self, ctx: DropContext, promise: Promise):
        # TODO: emit real-time deletion events to clients
        if isinstance(ctx, DropSimpleContext):
            is_leaf = dependency_grah.is_a_leaf(ctx.name)
            if not is_leaf:
                promise.set(InvalidResponse(f"Cannot drop '{ctx.name}', it has dependencies or doesn't exist"))
                logger.warning(f"[EntityManager] entity is not a leaf '{ctx.name}'")
                return
            dependency_grah.remove(ctx.name)
            promise.set(ValidResponse(f"Successfully dropped '{ctx.name}'"))
            ctx_node = self._name_to_context.get(ctx.name)
            if ctx_node is not None:
                await self._task_commands_producer.produce(
                    (TaskManagerCommand.DELETE, ctx_node)
                )
                await self._destroy_entity(ctx_node)

            logger.success(f"[EntityManager] removed entity: '{ctx.name}'")

        if isinstance(ctx, DropCascadeContext):
            removed_nodes = dependency_grah.remove_recursive(ctx.name)
            if not removed_nodes:
                promise.set(InvalidResponse (f"Nothing to drop, '{ctx.name}' doesn't exist"))
                logger.warning(f"[EntityManager] nothing to remove for '{ctx.name}'")
                return
            for node in removed_nodes:
                ctx_node = self._name_to_context.get(node)
                if ctx_node is not None:
                    await self._task_commands_producer.produce(
                        (TaskManagerCommand.DELETE, ctx_node)
                    )
                    await self._destroy_entity(ctx_node)
                logger.info(f"[EntityManager] removed entity: {node}")

            promise.set(ValidResponse(f"Successfully dropped '{ctx.name}' and dependencies '{removed_nodes}'"))
            logger.success(
                f"[EntityManager] removed entities (CASCADE): '{removed_nodes}'"
            )

    async def _destroy_entity(self, ctx: CreateContext):
        if isinstance(ctx, CreateHTTPLookupTableContext):
            callback_store.delete(ctx.name)

        metadata_table, metadata_column = self.resolve_metadata(type(ctx))
        delete_metadata(self.backend_conn, metadata_table, metadata_column, ctx.name)
        self._name_to_context.pop(ctx.name)
        if ctx.has_data:
            sql_query = f"DROP TABLE {ctx.name}"
            self.backend_conn.sql(sql_query)

    def resolve_metadata(self, ctx_type: type) -> tuple[str, str]:
        return self.CTX_METADATA_MAP.get(
            ctx_type,
            (METADATA_TABLE_TABLE_NAME, "table_name"),  # default
        )
