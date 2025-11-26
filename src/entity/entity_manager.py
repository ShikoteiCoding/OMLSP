"""
Entity Manager managing registration and dependancies of Contex.
"""

from duckdb import DuckDBPyConnection

from channel import Channel
from context.context import (
    CreateContext,
    DropContext,
    DropSimpleContext,
    DropCascadeContext,
)
from store.db import (
    METADATA_TABLE_TABLE_NAME,
    METADATA_VIEW_TABLE_NAME,
    METADATA_SINK_TABLE_NAME,
    METADATA_SOURCE_TABLE_NAME,
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
)
from graph.dependency_graph import dependency_grah
from task.manager import TaskManager

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
    }
    #: Duckdb connections for backend metadata
    backend_conn: DuckDBPyConnection

    #: Reference to all tasks by task id
    _name_to_context: dict[str, CreateContext] = {}

    #: Outgoing Task context to be orchestrated by TaskManager
    _task_events: Channel[CreateContext | str]

    #: Outgoing Context channel to add in DependencyGrah
    _entity_context: Channel[CreateContext | DropContext]

    def __init__(self, backend_conn: DuckDBPyConnection):
        super().__init__(name="EntityManager")
        self.backend_conn = backend_conn
        self._task_events = Channel[CreateContext | str](100)
        self._entity_context = Channel[CreateContext | DropContext](100)

    def add_ctx_channel(self, channel: Channel[CreateContext | DropContext]):
        self._entity_context = channel

    def connect_task_manager(self, task_manager: TaskManager) -> None:
        """
        Connect EntityManager through one Channel.

        Channel:
            - Task Event Channel

        See channel.py for Channel implementation.
        """
        task_manager.add_taskctx_channel(self._task_events)

    async def on_start(self):
        """Main loop for the EntityManager, runs forever."""
        self._nursery.start_soon(self._process)

    async def on_stop(self):
        """Close channel."""
        await self._task_events.aclose()

    async def _process(self):
        async for ctx in self._entity_context:
            if isinstance(ctx, CreateContext):
                await self._register_entity(ctx)
            elif isinstance(ctx, DropContext):
                await self._delete_entity(ctx)

    async def _register_entity(self, ctx: CreateContext):
        dependency_grah.ensure_vertex(ctx.name)
        # Register dependencies in the graph
        for parent in getattr(ctx, "upstreams", []):
            dependency_grah.add_vertex(parent, ctx.name)

        await self._task_events.send(ctx)
        self._name_to_context[ctx.name] = ctx
        logger.success(f"[EntityManager] registered context '{ctx.name}'")

    async def _delete_entity(self, ctx: DropContext):
        if isinstance(ctx, DropSimpleContext):
            is_leaf = dependency_grah.is_a_leaf(ctx.name)
            if not is_leaf:
                logger.warning(f"[EntityManager] entity is not a leaf '{ctx}'")
                return
            dependency_grah.drop_leaf(ctx.name)
            ctx_node = self._name_to_context.get(ctx.name)
            if ctx_node is not None:
                await self._destroy_entity(ctx_node)
                await self._task_events.send(ctx_node)

            logger.success(f"[EntityManager] dropped entity: {ctx}")

        if isinstance(ctx, DropCascadeContext):
            dropped_from_graph = dependency_grah.drop_recursive(ctx.name)
            if not dropped_from_graph:
                logger.warning(f"[EntityManager] nothing to drop for '{ctx}'")
                return
            print(dropped_from_graph)
            for n in dropped_from_graph:
                ctx_node = self._name_to_context.get(n)
                if ctx_node is not None:
                    await self._destroy_entity(ctx_node)
                    await self._task_events.send(ctx_node.name)

            logger.success(
                f"[EntityManager] dropped cascade entities: {dropped_from_graph}"
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
