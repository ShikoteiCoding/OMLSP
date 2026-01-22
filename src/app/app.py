from __future__ import annotations

from typing import Any, TYPE_CHECKING
from duckdb import DuckDBPyConnection
from loguru import logger

from catalog.catalog import CatalogReader, CatalogWriter
from catalog.table_catalog import TableCatalog
from catalog.view_catalog import ViewCatalog
from catalog.source_catalog import SourceCatalog
from catalog.sink_catalog import SinkCatalog
from catalog.secret_catalog import SecretCatalog

from context.context import (
    CreateContext,
    CreateSecretContext,
    CreateSinkContext,
    CreateSourceContext,
    CreateTableContext,
    CreateViewContext,
    EvaluableContext,
    InvalidContext,
    CreateWSTableContext,
    DropContext,
)
from engine.engine import duckdb_to_dicts, EVALUABLE_QUERY_DISPATCH
from services import Service
from sql.parser import Parser
from sql.plan import extract_one_query_context
from sql.resolver import Resolver
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

    #: Catalog read access for validating DDL & DML
    _catalog_reader: CatalogReader

    #: Catalog write access for registering DDL
    _catalog_writer: CatalogWriter

    def __init__(
        self,
        conn: DuckDBPyConnection,
        properties_schema: dict[str, Any],
        catalog_reader: CatalogReader,
        catalog_writer: CatalogWriter,
    ):
        super().__init__(name="App")
        self._conn = conn
        self._properties_schema = properties_schema
        self._catalog_reader = catalog_reader
        self._catalog_writer = catalog_writer

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

    def _register_ddl(self, ctx: CreateContext | DropContext, original_sql: str) -> str:
        if isinstance(ctx, CreateContext):
            if isinstance(ctx, CreateTableContext):
                entity = TableCatalog(
                    name=ctx.name,
                    properties=ctx.properties,
                    sql=ctx.query if hasattr(ctx, "query") else original_sql,
                    definition=original_sql,
                    lookup=ctx.lookup,
                )
                self._catalog_writer.create_table(entity)

            elif isinstance(ctx, CreateSourceContext):
                entity = SourceCatalog(
                    name=ctx.name,
                    properties=ctx.properties,
                    sql=ctx.query if hasattr(ctx, "query") else original_sql,
                    definition=original_sql,
                )
                self._catalog_writer.create_source(entity)

            elif isinstance(ctx, CreateViewContext):
                entity = ViewCatalog(
                    name=ctx.name,
                    upstreams=ctx.upstreams,
                    sql=ctx.query,
                    definition=original_sql,
                    materialized=ctx.materialized,
                )
                self._catalog_writer.create_view(entity)

            elif isinstance(ctx, CreateSinkContext):
                entity = SinkCatalog(
                    name=ctx.name,
                    properties=ctx.properties,
                    upstreams=ctx.upstreams,
                    sql=ctx.query if hasattr(ctx, "query") else original_sql,
                    definition=original_sql,
                )
                self._catalog_writer.create_sink(entity)

            elif isinstance(ctx, CreateSecretContext):
                entity = SecretCatalog(
                    name=ctx.name,
                    properties=ctx.properties,
                    value=ctx.value,
                    sql=ctx.query if hasattr(ctx, "query") else original_sql,
                    definition=original_sql,
                )
                self._catalog_writer.create_secret(entity)

            return "CREATE"

        elif isinstance(ctx, DropContext):
            # Map drop_type to catalog method
            drop_type = ctx.drop_type.upper()
            if drop_type == "TABLE":
                self._catalog_writer.drop_table(ctx.name)
            elif drop_type == "SOURCE":
                self._catalog_writer.drop_source(ctx.name)
            elif drop_type == "VIEW":
                self._catalog_writer.drop_view(ctx.name)
            elif drop_type == "SINK":
                self._catalog_writer.drop_sink(ctx.name)
            elif drop_type == "SECRET":
                self._catalog_writer.drop_secret(ctx.name)

            return "DROP"

    async def on_receive(self, client_id: ClientId, sql: SQL) -> None:
        # Process SQL commands from clients, evaluate them, and dispatch results.
        # SQL comes from TCP clients or internal sql file entrypoint

        unresolved_sql = Parser.parse(sql)
        if not unresolved_sql:
            logger.warning("[{}] Parse error: {}", self.name, unresolved_sql)
            if client_id != self.name:
                await self.channel_registry.publish(client_id, str(unresolved_sql))
            return

        ctx = extract_one_query_context(unresolved_sql, self._properties_schema)

        if isinstance(ctx, InvalidContext):
            result = str(ctx.reason)
            logger.warning(
                "[{}] Invalid SQL received: {} - reason: {}",
                self.name,
                sql,
                result,
            )
            if client_id != self.name:
                await self.channel_registry.publish(client_id, result)
            return

        if not Resolver.resolve(ctx, self._catalog_reader):
            reason = f"Catalog resolution failed for query: {sql}. Tables or upstreams missing."
            logger.warning("[{}] {}", self.name, reason)
            if client_id != self.name:
                await self.channel_registry.publish(client_id, reason)
            return

        # Handle context with on_start eval conditions
        if isinstance(ctx, CreateWSTableContext) and ctx.on_start_query:
            on_start_result = duckdb_to_dicts(self._conn, ctx.on_start_query)
            if len(on_start_result) == 0:
                reason = (
                    f"Response from '{ctx.on_start_query}' is empty. Cannot proceed."
                )
                logger.warning("[{}] {}", self.name, reason)
                if client_id != self.name:
                    await self.channel_registry.publish(client_id, reason)
                return

        result = ""

        if isinstance(ctx, (CreateContext, DropContext)):
            self._register_ddl(ctx, sql)
            await self.channel_registry.publish("EntityManager", ctx)
            result = "Success"
        
        if isinstance(ctx, EvaluableContext):
            result = await self._eval_ctx(ctx)

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
