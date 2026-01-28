from __future__ import annotations

from typing import Any, TYPE_CHECKING
from duckdb import DuckDBPyConnection
from loguru import logger

from catalog.catalog import CatalogReader, CatalogWriter
from catalog.errors import CatalogError
from catalog.table_catalog import TableCatalog
from catalog.view_catalog import ViewCatalog
from catalog.source_catalog import SourceCatalog
from catalog.sink_catalog import SinkCatalog
from catalog.secret_catalog import SecretCatalog

from context.context import (
    CreateContext,
    CreateWSTableContext,
    DropContext,
    CreateSecretContext,
    CreateSinkContext,
    CreateSourceContext,
    CreateTableContext,
    CreateViewContext,
    EvaluableContext,
)


from engine.engine import duckdb_to_dicts, EVALUABLE_QUERY_DISPATCH
from services import Service
from sql.binder import Binder
from sql.errors import PlanError, SqlSyntaxError
from sql.parser import Parser
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

    #: Binder to perform AST checks against Catalog
    _binder: Binder

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

        self._binder = Binder(catalog_reader, properties_schema)

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

    def run_on_start_query(self, sql: SQL) -> str | None:
        on_start_result = duckdb_to_dicts(self._conn, sql)
        if len(on_start_result) == 0:
            reason = f"Response from '{sql}' is empty. Cannot proceed."
            logger.warning("[{}] {}", self.name, reason)
            return reason
        return

    async def send_client(self, client_id: ClientId, message: str) -> None:
        if client_id != self.name:
            await self.channel_registry.publish(client_id, message)

    async def on_receive(self, client_id: ClientId, sql: SQL) -> None:
        # Process SQL commands from clients, evaluate them, and dispatch results.
        # SQL comes from TCP clients or internal sql file entrypoint
        try:
            ast = Parser.parse(sql)
        except SqlSyntaxError as e:
            return await self.send_client(client_id, str(e))

        try:
            ctx = self._binder.bind(ast)
        except (CatalogError, PlanError) as e:
            return await self.send_client(client_id, str(e))

        # Handle context with on_start eval conditions
        if isinstance(ctx, CreateWSTableContext) and ctx.on_start_query:
            if reason := self.run_on_start_query(ctx.on_start_query):
                return await self.send_client(client_id, reason)

        result = ""

        if isinstance(ctx, (CreateContext, DropContext)):
            self._register_ddl(ctx, sql)
            await self.channel_registry.publish("EntityManager", ctx)
            result = "Success"

        if isinstance(ctx, EvaluableContext):
            result = await self._eval_ctx(ctx)

        await self.send_client(client_id, result)

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
