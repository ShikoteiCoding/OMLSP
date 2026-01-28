from __future__ import annotations

from typing import TYPE_CHECKING, Any

from sqlglot import exp

from catalog.errors import CatalogError
from context.context import ValidContext
from sql.plan import extract_one_query_context

if TYPE_CHECKING:
    from catalog.catalog import CatalogReader


class Binder:
    #: Catalog read access
    _catalog_reader: CatalogReader
    #: JSONSchema for DDL properties validation
    _properties_schema: dict[str, Any]

    def __init__(
        self,
        catalog_reader: CatalogReader,
        properties_schema: dict[str, Any],
    ):
        self._catalog_reader = catalog_reader
        self._properties_schema = properties_schema

    def bind(self, ast: exp.Expression) -> ValidContext:
        """
        Bind AST to Context after validating dependencies.

        Raises:
            CatalogError: When dependency validation fails or context extraction fails.
        """
        self._validate_dependencies(ast)
        return extract_one_query_context(ast, self._properties_schema)

    def _validate_dependencies(self, ast: exp.Expression) -> None:
        """
        Validate dependencies exist in catalog.

        Raises:
            CatalogError: When a referenced table/view/source is not found.
        """
        if isinstance(ast, exp.Select):
            self._check_tables_exist(ast)
        elif isinstance(ast, exp.Create):
            query = ast.expression
            if query and isinstance(query, exp.Select):
                self._check_tables_exist(query)

    def _check_tables_exist(self, expression: exp.Expression) -> None:
        """
        Traverse the expression to find all tables and verify they exist in the catalog.

        Raises:
            CatalogError: When a table is not found in the catalog.
        """
        for table_exp in expression.find_all(exp.Table):
            table_name = table_exp.name

            if table_name in (
                "information_schema.tables",
                "information_schema.columns",
            ):
                continue
            if (
                not self._catalog_reader.get_table(table_name)
                and not self._catalog_reader.get_view(table_name)
                and not self._catalog_reader.get_source(table_name)
            ):
                raise CatalogError(
                    f"Table, View or Source '{table_name}' not found in catalog"
                )
