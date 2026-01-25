from __future__ import annotations

from typing import TYPE_CHECKING, Any

from sqlglot import exp


from sql.plan import extract_one_query_context

if TYPE_CHECKING:
    from catalog.catalog import CatalogReader
    from context.context import InvalidContext, ValidContext


class Binder:
    """
    Binder for validating AST against catalog and binding to Context.
    """

    #: Catalog read access
    _catalog_reader: CatalogReader

    #: JSONSchema for DDL propreties validation
    _properties_schema: dict[str, Any]

    def __init__(
        self,
        catalog_reader: CatalogReader,
        properties_schema: dict[str, Any],
    ):
        self._catalog_reader = catalog_reader
        self._properties_schema = properties_schema

    def bind(self, ast: exp.Expression) -> InvalidContext | ValidContext:
        """
        Bind AST to Context after validating dependencies.
        """
        if validation_error := self._validate_dependencies(ast):
            return InvalidContext(reason=validation_error)

        return extract_one_query_context(ast, self._properties_schema)

    def _validate_dependencies(self, ast: exp.Expression) -> str | None:
        if isinstance(ast, exp.Select):
            return self._check_tables_exist(ast)

        if isinstance(ast, exp.Create):
            query = ast.expression
            if query and isinstance(query, exp.Select):
                return self._check_tables_exist(query)

        return None

    def _check_tables_exist(self, expression: exp.Expression) -> str | None:
        """
        Traverse the expression to find all tables and verify they exist in the catalog.
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
                return f"Table, View or Source '{table_name}' not found in catalog"

        return None
