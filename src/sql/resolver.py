from __future__ import annotations

from typing import TYPE_CHECKING

from context.context import (
    CreateSinkContext,
    CreateViewContext,
    SelectContext,
    ValidContext,
)

if TYPE_CHECKING:
    from catalog.catalog import CatalogReader


class Resolver:
    """
    Resolver for validating context against catalog.
    """

    @staticmethod
    def resolve(ctx: ValidContext, catalog_reader: CatalogReader) -> str | None:
        if isinstance(ctx, SelectContext):
            table = ctx.table
            if (
                not catalog_reader.get_table(table)
                and not catalog_reader.get_view(table)
                and not catalog_reader.get_source(table)
            ):
                if table not in (
                    "information_schema.tables",
                    "information_schema.columns",
                ):  # Basic allowlist for reflection
                    return f"Table '{table}' not found in catalog"

            for join_table in ctx.joins.keys():
                if (
                    not catalog_reader.get_table(join_table)
                    and not catalog_reader.get_view(join_table)
                    and not catalog_reader.get_source(join_table)
                ):
                    return f"Table, View or Source '{join_table}' not found in catalog"

        elif isinstance(ctx, (CreateViewContext, CreateSinkContext)):
            for upstream in ctx.upstreams:
                if (
                    not catalog_reader.get_table(upstream)
                    and not catalog_reader.get_view(upstream)
                    and not catalog_reader.get_source(upstream)
                ):
                    return f"Table, View or Source '{upstream}' not found in catalog"

        return
