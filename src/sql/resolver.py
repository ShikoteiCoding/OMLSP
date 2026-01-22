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
    def resolve(ctx: ValidContext, catalog_reader: CatalogReader) -> bool:
        if isinstance(ctx, SelectContext):
            if (
                not catalog_reader.get_table(ctx.table)
                and not catalog_reader.get_view(ctx.table)
                and not catalog_reader.get_source(ctx.table)
            ):
                if ctx.table not in (
                    "information_schema.tables",
                    "information_schema.columns",
                ):  # Basic allowlist for reflection
                    return False
            for join_table in ctx.joins.keys():
                if (
                    not catalog_reader.get_table(join_table)
                    and not catalog_reader.get_view(join_table)
                    and not catalog_reader.get_source(join_table)
                ):
                    return False

        elif isinstance(ctx, (CreateViewContext, CreateSinkContext)):
            for upstream in ctx.upstreams:
                if (
                    not catalog_reader.get_table(upstream)
                    and not catalog_reader.get_view(upstream)
                    and not catalog_reader.get_source(upstream)
                ):
                    return False

        return True
