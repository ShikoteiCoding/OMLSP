from __future__ import annotations


class CatalogError(Exception):
    """
    Error raised during catalog operations such as binding AST to context.
    """

    def __init__(self, reason: str):
        self.reason = reason
        super().__init__(reason)

    def __str__(self) -> str:
        return self.reason
