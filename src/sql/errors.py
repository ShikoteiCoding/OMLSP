from __future__ import annotations


class SqlSyntaxError(Exception):
    """
    Error raised during SQL parsing when syntax is invalid.
    """

    def __init__(self, reason: str):
        self.reason = reason
        super().__init__(reason)

    def __str__(self) -> str:
        return self.reason


class PlanError(Exception):
    """
    Error raised during SQL plan extraction when context cannot be built.
    """

    def __init__(self, reason: str):
        self.reason = reason
        super().__init__(reason)

    def __str__(self) -> str:
        return self.reason
