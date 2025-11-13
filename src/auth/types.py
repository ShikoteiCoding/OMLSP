"""Type classes for :mod:`omlsp.auth`."""

import abc

from duckdb import DuckDBPyConnection
from typing import Any

__all__ = ["BaseSignerT"]


class SecretsHandlerT(abc.ABC):
    @abc.abstractmethod
    def _add(self):
        pass

    @abc.abstractmethod
    def render(
        self, conn: DuckDBPyConnection, request_kwargs: dict[str, Any]
    ) -> dict[str, Any]:
        pass


class BaseSignerT(abc.ABC):
    def __init__(self, secret_handler: SecretsHandlerT):
        self.secret_handler = secret_handler

    @abc.abstractmethod
    def sign(
        self, conn: DuckDBPyConnection, request_kwargs: dict[str, Any]
    ) -> dict[str, Any]:
        pass
