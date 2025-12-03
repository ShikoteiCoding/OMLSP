from __future__ import annotations


from duckdb import DuckDBPyConnection
from typing import Any

from auth.types import SecretsHandlerT
from store import get_secret_value_by_name
from loguru import logger


class SecretsHandler(SecretsHandlerT):
    """
    This class stores path to Headers sub keys which needs dynamic rendering.

    The idea is to let the signer object render the secret upon signing.
    """

    #: List of headers key to render
    paths: list[tuple[str, str]] = []

    #: If the secrets handler is already rendered
    is_rendered: bool = False

    #: Local cache of rendered secrets
    #: TODO: carry cache to all metadata functions with TTL
    cache: dict[str, str] = {}

    @classmethod
    def init(cls, secrets: list[tuple[str, str]]) -> SecretsHandler:
        new = cls.__new__(cls)
        new.paths = secrets
        return new

    def render(self, conn: DuckDBPyConnection, request_kwargs: dict[str, Any]):
        if not self.is_rendered:
            # Get secrets from db
            for header_key, secret_name in self.paths:
                secret = get_secret_value_by_name(conn, secret_name)
                request_kwargs["headers"][header_key] = secret
                self.cache[header_key] = secret
                logger.debug(f"Secret loaded '{secret_name}' for header '{header_key}'")
            # Make sure this is rendered to avoid future reloads
            self.is_rendered = True
            return request_kwargs

        # Get rendering from cache
        for header_key, secret_name in self.paths:
            request_kwargs["headers"][header_key] = self.cache[header_key]
        return request_kwargs
