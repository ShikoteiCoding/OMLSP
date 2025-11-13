from __future__ import annotations

import re

from duckdb import DuckDBPyConnection
from typing import Any

from auth.types import SecretsHandlerT
from metadata import get_secret_value_by_name


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
    def init(cls, headers: dict[str, str]) -> SecretsHandler:
        new = cls.__new__(cls)
        pattern = re.compile(r"^SECRET\s+(.+)$")

        # NOTE: For now we assume the secret exists and is
        # valid and has not been removed in-between. Which
        # also means this headers parsing could be moved
        # elsewhere instead
        for key, value in headers.items():
            if match := re.match(pattern, value.strip()):
                if match:
                    new._add(key, str(match.group(1)))

        return new

    def _add(self, header_subkey: str, secret_name: str):
        self.paths.append((header_subkey, secret_name))

    def render(self, conn: DuckDBPyConnection, request_kwargs: dict[str, Any]):
        if not self.is_rendered:
            # Get secrets from db
            for header_key, secret_name in self.paths:
                secret = get_secret_value_by_name(conn, secret_name)
                request_kwargs["headers"][header_key] = secret
                self.cache[header_key] = secret

            # Make sure this is rendered to avoid future reloads
            self.is_rendered = True
            return request_kwargs

        # Get rendering from cache
        for header_key, secret_name in self.paths:
            request_kwargs["headers"][header_key] = self.cache[header_key]
        return request_kwargs
