from __future__ import annotations

from duckdb import DuckDBPyConnection
from typing import Any, Type

from _types._types import BaseSignerT, SecretsHandlerT
from metadata import get_secret_value_by_name

__all__ = ["NoSigner", "HMACSHA256", "OAuth2", "AUTH_DISPATCHER"]


class SecretsHandler(SecretsHandlerT):
    """
    This class stores path to Headers sub keys which needs dynamic rendering.

    The idea is to let the signer object render the secret upon signing.
    """

    #: List of headers key to render
    paths: list[tuple[str, str]]

    #: If the secrets handler is already rendered
    is_rendered: bool = False

    #: Local cache of rendered secrets
    #: TODO: carry cache to all metadata functions with TTL
    cache: dict[str, str] = {}

    def __init__(self):
        self.paths: list[tuple[str, str]] = []

    def add(self, header_subkey: str, secret_name: str):
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


class NoSigner(BaseSignerT):
    def __init__(self, secret_handler: SecretsHandlerT):
        super().__init__(secret_handler)

    def sign(
        self, conn: DuckDBPyConnection, request_kwargs: dict[str, Any]
    ) -> dict[str, Any]:
        return self.secret_handler.render(conn, request_kwargs)


class HMACSHA256(BaseSignerT):
    # TODO: implement HMAC-SHA256 signing

    def __init__(self): ...

    def sign(self, request_kwargs: dict[str, Any]) -> dict[str, Any]: ...


class OAuth2(BaseSignerT):
    # TODO: implement OAuth2 signing

    def __init__(self): ...

    def sign(self, request_kwargs: dict[str, Any]) -> dict[str, Any]: ...


AUTH_DISPATCHER: dict[str, Type[BaseSignerT]] = {
    "NoSigner": NoSigner,
    "HMACSHA256": HMACSHA256,
    "OAuth2": OAuth2,
}
