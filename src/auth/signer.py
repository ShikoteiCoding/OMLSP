from __future__ import annotations

from duckdb import DuckDBPyConnection
from typing import Any, Type

from auth.types import BaseSignerT, SecretsHandlerT


__all__ = ["NoSigner", "HMACSHA256", "OAuth2", "AUTH_DISPATCHER"]


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
