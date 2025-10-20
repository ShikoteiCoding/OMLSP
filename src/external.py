from __future__ import annotations

import trio
import jq as jqm
import httpx
import time
from duckdb import DuckDBPyConnection
from typing import Any

from auth import AUTH_DISPATCHER, BaseSignerT, SecretsHandler
from loguru import logger
from typing import Protocol

MAX_RETRIES = 5


def parse_http_properties(
    properties: dict[str, str],
) -> tuple[Any, BaseSignerT, dict[str, Any], dict[str, Any]]:
    """Parse property dict provided by context and get required http parameters."""
    # Build kwargs dict compatible with both httpx or requests
    requests_kwargs: dict[str, Any] = {
        "headers": {},
        "json": {},
        "params": {},
    }
    meta_kwargs = {"type": "no-pagination"}

    # Manually extract standard values
    requests_kwargs["url"] = properties["url"]
    requests_kwargs["method"] = properties["method"]
    jq = jqm.compile(properties["jq"])

    # TODO: Move this the same way as Pagination in the
    # TransportBuilder layer -> We should align dynamic
    # dispatched instanciation.
    secrets_handler = SecretsHandler()
    signer_class = AUTH_DISPATCHER[requests_kwargs.get("signer_class", "NoSigner")](
        secrets_handler
    )

    # Build "dict" kwargs dynamically
    # TODO: improve with defaultdict(dict)
    for key, value in properties.items():
        # Handle headers, will always be a dict
        if key.startswith("headers."):
            # TODO: Implement more robust link between executable and SECRET.
            subkey = key.split(".", 1)[1]
            if "SECRET" in value:
                # Get secret_name from value which should respect format:
                # SECRET secret_name
                value = value.split(" ")[1]
                secrets_handler.add(subkey, value)
            requests_kwargs["headers"][subkey] = value

        elif key.startswith("json."):
            requests_kwargs["json"][key.split(".", 1)[1]] = value
            # TODO: handle bytes, form

        elif key.startswith("params."):
            requests_kwargs["params"][key.split(".", 1)[1]] = value

        elif key.startswith("pagination."):
            subkey = key.split(".", 1)[1]
            meta_kwargs[subkey] = value

    # httpx consider empty headers or json as an actual headers or json
    # that it will encode to the server. Some API do not like this and
    # will issue 403 malformed error code. Let's just pop them.
    if requests_kwargs["headers"] == {}:
        requests_kwargs.pop("headers")
    if requests_kwargs["json"] == {}:
        requests_kwargs.pop("json")
    if requests_kwargs["params"] == {}:
        requests_kwargs.pop("params")

    return jq, signer_class, requests_kwargs, meta_kwargs


async def async_request(
    client: httpx.AsyncClient,
    signer: BaseSignerT,
    request_kwargs: dict[str, Any],
    conn: DuckDBPyConnection,
) -> httpx.Response | None:
    """Retry async HTTP request with exponential backoff."""
    attempt = 0
    while attempt < MAX_RETRIES:
        try:
            response = await client.request(**signer.sign(conn, request_kwargs))
            if response.is_success:
                logger.debug(f"{response.status_code}: response for {request_kwargs}.")
                return response
        except Exception as e:
            logger.warning(f"Async request failed (attempt {attempt + 1}): {e}")

        attempt += 1
        await trio.sleep(2**attempt)

    logger.error(f"request to {request_kwargs} failed after {MAX_RETRIES} attempts")
    return None


def sync_request(
    client: httpx.Client,
    signer: BaseSignerT,
    request_kwargs: dict[str, Any],
    conn: DuckDBPyConnection,
) -> httpx.Response | None:
    """Retry sync HTTP request with exponential backoff."""
    attempt = 0
    while attempt < MAX_RETRIES:
        try:
            response = client.request(**signer.sign(conn, request_kwargs))
            if response.is_success:
                logger.debug(f"{response.status_code}: response for {request_kwargs}.")
                return response
            response.raise_for_status()
        except Exception as e:
            logger.warning(f"Sync request failed (attempt {attempt + 1}): {e}")

        attempt += 1
        time.sleep(2**attempt)

    logger.error(f"request to {request_kwargs} failed after {MAX_RETRIES} attempts")
    return None


def parse_response(data: dict[str, Any], jq: Any = None) -> list[dict[str, Any]]:
    """Apply jq transformation to parsed JSON."""
    return jq.input(data).all()


class Pagination(Protocol):
    def update_params(self):
        pass

    def extract_next_cursor(self):
        pass

    def has_cursor(self):
        pass


class BasePagination:
    def __init__(self, meta: dict[str, Any]):
        self.meta = meta

    def update_params(self, cursor: str, params: dict[str, str]):
        pass

    def extract_next_cursor(
        self, batch: list[dict[str, Any]], response: httpx.Response
    ):
        pass

    def has_cursor(self):
        pass


class NoPagination(BasePagination):
    def __init__(self, meta: dict[str, Any]):
        self.meta = meta

    def update_params(self, cursor: str, params: dict[str, str]) -> dict:
        return {}

    def extract_next_cursor(
        self, batch: list[dict[str, Any]], response: httpx.Response
    ):
        """Return the next cursor or None"""
        return None

    def has_cursor(self) -> bool:
        """Returns True if this strategy uses cursors"""
        return False


class PageBasedPagination(BasePagination):
    def __init__(self, meta):
        super().__init__(meta)
        self.page_param = meta.get("page_param", "page")
        self.size_param = meta.get("size_param", "limit")
        self.page = 0

    def update_params(self, cursor: str, params: dict[str, Any]):
        params[self.page_param] = self.page
        self.page += 1
        return params


class CursorBasedPagination(BasePagination):
    def __init__(self, meta):
        super().__init__(meta)
        self.cursor_param = meta.get("cursor_param", "cursor")
        self.cursor_id = meta.get("cursor_id", "id")

    def update_params(self, cursor: str, params: dict[str, str]):
        if cursor:
            params[self.cursor_param] = cursor
        return params

    def has_cursor(self) -> bool:
        return True

    def extract_next_cursor(
        self, batch: list[dict[str, Any]], response: httpx.Response
    ):
        return batch[-1].get(self.cursor_id)


class HeaderBasedPagination(BasePagination):
    def __init__(self, meta):
        super().__init__(meta)
        self.cursor_param = meta.get("cursor_param", "cursor")
        self.next_cursor_header = meta.get("next_header")

    def update_params(self, cursor: str, params: dict[str, str]):
        if cursor:
            params[self.cursor_param] = cursor
        return params

    def has_cursor(self) -> bool:
        return True

    def extract_next_cursor(
        self, batch: list[dict[str, Any]], response: httpx.Response
    ):
        if not self.next_cursor_header:
            return None
        return response.headers.get(self.next_cursor_header)


PAGINATION_DISPATCH: dict[str, type[BasePagination]] = {
    "no-pagination": NoPagination,
    "page-based": PageBasedPagination,
    "cursor-based": CursorBasedPagination,
    "header-based": HeaderBasedPagination,
}


async def async_http_requester(
    jq,
    base_signer: BaseSignerT,
    request_kwargs: dict[str, Any],
    pagination_strategy: BasePagination,
    conn: DuckDBPyConnection,
) -> list[dict]:
    async with httpx.AsyncClient() as client:
        logger.debug(f"running async request with {request_kwargs}")
        results, cursor = [], None
        base_params = request_kwargs.get("params", {}).copy()
        limit = int(base_params.get("limit", 100))

        while True:
            params = pagination_strategy.update_params(cursor, base_params.copy())
            req = {**request_kwargs, "params": params}
            if req["params"] == {}:
                req.pop("params")

            response = await async_request(client, base_signer, req, conn)
            if not response:
                break

            batch = parse_response(response.json(), jq)
            if not batch:
                break

            results.extend(batch)
            if len(batch) < limit:
                break

            if pagination_strategy.has_cursor():
                cursor = pagination_strategy.extract_next_cursor(batch, response)
                if cursor is None:
                    break
            else:
                break

        return results


def sync_http_requester(
    jq,
    base_signer: BaseSignerT,
    request_kwargs: dict[str, Any],
    pagination_strategy: BasePagination,
    conn: DuckDBPyConnection,
) -> list[dict]:
    with httpx.Client() as client:
        logger.debug(f"running sync request with {request_kwargs}")
        results, cursor = [], None
        base_params = request_kwargs.get("params", {}).copy()
        limit = int(base_params.get("limit", 100))

        while True:
            params = pagination_strategy.update_params(cursor, base_params.copy())
            req = {**request_kwargs, "params": params}
            if req["params"] == {}:
                req.pop("params")

            response = sync_request(client, base_signer, req, conn)
            if not response:
                break

            batch = parse_response(response.json(), jq)
            if not batch:
                break

            results.extend(batch)
            if len(batch) < limit:
                break

            if pagination_strategy.has_cursor():
                cursor = pagination_strategy.extract_next_cursor(batch, response)
                if cursor is None:
                    break

        return results
