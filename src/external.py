import trio
import jq as jqm
import httpx
import json
import time
from duckdb import DuckDBPyConnection
from functools import partial
from string import Template
from trio_websocket import open_websocket_url
from abc import ABC, abstractmethod
from typing import Any, Callable, Coroutine, AsyncGenerator

from auth import AUTH_DISPATCHER, BaseSignerT, SecretsHandler
from loguru import logger

MAX_RETRIES = 5


# ============================================================
# HTTP Request Helpers
# ============================================================


def parse_http_properties(
    properties: dict[str, str],
) -> tuple[dict[str, Any], BaseSignerT, dict[str, Any], dict[str, Any]]:
    """Parse property dict provided by context and get required http parameters."""
    # Build kwargs dict compatible with both httpx or requests
    requests_kwargs = {"headers": {}, "json": {}, "params": {}}
    meta_kwargs = {}

    # Manually extract standard values
    requests_kwargs["url"] = properties["url"]
    requests_kwargs["method"] = properties["method"]
    jq = jqm.compile(properties["jq"])

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

    return jq, signer_class, requests_kwargs, meta_kwargs


async def async_request(
    client: httpx.AsyncClient,
    signer: BaseSignerT,
    request_kwargs: dict[str, Any],
    conn: DuckDBPyConnection,
) -> httpx.Response:
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
        await trio.sleep(2 ** attempt)

    logger.error(f"request to {request_kwargs} failed after {MAX_RETRIES} attempts")
    return None


def sync_request(
    client: httpx.Client,
    signer: BaseSignerT,
    request_kwargs: dict[str, Any],
    conn: DuckDBPyConnection,
) -> httpx.Response:
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
        time.sleep(2 ** attempt)

    logger.error(f"request to {request_kwargs} failed after {MAX_RETRIES} attempts")
    return None


def parse_response(data: dict[str, Any], jq: Any = None) -> list[dict[str, Any]]:
    """Apply jq transformation to parsed JSON."""
    return jq.input(data).all()


def build_paginated_url(base_url: str, params: dict[str, Any], sep: str = "?") -> str:
    if sep in base_url:
        sep = "&"
    q = "&".join(f"{k}={v}" for k, v in params.items())
    return f"{base_url}{sep}{q}"


# ============================================================
# Pagination Strategy Classes
# ============================================================


class BasicPagination(ABC):
    def __init__(self, meta: dict[str, Any]):
        self.meta = meta

    def update_params(self, cursor: str, params: dict[str, str]) -> dict:
        return {}

    def extract_next_cursor(self, batch: list[dict[str, Any]], response: httpx.Response):
        """Return the next cursor or None"""
        return None

    def has_cursor(self) -> bool:
        """Returns True if this strategy uses cursors"""
        return False

class PageBasedPagination(BasicPagination):
    def __init__(self, meta):
        super().__init__(meta)
        self.page_param = meta.get("page_param", "page")
        self.size_param = meta.get("size_param", "limit")
        self.page = 0

    def update_params(self, cursor: str, params: dict[str, str]):
        params[self.page_param] = self.page
        self.page += 1
        return params


class CursorBasedPagination(BasicPagination):
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

    def extract_next_cursor(self, batch: list[dict[str, Any]], response: httpx.Response):
        return batch[-1].get(self.cursor_id)


class HeaderBasedPagination(BasicPagination):
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

    def extract_next_cursor(self, batch: list[dict[str, Any]], response: httpx.Response):
        return response.headers.get(self.next_cursor_header)


STRATEGIES = {
    None: BasicPagination,
    "page-based": PageBasedPagination,
    "cursor-based": CursorBasedPagination,
    "header-based": HeaderBasedPagination,
}


# ============================================================
# Unified Pagination Handler
# ============================================================


async def fetch_paginated_data(
    client: httpx.AsyncClient | httpx.Client,
    jq: Any,
    signer: BaseSignerT,
    request_kwargs: dict[str, Any],
    meta_kwargs: dict[str, Any],
    conn: DuckDBPyConnection,
    is_async: bool,
) -> list[dict]:
    """Unified pagination handler (works for async and sync)."""
    pagination_type = meta_kwargs.get("type", None)
    strategy = STRATEGIES.get(pagination_type, BasicPagination)(meta_kwargs)
    results, cursor = [], None
    request_fn = async_request if is_async else sync_request
    base_params = request_kwargs.get("params", {}).copy()
    limit = int(base_params.get("limit", 100))

    while True:
        params = strategy.update_params(cursor, base_params.copy())
        req = {**request_kwargs, "params": params}
        if req["params"] == {}:
            req.pop("params")

        response = (
            await request_fn(client, signer, req, conn)
            if is_async
            else request_fn(client, signer, req, conn)
        )
        if not response:
            break

        batch = parse_response(response.json(), jq)
        if not batch:
            break

        results.extend(batch)
        if len(batch) < limit:
            break

        if strategy.has_cursor():
            cursor = strategy.extract_next_cursor(batch, response)
            if cursor is None:
                break

    return results


# ============================================================
# HTTP Requesters
# ============================================================


async def async_http_requester(
    jq,
    base_signer: BaseSignerT,
    request_kwargs: dict[str, Any],
    meta_kwargs: dict[str, Any],
    conn: DuckDBPyConnection,
) -> list[dict]:
    async with httpx.AsyncClient() as client:
        logger.debug(f"running async request with {request_kwargs}")
        return await fetch_paginated_data(
            client, jq, base_signer, request_kwargs, meta_kwargs, conn, True
        )


def sync_http_requester(
    jq,
    base_signer: BaseSignerT,
    request_kwargs: dict[str, Any],
    meta_kwargs: dict[str, Any],
    conn: DuckDBPyConnection,
) -> list[dict]:
    with httpx.Client() as client:
        logger.debug(f"running sync request with {request_kwargs}")
        return trio.run(
            fetch_paginated_data,
            client,
            jq,
            base_signer,
            request_kwargs,
            meta_kwargs,
            conn,
            False,
        )


def build_http_requester(
    properties: dict[str, Any], is_async: bool = True
) -> (
    Callable[[DuckDBPyConnection], Coroutine[Any, Any, list[dict]]]
    | Callable[[DuckDBPyConnection], list[dict]]
):
    jq, base_signer, request_kwargs, meta_kwargs = parse_http_properties(properties)

    if is_async:

        def _async_inner(conn):
            return async_http_requester(
                jq, base_signer, request_kwargs, meta_kwargs, conn
            )

        return _async_inner

    def _sync_inner(conn):
        return sync_http_requester(jq, base_signer, request_kwargs, meta_kwargs, conn)

    return _sync_inner


# ============================================================
# WebSocket Handlers
# ============================================================


def parse_ws_properties(params: dict[str, str]) -> dict[str, Any]:
    parsed_params = {}

    for key, value in params.items():
        if key == "jq":
            parsed_params["jq"] = jqm.compile(value)
        else:
            parsed_params[key] = value

    return parsed_params


def build_ws_properties(
    properties: dict[str, Any], template: dict[str, Any]
) -> dict[str, Any]:
    """
    Build websocket properties with Template substitution
    applied against a single context (template variables).
    """
    new_props: dict[str, Any] = {}

    for key, value in properties.items():
        if key in ["jq"]:  # don't template compile-only keys
            new_props[key] = value
            continue

        if isinstance(value, str):
            new_props[key] = Template(value).safe_substitute(template)
        else:
            new_props[key] = value

    return new_props


async def ws_generator(
    properties: dict[str, Any], nursery: trio.Nursery
) -> AsyncGenerator[list[dict[str, Any]], None]:
    url = properties["url"]
    logger.debug("Starting websocket generator on {}", url)
    async with open_websocket_url(url) as ws:
        while True:
            message = await ws.get_message()
            yield parse_response(json.loads(message), properties["jq"])


async def ws_generator_aggregator(
    list_of_properties: list[dict[str, Any]], nursery: trio.Nursery
) -> AsyncGenerator[list[dict[str, Any]], None]:
    """
    Trio version: fan-in multiple ws_generator streams into one.
    """

    # This happens when either no templating was required
    # (no on_start_query condition) or when only 1 element
    # in the start condition to substitute
    if len(list_of_properties) == 1:
        async for msg in ws_generator(
            properties=list_of_properties[0], nursery=nursery
        ):
            yield msg

    # This happens when start condition has multiple
    # values and thus creating multiple websocket
    # to aggregate in one simple field
    else:
        # Locally scopped channel for agregation purposes
        # size 0 is an attempt to not miss any event
        # this might be blocking operation in case of backpressure
        send, recv = trio.open_memory_channel[list[dict]](10)

        # TODO: Move fan-in mechanism inside the Continuous task
        # so we can reuse for Trasnform Task
        async def consume(props: dict[str, Any]):
            async for msg in ws_generator(properties=props, nursery=nursery):
                await send.send(msg)

        for props in list_of_properties:
            nursery.start_soon(consume, props)

        async for msg in recv:
            yield msg


# TODO: As an improvement allow for "pivoted" templating for multi stream
# For instance binance can merge WS streams into single stream using this
# /stream?streams=<streamName1>/<streamName2>/<streamName3>
# How to enable that ? We split parsing between base_url and "generated"
# stream name (of arbitrary lenght controlled by input)
# We also need to pivot the on_start_query so it is a list
def build_ws_generator(
    properties: dict[str, Any], templates_list: list[dict[str, str]]
) -> Callable[[trio.Nursery], AsyncGenerator[list[dict[str, Any]], None]]:
    """
    Build a websocket data generator, if multiple templates are provided
    in the templates_list, then one connection is created for each substitute
    template. The ws_generator_aggregator for now takes care of the fan-in
    mechanism to make sure only one single output is provided.
    This for now doesn't handle any backpressure (i.e no buffer, locks etc).
    """

    # This happens when on_start_query doesn't exist
    # ie the WS generator has no start conditions
    if len(templates_list) == 0:
        list_of_properties = [parse_ws_properties(properties)]
    else:
        # For each template element create a new properties dict
        list_of_properties = [
            parse_ws_properties(build_ws_properties(properties, template))
            for template in templates_list
        ]

    # Multiple list of properties needs one ws connection
    # This requires fan-in mechanism through aggregator
    return partial(ws_generator_aggregator, list_of_properties)


if __name__ == "__main__":

    async def main():
        async with trio.open_nursery() as nursery:
            properties = {
                "url": "wss://stream.binance.com/ws/ethbtc@miniTicker",
                "jq": jqm.compile("""{
                    event_type: .e,
                    event_time: .E,
                    symbol: .s,
                    close: .c,
                    open: .o,
                    high: .h,
                    low: .l,
                    base_volume: .v,
                    quote_volume: .q
                }"""),
            }

            async for msg in ws_generator(properties, nursery):
                logger.info(msg)

    trio.run(main)
