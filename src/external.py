import trio
import jq as jqm
import httpx
import json

from duckdb import DuckDBPyConnection
from functools import partial
from string import Template
from trio_websocket import open_websocket_url
from typing import Any, Callable, Coroutine, AsyncGenerator

from auth import AUTH_DISPATCHER, BaseSignerT, SecretsHandler

from loguru import logger

MAX_RETRIES = 5


def parse_http_properties(
    properties: dict[str, str],
) -> tuple[dict[str, Any], BaseSignerT, dict[str, Any], dict[str, Any]]:
    """
    Parse property dict provided by context and get required http parameters.
    """
    # Build kwargs dict compatible with both httpx or requests
    requests_kwargs = {}
    requests_kwargs["headers"] = {}
    requests_kwargs["json"] = {}

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
            subkey = key.split(".")[1]

            # TODO: Implement more robust link between executable and SECRET.
            if "SECRET" in value:
                # Get secret_name from value which should respect format:
                # SECRET secret_name
                value = value.split(" ")[1]

                secrets_handler.add(subkey, value)

            requests_kwargs["headers"][subkey] = value

        # Handle json, will always be a dict
        elif key.startswith("json."):
            requests_kwargs["json"][key.split(".")[1]] = value
        # TODO: handle bytes, form and url params

        elif key.startswith("pagination."):
            # Extract subkey after 'pagination.'
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
    attempt = 0
    while attempt < MAX_RETRIES:
        response = await client.request(**signer.sign(conn, request_kwargs))

        try:
            if response.is_success:
                logger.debug(f"{response.status_code}: response for {request_kwargs}.")
                return response
        except Exception:
            pass

        logger.error(
            f"{response.status_code}: request failed {request_kwargs} with {response.text}."
        )

        attempt += 1
        if attempt < MAX_RETRIES:
            delay = attempt
            await trio.sleep(delay)

    logger.error(f"request to {request_kwargs} failed after {MAX_RETRIES} attempts")
    return None


def sync_request(
    client: httpx.Client,
    signer: BaseSignerT,
    request_kwargs: dict[str, Any],
    conn: DuckDBPyConnection,
) -> httpx.Response:
    try:
        response = client.request(**signer.sign(conn, request_kwargs))
        logger.debug("response for {}: {}", request_kwargs, response.status_code)

        if response.is_success:
            return response

        try:
            response.raise_for_status()
        except Exception as e:
            logger.warning(f"unable to request: {e}")

    except Exception as e:
        logger.error(f"HTTP request failed: {e}")

    return None


def parse_response(data: dict[str, Any], jq: Any = None) -> list[dict[str, Any]]:
    res = jq.input(data).all()
    return res


def build_paginated_url(base_url: str, params: dict[str, Any], sep: str = "?") -> str:
    if sep in base_url:
        sep = "&"
    q = "&".join(f"{k}={v}" for k, v in params.items())
    return f"{base_url}{sep}{q}"


async def async_fetch_paginated_data(
    client: httpx.AsyncClient,
    jq: Any,
    base_signer,
    request_kwargs: dict[str, Any],
    meta_kwargs: dict[str, Any],
    conn: DuckDBPyConnection,
) -> list[dict]:
    """
    Unified asynchronous pagination handler
    Supports multiple pagination strategies used by APIs

    Pagination Types
    ----------------
    1. limit_offset (page-based)
       - The API uses numeric offsets or pages (e.g., ?page=1, ?limit=100)
       - Required meta_kwargs:
            pagination_type = "limit_offset"
            pagination_page_param = e.g, "page" or "start"
            pagination_limit_param = e.g, "limit"
            pagination_page_start = 0
       - Example: https://api.coinlore.net/api/tickers?start=0&limit=100

    2. cursor (ID-based or token-based)
       - The API provides a cursor (like an "id" or "nextToken") in response
       - Required meta_kwargs:
            pagination_type = "cursor"
            pagination_cursor_param = # request query param name e.g, "fromId"
            pagination_cursor_id = # JSON key from last item e.g, "a"
       - Example: https://api.binance.com/api/v3/aggTrades?fromId=12345

    3. header (next-cursor via response headers)
       - The API includes the next cursor in a response header
       - Required meta_kwargs:
            pagination_type = "header"
            pagination_next_header = # name of response header e.g, "cb-after"
            pagination_cursor_param = # query param for next call e.g, "after"
       - Example: Coinbase API: "cb-after" header → ?after=xxx

    4. body_link (next URL inside JSON)
       - The response JSON includes a direct “next” URL to call.
       - TODO: implement this

    Optional Controls
    -----------------
    pagination_limit : int
        Maximum number of items per request (default 100)
    pagination_max : int
        Stop after fetching this many total items (safety limit)

    Returns
    -------
    list[dict]
        Flattened list of parsed records across all pages.
    """
    pagination_type = meta_kwargs.get("type")
    results = []

    # pagination params
    limit = meta_kwargs.get("limit", 100)
    limit_param = meta_kwargs.get("limit_param", "limit")
    page_param = meta_kwargs.get("page_param", "page")
    cursor_param = meta_kwargs.get("cursor_param", "cursor")
    cursor_id = meta_kwargs.get("cursor_id", "id")
    next_cursor_header = meta_kwargs.get("next_header")

    # State
    # server decides what the “next” position is
    cursor = None
    # predictable, user decides
    page = int(meta_kwargs.get("pagination_page_start", 0))

    total = 0
    while True:
        # --- Build URL ---
        params = {limit_param: limit}
        if pagination_type == "limit_offset":
            params[page_param] = page
        elif pagination_type in ("header", "cursor", "body_link") and cursor:
            params[cursor_param] = cursor
        elif pagination_type not in ("limit_offset", "cursor", "header", "body_link"):
            # No pagination → single request
            response = await async_request(client, base_signer, request_kwargs, conn)
            return parse_response(response.json(), jq)

        paginated_url = build_paginated_url(request_kwargs["url"], params)

        # --- Execute request ---
        paginated_kwargs = request_kwargs.copy()
        paginated_kwargs["url"] = paginated_url

        response = await async_request(client, base_signer, paginated_kwargs, conn)
        batch = parse_response(response.json(), jq)

        # --- Exit condition ---
        if not batch:
            break

        results.extend(batch)

        if len(batch) < limit:
            break

        total += len(batch)
        if "max" in meta_kwargs and total >= int(meta_kwargs["max"]):
            break

        # --- Update pagination state ---
        if pagination_type == "limit_offset":
            page += 1
        elif pagination_type == "cursor":
            last_item = batch[-1]
            cursor = last_item.get(cursor_id)
            if not cursor:
                break
        elif pagination_type == "header":
            next_cursor_header = meta_kwargs.get("pagination_next_header")
            if next_cursor_header:
                cursor = response.headers.get(next_cursor_header)
            if not cursor:
                break
    return results


def _sync_fetch_paginated_data(
    client: httpx.AsyncClient,
    jq: Any,
    base_signer,
    request_kwargs: dict[str, Any],
    meta_kwargs: dict[str, Any],
    conn: DuckDBPyConnection,
) -> list[dict]:
    pagination_type = meta_kwargs.get("type")
    results = []

    # pagination params
    limit = meta_kwargs.get("limit", 100)
    limit_param = meta_kwargs.get("limit_param", "limit")
    page_param = meta_kwargs.get("page_param", "page")
    cursor_param = meta_kwargs.get("cursor_param", "cursor")
    cursor_id = meta_kwargs.get("cursor_id", "id")
    next_cursor_header = meta_kwargs.get("next_header")

    # State
    # server decides what the “next” position is
    cursor = None
    # predictable, user decides
    page = int(meta_kwargs.get("pagination_page_start", 0))

    total = 0
    while True:
        # --- Build URL ---
        params = {limit_param: limit}
        if pagination_type == "limit_offset":
            params[page_param] = page
        elif pagination_type in ("header", "cursor", "body_link") and cursor:
            params[cursor_param] = cursor
        elif pagination_type not in ("limit_offset", "cursor", "header", "body_link"):
            # No pagination → single request
            response = sync_request(client, base_signer, request_kwargs, conn)
            return parse_response(response.json(), jq)

        paginated_url = build_paginated_url(request_kwargs["url"], params)

        # --- Execute request ---
        paginated_kwargs = request_kwargs.copy()
        paginated_kwargs["url"] = paginated_url

        response = sync_request(client, base_signer, paginated_kwargs, conn)
        batch = parse_response(response.json(), jq)

        # --- Exit condition ---
        if not batch:
            break

        results.extend(batch)

        if len(batch) < limit:
            break

        total += len(batch)
        if "max" in meta_kwargs and total >= int(meta_kwargs["max"]):
            break

        # --- Update pagination state ---
        if pagination_type == "limit_offset":
            page += 1
        elif pagination_type == "cursor":
            last_item = batch[-1]
            cursor = last_item.get(cursor_id)
            if not cursor:
                break
        elif pagination_type == "header":
            next_cursor_header = meta_kwargs.get("pagination_next_header")
            if next_cursor_header:
                cursor = response.headers.get(next_cursor_header)
            if not cursor:
                break
    return results


async def async_http_requester(
    jq,
    base_signer: BaseSignerT,
    request_kwargs: dict[str, Any],
    meta_kwargs: dict[str, Any],
    conn: DuckDBPyConnection,
) -> list[dict]:
    async with httpx.AsyncClient() as client:
        logger.debug(f"running request with properties: {request_kwargs}")
        return await async_fetch_paginated_data(
            client, jq, base_signer, request_kwargs, meta_kwargs, conn
        )


def sync_http_requester(
    jq,
    base_signer: BaseSignerT,
    request_kwargs: dict[str, Any],
    meta_kwargs: dict[str, Any],
    conn: DuckDBPyConnection,
) -> list[dict]:
    client = httpx.Client()
    logger.debug(f"running request with properties: {request_kwargs}")
    return _sync_fetch_paginated_data(
        client, jq, base_signer, request_kwargs, meta_kwargs, conn
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
