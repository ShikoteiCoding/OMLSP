import trio
import jq as jqm
import httpx
import json
import inspect

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
        key = str(key)
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
            subkey = key.split(".", 1)[1]
            if subkey == "type":
                meta_kwargs["pagination_type"] = value
            elif subkey == "limit":
                meta_kwargs["pagination_limit"] = int(value)
            elif subkey == "limit_param":
                meta_kwargs["pagination_limit_param"] = value
            elif subkey == "page_param":
                meta_kwargs["pagination_page_param"] = value
            elif subkey == "cursor_param":
                meta_kwargs["pagination_cursor_param"] = value
            elif subkey == "cursor_id":
                meta_kwargs["pagination_cursor_id"] = value
            elif subkey == "page_start":
                meta_kwargs["pagination_page_start"] = value
            elif subkey == "max":
                meta_kwargs["max"] = value

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
    jq: Any,
    signer: BaseSignerT,
    request_kwargs: dict[str, Any],
    conn: DuckDBPyConnection,
) -> list[dict]:
    attempt = 0
    while attempt < MAX_RETRIES:
        response = await client.request(**signer.sign(conn, request_kwargs))

        try:
            if response.is_success:
                logger.debug(f"{response.status_code}: response for {request_kwargs}.")
                return parse_response(response.json(), jq)
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
    return []


def sync_request(
    client: httpx.Client,
    jq: Any,
    signer: BaseSignerT,
    request_kwargs: dict[str, Any],
    conn: DuckDBPyConnection,
) -> list[dict]:
    try:
        response = client.request(**signer.sign(conn, request_kwargs))
        logger.debug("response for {}: {}", request_kwargs, response.status_code)

        if response.is_success:
            return parse_response(response.json(), jq)

        try:
            response.raise_for_status()
        except Exception as e:
            logger.warning(f"unable to request: {e}")

    except Exception as e:
        logger.error(f"HTTP request failed: {e}")

    return []


def parse_response(data: dict[str, Any], jq: Any = None) -> list[dict[str, Any]]:
    res = jq.input(data).all()
    return res


def build_paginated_url(base_url, params, sep="?"):
    if sep in base_url:
        sep = "&"
    q = "&".join(f"{k}={v}" for k, v in params.items())
    return f"{base_url}{sep}{q}"


async def fetch_paginated_data(
    request_func: Callable[..., Any],
    client: httpx.AsyncClient,
    jq,
    base_signer,
    request_kwargs: dict[str, Any],
    meta_kwargs: dict[str, Any],
    conn: DuckDBPyConnection,
) -> list[dict]:
    """
    Unified pagination handler supporting:
    - limit_offset (page-based)
    - cursor (cursor-based)

    #TODO need to implement
    - header (next cursor via headers)
    - body_link (next URL inside JSON)
    Works with async or sync request functions
    """
    pagination_type = meta_kwargs.get("pagination_type")
    results = []

    # pagination params
    limit = meta_kwargs.get("pagination_limit", 100)
    limit_param = meta_kwargs.get("pagination_limit_param", "limit")
    page_param = meta_kwargs.get("pagination_page_param", "page")
    cursor_param = meta_kwargs.get("pagination_cursor_param", "cursor")
    cursor_id = meta_kwargs.get("pagination_cursor_id", "id")

    # for later
    # next_cursor_header = meta_kwargs.get("pagination_next_header")
    # next_link_jq = meta_kwargs.get("pagination_next_link_jq")

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
        elif pagination_type == "cursor" and cursor:
            params[cursor_param] = cursor
        elif pagination_type in ("header", "body_link"):
            pass  # handled after first response
        elif pagination_type not in ("limit_offset", "cursor", "header", "body_link"):
            # No pagination → single request
            if inspect.iscoroutinefunction(request_func):
                return await request_func(client, jq, base_signer, request_kwargs, conn)
            return request_func(client, jq, base_signer, request_kwargs, conn)

        paginated_url = build_paginated_url(request_kwargs["url"], params)

        # --- Execute request ---
        paginated_kwargs = request_kwargs.copy()
        paginated_kwargs["url"] = paginated_url

        if inspect.iscoroutinefunction(request_func):
            batch = await request_func(client, jq, base_signer, paginated_kwargs, conn)
        else:
            # Run sync call safely in Trio’s thread context
            batch = await trio.to_thread.run_sync(
                lambda: request_func(client, jq, base_signer, paginated_kwargs, conn)
            )

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

        # for later
        # elif pagination_type == "header":
        #     next_cursor_header = meta_kwargs.get("pagination_next_header")
        #     if next_cursor_header:
        #         cursor = response.headers.get(next_cursor_header)
        #     if not cursor:
        #         break

        # elif pagination_type == "body_link":
        #     next_link_jq = meta_kwargs.get("pagination_next_link_jq")
        #     next_url = jq(next_link_jq, batch)
        #     if not next_url:
        #         break
        #     request_kwargs["url"] = next_url

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
        return await fetch_paginated_data(
            async_request, client, jq, base_signer, request_kwargs, meta_kwargs, conn
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
    return trio.run(
        fetch_paginated_data,
        sync_request,
        client,
        jq,
        base_signer,
        request_kwargs,
        meta_kwargs,
        conn,
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
