import trio
import jq as jqm
import json
from duckdb import DuckDBPyConnection
from functools import partial
from string import Template
from trio_websocket import open_websocket_url
from typing import Any, Callable, Coroutine, AsyncGenerator
from loguru import logger
from external import (
    parse_http_properties,
    async_http_requester,
    sync_http_requester,
    parse_response,
    STRATEGIES,
    NoPagination,
)


# ============================================================
# Transport Builder
# ============================================================


class TransportBuilder:
    def __init__(self, props: dict[str, Any]):
        self.properties = props
        self.mode = None

    def build(self, name: str):
        mapping = {
            "async": self._set_async,
            "sync": self._set_sync,
            "http": lambda: HttpBuilder(self),
            # TODO add WsBuilder
        }
        if name not in mapping:
            raise ValueError(f"Unknown build step: {name}")
        return mapping[name]()

    def _set_async(self):
        self.mode = "async"
        return self

    def _set_sync(self):
        self.mode = "sync"
        return self


class HttpBuilder(TransportBuilder):
    def __init__(self, base: TransportBuilder):
        super().__init__(base.properties)
        self.mode = base.mode
        self.signer = getattr(base, "signer", None)
        self.jq = getattr(base, "jq", None)
        self.meta_kwargs = {}
        self.request_kwargs = {}
        self.strategy = NoPagination
    
    def configure(self):
        jq, signer, request_kwargs, meta_kwargs = parse_http_properties(self.properties)
        self.jq = jq
        self.signer = signer
        self.request_kwargs = request_kwargs
        self.meta_kwargs = meta_kwargs

        pagination_type = meta_kwargs.get("type") if meta_kwargs else None 
        self.strategy = STRATEGIES.get(pagination_type, NoPagination)(meta_kwargs)
        return self

    def finalize(self):
        strategy = self.strategy

        if self.mode == "async":

            async def requester(conn):
                return await async_http_requester(
                    self.jq, self.signer, self.request_kwargs, strategy, conn
                )

            return requester

        else:

            def requester(conn):
                return sync_http_requester(
                    self.jq, self.signer, self.request_kwargs, strategy, conn
                )

            return requester


def build_http_requester(
    properties: dict[str, Any], is_async: bool = True
) -> (
    Callable[[DuckDBPyConnection], Coroutine[Any, Any, list[dict]]]
    | Callable[[DuckDBPyConnection], list[dict]]
):
    mode = "async" if is_async else "sync"
    return TransportBuilder(properties).build(mode).build("http").configure().finalize()


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
