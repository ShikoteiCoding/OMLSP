from __future__ import annotations

import trio
import jq as jqm
import json
import abc
from functools import partial
from string import Template
from trio_websocket import open_websocket_url
from typing import Any, Callable, Coroutine, AsyncGenerator, Literal
from loguru import logger
from external import (
    parse_http_properties,
    async_http_requester,
    sync_http_requester,
    parse_response,
    PAGINATION_DISPATCH,
    BasePagination,
)
from auth import BaseSignerT


type TransportMode = Literal["sync", "async"]


class Transport(abc.ABC):
    """Runtime transport object (sync or async)."""

    def __init__(self, properties: dict[str, Any], config: dict[str, str]):
        self.properties = properties
        self.config = config

    @abc.abstractmethod
    def configure(self) -> Transport:
        """Prepare internal state before execution."""
        pass

    @abc.abstractmethod
    def finalize(
        self,
    ) -> Callable:
        """Return the executable callable (sync or async)."""
        pass


class TransportBuilder:
    """Entry-point builder for various transport layers."""

    #: Config for child build (spark like syntax)
    config: dict[str, str]

    def __init__(self, props: dict[str, str]):
        self.properties = props
        self.config = {}

    # allow chaining for mode
    def build(self, name: str) -> Transport:
        if name == "http":
            return HttpTransport(self.properties, self.config)
        elif name == "ws":
            return WSTransport(self.properties, self.config)

        raise ValueError(f"Unknown name: {name}")

    def option(self, key: str, value: Any) -> TransportBuilder:
        self.config[key] = value

        return self


class HttpTransport(Transport):
    #: HTTP requires JQ to parse HTTP response
    jq: Any

    #: HTTP Auth signer with access to corresponding secrets
    signer: BaseSignerT

    #: httpx / requests kwargs, nested values are possible
    request_kwargs: dict[str, dict | str]

    #: Pagination strategy
    strategy: BasePagination

    #: Internal configs from TransportBuilder
    mode: TransportMode

    def __init__(self, properties: dict[str, Any], config: dict[str, str]):
        super().__init__(properties, config)

        # TODO: Create static typing configs per Transport
        # Should follow pydantic Model Params syntax
        self.mode = config["mode"]  # type: ignore

    def configure(self) -> HttpTransport:
        jq, signer, request_kwargs, meta_kwargs = parse_http_properties(self.properties)
        self.jq = jq
        self.signer = signer
        self.request_kwargs = request_kwargs

        # Pagination strategy get pre-compiled as a callable
        pagination_type = meta_kwargs.pop("type")
        self.strategy = PAGINATION_DISPATCH[pagination_type](meta_kwargs)
        return self

    def finalize(
        self,
    ) -> Callable[[Any], list[dict]] | Callable[[Any], Coroutine[Any, Any, list[dict]]]:
        if self.mode == "async":

            async def async_requester(conn):
                return await async_http_requester(
                    self.jq, self.signer, self.request_kwargs, self.strategy, conn
                )

            return async_requester

        elif self.mode == "sync":

            def sync_requester(conn):
                return sync_http_requester(
                    self.jq, self.signer, self.request_kwargs, self.strategy, conn
                )

            return sync_requester

        raise NotImplementedError()


class WSTransport(Transport):
    #: WS requires JQ to parse HTTP response
    jq: Any

    #: WS url
    url: str

    #: List of templates to substitute to url
    _templates_list: list[Any] = []

    def __init__(self, properties: dict[str, Any], config: dict[str, Any]):
        super().__init__(properties, config)

        self._templates_list = config["templates_list"]

    def configure(self) -> WSTransport:
        jq, url = parse_ws_properties(self.properties)
        self.jq = jq
        self.url = url

        return self

    # TODO: As an improvement allow for "pivoted" templating for multi stream
    # For instance binance can merge WS streams into single stream using this
    # /stream?streams=<streamName1>/<streamName2>/<streamName3>
    # How to enable that ? We split parsing between base_url and "generated"
    # stream name (of arbitrary lenght controlled by input)
    # We also need to pivot the on_start_query so it is a list
    def finalize(
        self,
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
        if len(self._templates_list) == 0:
            list_of_properties = [(self.jq, self.url)]
        else:
            # For each template element create a new properties dict
            list_of_properties = [
                (self.jq, Template(self.url).safe_substitute(template))
                for template in self._templates_list
            ]

        # Multiple list of properties needs one ws connection
        # This requires fan-in mechanism through aggregator
        return partial(ws_generator_aggregator, list_of_properties)


def parse_ws_properties(properties: dict[str, str]) -> tuple[Any, str]:
    jq = jqm.compile(properties["jq"])
    url = properties["url"]
    return jq, url


async def ws_generator(
    jq: Any, url: str, nursery: trio.Nursery
) -> AsyncGenerator[list[dict[str, Any]], None]:
    logger.debug("Starting websocket generator on {}", url)
    async with open_websocket_url(url) as ws:
        while True:
            message = await ws.get_message()
            yield parse_response(json.loads(message), jq)


async def ws_generator_aggregator(
    list_of_properties: list[tuple[Any, str]], nursery: trio.Nursery
) -> AsyncGenerator[list[dict[str, Any]], None]:
    """
    Trio version: fan-in multiple ws_generator streams into one.
    """

    # This happens when either no templating was required
    # (no on_start_query condition) or when only 1 element
    # in the start condition to substitute
    if len(list_of_properties) == 1:
        async for msg in ws_generator(
            jq=list_of_properties[0][0], url=list_of_properties[0][1], nursery=nursery
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
        async def consume(jq: Any, url: str):
            async for msg in ws_generator(jq=jq, url=url, nursery=nursery):
                await send.send(msg)

        for jq, url in list_of_properties:
            nursery.start_soon(consume, jq, url)

        async for msg in recv:
            yield msg
