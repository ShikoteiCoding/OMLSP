from __future__ import annotations

import trio
from functools import partial
from string import Template
from typing import (
    Any,
    Callable,
    Coroutine,
    AsyncGenerator,
    Literal,
    cast,
    Protocol,
    TypedDict,
)

from auth.secret_handler import SecretsHandler
from auth.signer import AUTH_DISPATCHER, NoSigner
from auth.types import BaseSignerT
from transport.http import (
    extract_http_properties,
    async_http_requester,
    sync_http_requester,
)
from transport.pagination import BasePagination, PAGINATION_DISPATCH
from transport.ws import parse_ws_properties, ws_generator_aggregator
from sql.types import Properties, SourceHttpProperties, SourceWSProperties, JQ


type TransportMode = Literal["sync", "async"]


class TransportBuilderOptions(TypedDict): ...


class HttpTransportOptions(TransportBuilderOptions):
    mode: TransportMode


class WSTransportOptions(TransportBuilderOptions):
    templates_list: list[str]


class TransportBuilder:
    """Entry-point builder for various transport layers."""

    #: Config for child build (spark like syntax)
    config: TransportBuilderOptions

    def __init__(self, properties: Properties):
        self.properties = properties
        self.config = {}

    # allow chaining for mode
    def build(self, name: str) -> Transport:
        if name == "http":
            return HttpTransport(
                cast(SourceHttpProperties, self.properties),
                cast(HttpTransportOptions, self.config),
            )
        elif name == "ws":
            return WSTransport(
                cast(SourceWSProperties, self.properties),
                cast(WSTransportOptions, self.config),
            )

        raise ValueError(f"Unknown name: {name}")

    def option(self, key: str, value: Any) -> TransportBuilder:
        self.config[key] = value

        return self


class TransportT(Protocol):
    def configure(self) -> TransportT: ...

    def finalize(self) -> Callable: ...


class Transport(TransportT):
    """
    Runtime transport object (sync or async).

    This inherits from TransportT to allow typing of configure() and finalize().
    """

    def __init__(self, properties: Properties, config: TransportBuilderOptions):
        self.properties = properties
        self.config = config


class HttpTransport(Transport):
    #: HTTP requires JQ to parse HTTP response
    jq: JQ

    #: HTTP Auth signer with access to corresponding secrets
    signer: BaseSignerT

    #: httpx / requests kwargs, nested values are possible
    request_kwargs: dict[str, dict | str]

    #: Pagination strategy
    strategy: BasePagination

    #: Internal configs from TransportBuilder
    mode: TransportMode

    def __init__(self, properties: SourceHttpProperties, config: HttpTransportOptions):
        super().__init__(properties, config)

        self.mode = config["mode"]

        # Cast properties
        self.properties = cast(SourceHttpProperties, self.properties)

    def configure(self) -> HttpTransport:
        # Configure SecretsHandler
        self.signer = AUTH_DISPATCHER.get(self.properties.signer_class, NoSigner)(
            SecretsHandler.init(self.properties.headers)
        )

        # Initialize http specific
        jq, request_kwargs, pagination_kwargs = extract_http_properties(self.properties)
        self.jq = jq
        self.request_kwargs = request_kwargs

        # Pagination strategy get pre-compiled as a callable
        pagination_type = pagination_kwargs.pop("type")
        self.strategy = PAGINATION_DISPATCH[pagination_type](pagination_kwargs)
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
    jq: JQ

    #: WS url
    url: str

    #: List of templates to substitute to url
    _templates_list: list[Any] = []

    def __init__(self, properties: SourceWSProperties, config: WSTransportOptions):
        super().__init__(properties, config)

        self._templates_list = config["templates_list"]

    def configure(self) -> WSTransport:
        jq, url = parse_ws_properties(cast(SourceWSProperties, self.properties))
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
    ) -> Callable[
        [trio.Nursery, trio.Event], AsyncGenerator[list[dict[str, Any]], None]
    ]:
        """
        Build a websocket data generator, if multiple templates are provided
        in the templates_list, then one connection is created for each substitute
        template.

        The :func:`ws_generator_aggregator` for now takes care of the fan-in
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
