import trio
import jq as jqm
from duckdb import DuckDBPyConnection
from typing import Any, Callable, Coroutine, AsyncGenerator
from loguru import logger
from external import (
    parse_http_properties,
    async_http_requester,
    sync_http_requester,
    parse_ws_properties,
    build_ws_properties,
    ws_generator,
    ws_generator_aggregator,
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

    def build(self, name: str) -> "TransportBuilder":
        builders  = {
            "async": self._set_async,
            "sync": self._set_sync,
            "http": lambda: HttpBuilder(self),
            "ws": lambda: WsBuilder(self),
            # TODO add WsBuilder
        }
        if name not in builders:
            raise ValueError(f"Unknown build step: {name}")
        return builders [name]()

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

    def finalize(self) -> (
    Callable[[DuckDBPyConnection], Coroutine[Any, Any, list[dict]]]
    | Callable[[DuckDBPyConnection], list[dict]]
):
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


class WsBuilder(TransportBuilder):
    def __init__(self, base: TransportBuilder):
        super().__init__(base.properties)
        self.mode = base.mode
        self.templates_list: list[dict[str, Any]] = []
        self.list_of_properties: list[dict[str, Any]] = []
        self.generator_fn = None

    def configure(self):
        """
        Configure websocket properties.
        Detect templating or start condition logic and build
        list_of_properties for ws connections.
        """
        # Parse base ws properties (same idea as parse_http_properties)
        base_props = self.properties

        # If templating or start query logic is already extracted in properties
        # (for instance from previous stage), handle it here.
        templates_list = base_props.get("templates_list", [])
        if not isinstance(templates_list, list):
            templates_list = []

        # Build the websocket properties
        if len(templates_list) == 0:
            list_of_properties = [parse_ws_properties(base_props)]
        else:
            list_of_properties = [
                parse_ws_properties(build_ws_properties(base_props, template))
                for template in templates_list
            ]

        self.list_of_properties = list_of_properties
        return self

    def finalize(self) -> Callable[[trio.Nursery], AsyncGenerator[list[dict[str, Any]], None]]:
        """
        Produce the websocket generator function.
        Returns a callable like: `lambda nursery: AsyncGenerator`
        """
        def generator(nursery: trio.Nursery):
            return ws_generator_aggregator(self.list_of_properties, nursery)

        return generator

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

    # Inject templates_list into properties so WsBuilder can handle them
    ws_properties = properties.copy()
    ws_properties["templates_list"] = templates_list

    # Use Builder chain
    ws_gen = (
        TransportBuilder(ws_properties)
            .build("async")
            .build("ws")
            .configure()
            .finalize()
    )

    # Return the callable expected by higher-level continuous source
    return ws_gen


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
