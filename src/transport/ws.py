import json
import trio

from loguru import logger
from trio_websocket import open_websocket_url
from typing import Any, AsyncGenerator

from transport.utils import jq_dict
from sql.types import SourceWSProperties, JQ


def parse_ws_properties(properties: SourceWSProperties) -> tuple[JQ, str]:
    return properties.jq, properties.url


async def ws_generator(
    jq: Any, url: str, nursery: trio.Nursery, cancel_event: trio.Event
) -> AsyncGenerator[list[dict[str, Any]], None]:
    logger.debug("Starting websocket generator on {}", url)
    async with open_websocket_url(url) as ws:
        while not cancel_event.is_set():
            message = await ws.get_message()
            yield jq_dict(json.loads(message), jq)


async def ws_generator_aggregator(
    list_of_properties: list[tuple[Any, str]],
    nursery: trio.Nursery,
    cancel_event: trio.Event,
) -> AsyncGenerator[list[dict[str, Any]], None]:
    """
    Trio version: fan-in multiple ws_generator streams into one.
    """

    # This happens when either no templating was required
    # (no on_start_query condition) or when only 1 element
    # in the start condition to substitute
    if len(list_of_properties) == 1:
        async for msg in ws_generator(
            jq=list_of_properties[0][0],
            url=list_of_properties[0][1],
            nursery=nursery,
            cancel_event=cancel_event,
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
            async for msg in ws_generator(
                jq=jq, url=url, nursery=nursery, cancel_event=cancel_event
            ):
                await send.send(msg)

        for jq, url in list_of_properties:
            nursery.start_soon(consume, jq, url)

        async for msg in recv:
            yield msg
