import json
import trio

from loguru import logger
from trio_websocket import open_websocket_url, WebSocketConnection
from typing import Any, AsyncGenerator

from transport.utils import jq_dict
from sql.types import SourceWSProperties, JQ


def parse_ws_properties(properties: SourceWSProperties) -> tuple[JQ, str]:
    return properties.jq, properties.url


async def heartbeat(
    ws: WebSocketConnection,
    id: int,
    timeout: int,
    interval: int,
    cancel_event: trio.Event,
):
    """
    Send periodic pings on WebSocket ``ws``.

    Wait up to ``timeout`` seconds to send a ping and receive a pong. Raises
    ``TooSlowError`` if the timeout is exceeded. If a pong is received, then
    wait ``interval`` seconds before sending the next ping.

    This function runs until cancelled by trio.Event
    """
    while not cancel_event.is_set():
        logger.debug("[WS{{{}}}] Pinging ws", id)
        with trio.fail_after(timeout):
            await ws.ping()
        await trio.sleep(interval)


async def ws_generator(
    jq: Any, url: str, nursery: trio.Nursery, cancel_event: trio.Event
) -> AsyncGenerator[list[dict[str, Any]], None]:
    """
    Yield messages from WebSocket connections.

    Periodically send heartbeat keep-alive with ping().

    This function runs until cancelled by trio.Event.
    """
    try:
        async with open_websocket_url(url) as ws:
            id = ws._id
            logger.debug("[WS{{{}}}] Starting websocket generator on {}", id, url)

            # Main message loop
            while not cancel_event.is_set():
                message = await ws.get_message()
                yield jq_dict(json.loads(message), jq)

            logger.debug("[WS{{{}}}] Received cancel event", id)
            await ws.aclose()
    except Exception as e:
        logger.error("ERROR HERE {}", e)


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
