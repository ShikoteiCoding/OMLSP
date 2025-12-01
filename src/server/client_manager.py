"""
Client Manager handling TCP SQL requests.
"""

from functools import partial

import trio
from duckdb import DuckDBPyConnection
from loguru import logger

from channel.broker import _get_event_bus, ChannelBroker
from services import Service

ClientId = str


__all__ = ["ClientManager"]


class ClientManager(Service):
    #: Duckdb connection
    _conn: DuckDBPyConnection

    #: EventBus ref
    _event_bus: ChannelBroker

    def __init__(self, conn: DuckDBPyConnection):
        super().__init__(name="ClientManager")
        self._conn = conn

        self._event_bus = _get_event_bus()

    async def on_start(self):
        self._listeners = await self._nursery.start(
            partial(
                trio.serve_tcp,
                handler=self._handle_client,
                port=8080,
                host="0.0.0.0",
            )
        )

        logger.info(
            "[ClientManager] Server running on {}",
            self._listeners[0].socket.getsockname(),
        )

    async def on_stop(self):
        logger.success("[{}] stopping.", self.name)
        # First, cancel the server tasks (serve_tcp etc)
        self._nursery.cancel_scope.cancel()

        # Then close listeners safely
        for listener in getattr(self, "_listeners", []):
            with trio.move_on_after(1):  # don't hang if already closed
                try:
                    await listener.aclose()
                except trio.ClosedResourceError:
                    pass
        logger.success("[{}] stopped.", self.name)

    # TODO: handler of trio.serve_tcp should be awaitable
    # Our current implementation is a never ending loop with
    # custom stream handling. Need to find correct implementation
    async def _handle_client(self, stream: trio.SocketStream):
        client_addr = stream.socket.getpeername()
        client_id = f"{client_addr[0]}:{client_addr[1]}"
        logger.info("[ClientManager] New client connected from {}", client_id)

        async with stream:
            try:
                while True:
                    data = await stream.receive_some(4096)
                    if not data:
                        logger.info("[ClientManager] Client disconnected {}", client_id)
                        break

                    sql_content = data.decode().strip()
                    if not sql_content:
                        continue

                    logger.info("[ClientManager] Client sent query: {}", sql_content)

                    response = await self._process_query(sql_content, client_id)

                    await stream.send_all((response + "\n\n").encode())

            except Exception as e:
                logger.error("[ClientManager] Client error '{}': {}", client_id, e)
                await stream.send_all(f"Error: {str(e)}\n\n".encode())

    async def _process_query(self, sql_content: str, client_id: str) -> str:
        try:
            await self._event_bus.publish(
                "client.sql.requests", (client_id, sql_content)
            )
            output_messages = []

            response = await self._event_bus.consumer(client_id).consume()

            output_messages.append(response)

            return "\n".join(output_messages)

        except Exception as e:
            logger.error(
                f"Client {client_id} - Error processing query: {type(e)} - {e}"
            )
            return f"Error: {str(e)}"
