"""
Client Manager handling TCP SQL requests.
"""

from functools import partial

import trio
from duckdb import DuckDBPyConnection
from loguru import logger

from eventbus.eventbus import _get_event_bus, Consumer, EventBus, Producer
from channel import Channel
from services import Service

ClientId = str


__all__ = ["ClientManager"]


class ClientManager(Service):
    #: Duckdb connection
    _conn: DuckDBPyConnection

    #: TODO
    _client_id_to_channel: dict[ClientId, Channel[str]]

    #: New
    _event_bus: EventBus

    #: Client(s) SQL requests to produce to App
    _client_sql_request_producer: Producer

    #: Client(s) SQL responses to consume from App
    _client_sql_response_consumer: Consumer

    def __init__(self, conn: DuckDBPyConnection):
        super().__init__(name="ClientManager")
        self._conn = conn
        self._client_id_to_channel: dict[ClientId, Channel[str]] = {}

        self._event_bus = _get_event_bus()
        self._client_sql_request_producer = self._event_bus.producer(
            "client.sql.requests"
        )
        self._client_sql_response_consumer = self._event_bus.consumer(
            "client.sql.responses"
        )

    async def on_start(self):
        self._listeners = await self._nursery.start(
            partial(
                trio.serve_tcp,
                handler=self._handle_client,
                port=8080,
                host="0.0.0.0",
            )
        )

        self._nursery.start_soon(self._dispatch_client_responses)

        logger.info(
            "[ClientManager] Server running on {}",
            self._listeners[0].socket.getsockname(),
        )

    async def on_stop(self):
        logger.success(f"[{self.name}] stopping.")
        # First, cancel the server tasks (serve_tcp etc)
        self._nursery.cancel_scope.cancel()

        # Then close listeners safely
        for listener in getattr(self, "_listeners", []):
            with trio.move_on_after(1):  # don't hang if already closed
                try:
                    await listener.aclose()
                except trio.ClosedResourceError:
                    pass
        logger.success(f"[{self.name}] stopped.")

    # TODO: handler of trio.serve_tcp should be awaitable
    # Our current implementation is a never ending loop with
    # custom stream handling. Need to find correct implementation
    async def _handle_client(self, stream: trio.SocketStream):
        client_addr = stream.socket.getpeername()
        client_id = f"{client_addr[0]}:{client_addr[1]}"
        logger.info("[ClientManager] New client connected from {}", client_id)

        # TODO: handle client closure (close channel etc)
        if client_id not in self._client_id_to_channel:
            self._client_id_to_channel[client_id] = Channel[str](10)

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
                logger.error(f"[ClientManager] Client error '{client_id}': {e}")
                await stream.send_all(f"Error: {str(e)}\n\n".encode())

    async def _process_query(self, sql_content: str, client_id: str) -> str:
        try:
            await self._client_sql_request_producer.produce((client_id, sql_content))
            output_messages = []

            current_client_channel = self._client_id_to_channel[client_id]
            response = await current_client_channel.recv()
            output_messages.append(response)

            return "\n".join(output_messages)

        except Exception as e:
            logger.error(
                f"Client {client_id} - Error processing query: {type(e)} - {e}"
            )
            return f"Error: {str(e)}"

    async def _dispatch_client_responses(self):
        """
        Re-route eval response to per-client response channels
        """
        async for client_id, response in self._client_sql_response_consumer.channel:
            client_channel = self._client_id_to_channel.get(client_id)
            if client_channel:
                await client_channel.send(response)
            logger.debug("[ClientManager] Re-routing eval response to {}", client_id)
