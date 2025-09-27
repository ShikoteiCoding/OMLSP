from functools import partial

import trio
from duckdb import DuckDBPyConnection
from loguru import logger

from channel import Channel
from services import Service

ClientId = str


__all__ = ["ClientManager"]


class ClientManager(Service):
    #: Duckdb connection
    conn: DuckDBPyConnection

    #: Incoming SQL from self.submit() or TCP client
    _sql_channel: Channel[tuple[ClientId, str]]

    #: Outgoing SQL result to TCP client
    _evaled_sql: Channel[tuple[ClientId, str]]

    def __init__(self, conn: DuckDBPyConnection):
        super().__init__(name="ClientManager")
        self.conn = conn
        self._client_id_to_channel: dict[ClientId, Channel[str]] = {}

    def add_sql_channel(self, channel: Channel[tuple[ClientId, str]]):
        self._sql_channel = channel

    def add_response_channel(self, channel: Channel[tuple[ClientId, str]]):
        self._evaled_sql = channel

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
            f"[ClientManager] Server running on {self._listeners[0].socket.getsockname()}"
        )

    async def _handle_client(self, stream: trio.SocketStream):
        client_addr = stream.socket.getpeername()
        client_id = f"{client_addr[0]}:{client_addr[1]}"
        logger.info(f"[ClientManager] New client connected from {client_id}")

        # TODO: handle client closure (close channel etc)
        if client_id not in self._client_id_to_channel:
            self._client_id_to_channel[client_id] = Channel[str](10)

        async with stream:
            try:
                while True:
                    data = await stream.receive_some(4096)
                    if not data:
                        logger.info(f"[ClientManager] Client disconnected {client_id}")
                        break

                    sql_content = data.decode().strip()
                    if not sql_content:
                        continue

                    logger.info(f"[ClientManager] Client sent query: {sql_content}")

                    response = await self._process_query(sql_content, client_id)

                    await stream.send_all((response + "\n\n").encode())

            except Exception as e:
                logger.error(f"[ClientManager] Client error '{client_id}': {e}")
                await stream.send_all(f"Error: {str(e)}\n\n".encode())

    async def _process_query(self, sql_content: str, client_id: str) -> str:
        try:
            await self._sql_channel.send((client_id, sql_content))
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
        async for client_id, response in self._evaled_sql:
            client_channel = self._client_id_to_channel.get(client_id)
            if client_channel:
                await client_channel.send(response)
            logger.debug(f"[ClientManager] Re-routing eval response to {client_id}")
