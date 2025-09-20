from functools import partial

import trio
from duckdb import DuckDBPyConnection
from loguru import logger
from trio import SocketListener

from channel import Channel

ClientId = str


class ClientManager:
    _nursery: trio.Nursery
    _running: bool
    _sql_channel: Channel[tuple[ClientId, str]]
    _response_channel: Channel[tuple[ClientId, str]]

    def __init__(self, conn: DuckDBPyConnection):
        self.conn = conn
        self._client_id_to_channel: dict[ClientId, Channel[str]] = {}

    async def add_sql_channel(self, channel: Channel[tuple[ClientId, str]]):
        self._sql_channel = channel

    async def add_response_channel(self, channel: Channel[tuple[ClientId, str]]):
        self._response_channel = channel

    async def run(self):
        self._running = True

        async with trio.open_nursery() as nursery:
            self._nursery = nursery

            listeners: list[SocketListener] = await nursery.start(
                partial(
                    trio.serve_tcp,
                    handler=self._handle_client,
                    port=8080,
                    host="0.0.0.0",
                )
            )
            nursery.start_soon(self._dispatch_client_responses)
            nursery.start_soon(self._watch_for_shutdown)
            logger.info(
                f"[ClienManager] Server running on {listeners[0].socket.getsockname()}"
            )
            logger.info("[ClienManager] Started.")

    async def _handle_client(self, stream: trio.SocketStream):
        client_addr = stream.socket.getpeername()
        client_id = f"{client_addr[0]}:{client_addr[1]}"
        logger.info(f"[Client] Connected from {client_id}")

        # TODO: handle client closure (close channel etc)
        if client_id not in self._client_id_to_channel:
            self._client_id_to_channel[client_id] = Channel[str](10)

        async with stream:
            try:
                while True:
                    data = await stream.receive_some(4096)
                    if not data:
                        logger.info(f"[Client] Disconnected {client_id}")
                        break

                    sql_content = data.decode().strip()
                    if not sql_content:
                        continue

                    logger.info(f"[Client] Received query: {sql_content}")

                    response = await self._process_query(sql_content, client_id)

                    await stream.send_all((response + "\n\n").encode())

            except Exception as e:
                logger.error(f"[Client] Error with {client_id}: {e}")
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
        async for client_id, response in self._response_channel:
            client_channel = self._client_id_to_channel.get(client_id)
            if client_channel:
                await client_channel.send(response)
            logger.debug(f"[ClientManager] Re-routing eval response to {client_id}")

    async def _watch_for_shutdown(self):
        try:
            while self._running:
                await trio.sleep(1)
        finally:
            logger.info("[Runner] Shutdown initiated.")
            self._running = False
