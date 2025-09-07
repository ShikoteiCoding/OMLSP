import trio

from functools import partial
from trio import SocketListener

from loguru import logger
from duckdb import DuckDBPyConnection

from commons.utils import Channel
from engine.engine import execute_eval_ctx
from context.context import (EvalContext, InvalidContext)


class ClientManager:
    _nursery: trio.Nursery
    _running: bool
    _sql_channel: Channel[str]
    _evalctx_channel: Channel[EvalContext | InvalidContext]

    def __init__(self, conn: DuckDBPyConnection):
        self.conn = conn

    async def add_sql_channel(self, channel: Channel[str]):
        self._sql_channel = channel

    async def add_evalctx_channel(self, channel: Channel[EvalContext | InvalidContext]):
        self._evalctx_channel = channel

    async def run(self):
        logger.error(f"[ClientManager] start")
        self._running = True

        async with trio.open_nursery() as nursery:
            self._nursery = nursery

            listeners: list[SocketListener] = await nursery.start(
                partial(
                    trio.serve_tcp, handler=self._handle_client, port=8080, host="0.0.0.0"
                )
            )

            logger.info(f"[ClienManager] Server running on {listeners[0].socket.getsockname()}")

            nursery.start_soon(self._watch_for_shutdown)

            logger.info("[ClienManager] Started.")

    async def _handle_client(self, stream: trio.SocketStream):
        client_addr = stream.socket.getpeername()
        logger.info(f"[Client] Connected from {client_addr}")

        async with stream:
            try:
                while True:
                    data = await stream.receive_some(4096)
                    if not data:
                        logger.info(f"[Client] Disconnected {client_addr}")
                        break

                    sql_content = data.decode().strip()
                    if not sql_content:
                        continue

                    logger.info(f"[Client] Received query: {sql_content}")

                    response = await self._process_query(sql_content, client_addr)

                    await stream.send_all((response + "\n\n").encode())

            except Exception as e:
                logger.error(f"[Client] Error with {client_addr}: {e}")
                await stream.send_all(f"Error: {str(e)}\n\n".encode())

    async def _process_query(self, sql_content: str, client_id) -> str:
        try:
            await self._sql_channel.send(sql_content)
            output_messages = []

            # exactly one element
            eval_ctx = await self._evalctx_channel.recv() 

            if isinstance(eval_ctx, InvalidContext):
                output_messages.append(eval_ctx.reason)
            else:
                result = await execute_eval_ctx(self.conn, eval_ctx)
                output_messages.append(result)

            return "\n".join(output_messages)

        except Exception as e:
            logger.error(f"Client {client_id} - Error processing query: {e}")
            return f"Error: {str(e)}"
        
    async def _watch_for_shutdown(self):
        try:
            while self._running:
                await trio.sleep(1)
        finally:
            logger.info("[Runner] Shutdown initiated.")
            self._running = False
