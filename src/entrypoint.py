import asyncio

from loguru import logger
from duckdb import DuckDBPyConnection

from engine import start_background_runnners_or_register, handle_select_or_set
from parser import (
    extract_query_contexts,
    CreateLookupTableContext,
    CreateTableContext,
    InvalidContext,
    SelectContext,
    SetContext,
    CommandContext,
)

query_queue = asyncio.Queue()


async def process_queries(con: DuckDBPyConnection, properties_schema: dict) -> None:
    """
    Process CREATE and SELECT queries
    """
    while True:
        sql_content, writer, client_id = await query_queue.get()
        try:
            logger.info(f"Client {client_id} - Received query: {sql_content.strip()}")
            ordered_query_contexts = extract_query_contexts(
                sql_content, properties_schema
            )
            logger.debug(
                f"Client {client_id} - Submitted statements: {ordered_query_contexts}"
            )

            for context in ordered_query_contexts:
                if isinstance(context, (CreateTableContext, CreateLookupTableContext)):
                    # TODO: wrap in handle_create func
                    asyncio.create_task(
                        start_background_runnners_or_register(context, con)
                    )
                    writer.write("query sent\n\n".encode())
                elif isinstance(context, (SelectContext, SetContext, CommandContext)):
                    output = handle_select_or_set(con, context)
                    writer.write(f"{output}\n\n".encode())

                elif isinstance(context, InvalidContext):
                    output = context.reason
                    writer.write(f"{output}\n\n".encode())

        except Exception as e:
            logger.error(f"Client {client_id} - Error processing query: {e}")
            writer.write(f"Error: {str(e)}\n\n".encode())
        finally:
            await writer.drain()
            query_queue.task_done()


async def handle_client(
    reader: asyncio.StreamReader, writer: asyncio.StreamWriter
) -> None:
    addr = writer.get_extra_info("peername")

    client_id_data = await reader.readuntil(b"\n")
    client_id = client_id_data.decode().strip()
    logger.info(f"Client {client_id} connected from {addr}")

    query_lines = []
    while True:
        data = await reader.readuntil(b"\n")
        query_part = data.decode().strip()
        if query_part.lower() == "exit":
            logger.info(f"Client {client_id} disconnected from {addr}")
            break
        if query_part:
            query_lines.append(query_part)
            full_query = " ".join(query_lines)
            if full_query.strip().endswith(";"):
                await query_queue.put((full_query, writer, client_id))
                query_lines = []

    writer.close()
    await writer.wait_closed()


async def start_server(con: DuckDBPyConnection, properties_schema: dict) -> None:
    """
    Start a TCP server to accept client connections
    """
    server = await asyncio.start_server(handle_client, "0.0.0.0", 8080)

    logger.info(f"Server running on {server.sockets[0].getsockname()}")
    tasks = [
        asyncio.create_task(server.serve_forever()),
        asyncio.create_task(process_queries(con, properties_schema)),
    ]
    _, _ = await asyncio.wait(tasks, return_when=asyncio.ALL_COMPLETED)
