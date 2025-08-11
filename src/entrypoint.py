import asyncio

from loguru import logger
from duckdb import DuckDBPyConnection

from engine import run_executables, handle_select
from parser import parse_sql_statements

query_queue = asyncio.Queue()


async def process_queries(con: DuckDBPyConnection, properties_schema: dict) -> None:
    """
    Process CREATE and SELECT queries
    """
    while True:
        sql_content, writer, client_id = await query_queue.get()
        try:
            logger.info(f"Client {client_id} - Received query: {sql_content.strip()}")
            create_queries, select_queries = parse_sql_statements(
                sql_content, properties_schema
            )
            logger.debug(
                f"Client {client_id} - Create queries: {create_queries} - Select queries: {select_queries}"
            )

            if create_queries:
                asyncio.create_task(run_executables(create_queries, con))
                writer.write("query sent\n\n".encode())
            if select_queries:
                # TODO: handle multiple queries
                first_query = select_queries[0]
                output = handle_select(con, first_query)
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
