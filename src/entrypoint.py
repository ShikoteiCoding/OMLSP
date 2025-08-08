import asyncio
import polars as pl
from loguru import logger
from duckdb import connect, DuckDBPyConnection

from parser import parse_sql_statements

async def handle_client(
    reader: asyncio.StreamReader, writer: asyncio.StreamWriter, con: DuckDBPyConnection, properties_schema: dict
) -> None:
    addr = writer.get_extra_info("peername")

    client_id_data = await reader.readuntil(b"\n")
    client_id = client_id_data.decode().strip()
    logger.info(f"Client {client_id} connected from {addr}")

    query_lines = []
    while True:
        try:
            data = await reader.readuntil(b"\n")
        except asyncio.IncompleteReadError:
            break

        query_part = data.decode().strip()
        if query_part.lower() == "exit":
            logger.info(f"Client {client_id} disconnected from {addr}")
            break

        query_lines.append(query_part)
        full_query = " ".join(query_lines)

        if full_query.strip().endswith(";"):
            query_lines = []
            try:
                logger.info(f"Client {client_id} - Received query: {full_query}")
                create_queries, select_queries = parse_sql_statements(
                    full_query, properties_schema
                )
                logger.debug(
                    f"Client {client_id} - Create: {create_queries}, Select: {select_queries}"
                )
                cursor = con.execute(select_queries[0]["query"])
                rows = cursor.fetchall()

                if cursor.description:
                    columns = [desc[0] for desc in cursor.description]
                    df = pl.DataFrame(rows, schema=columns, orient="row")
                    writer.write(f"{df.head()}\n\n".encode())
                else:
                    writer.write("No rows returned\n\n".encode())
                await writer.drain()

            except Exception as e:
                logger.error(f"Client {client_id} - Error: {e}")
                writer.write(f"Error: {e}\n\n".encode())
                await writer.drain()

    writer.close()
    await writer.wait_closed()


async def start_server(con: DuckDBPyConnection, properties_schema: dict) -> None:
    server = await asyncio.start_server(
        lambda r, w: handle_client(r, w, con, properties_schema), "0.0.0.0", 8080
    )
    logger.info(f"Server running on {server.sockets[0].getsockname()}")
    async with server:
        await server.serve_forever()