import asyncio
from loguru import logger
from duckdb import connect, DuckDBPyConnection
from parser import parse_select_to_dict

query_queue = asyncio.Queue()


async def process_queries(con: DuckDBPyConnection) -> None:
    """
    Process SELECT queries
    """
    while True:
        query_text, writer, client_id = await query_queue.get()
        try:
            logger.info(f"Client {client_id} - Received query: {query_text.strip()}")
            parsed_query = parse_select_to_dict(query_text)

            logger.info(f"Client {client_id} - Parsed query: {parsed_query}")
            result = con.execute(query_text).fetchall()  # retrieves all
            if result:
                # TODO change the way to send out result
                result_str = "".join(str(row) for row in result)
                writer.write(f"{result_str}\n\n".encode())
            else:
                writer.write("No rows returned\n\n".encode())
        except Exception as e:
            logger.error(f"Client {client_id} - Error processing query: {e}")
            writer.write(f"Error: {str(e)}\n\n".encode())
        finally:
            await writer.drain()
            query_queue.task_done()


async def handle_client(
    reader: asyncio.StreamReader, writer: asyncio.StreamWriter
) -> None:
    """
    Handle a client connection, reading SELECT queries and adding to queue
    """
    addr = writer.get_extra_info("peername")

    client_id_data = await reader.readuntil(b"\n")
    client_id = client_id_data.decode().strip()
    logger.info(f"Client {client_id} connected from {addr}")

    while True:
        data = await reader.readuntil(b"\n")
        query = data.decode().strip()
        if query.lower() == "exit":
            logger.info(f"Client {client_id} disconnected from {addr}")
            break
        if query:
            await query_queue.put((query, writer, client_id))  # Add query to queue


async def start_server(con: DuckDBPyConnection) -> None:
    """
    Start a TCP server to accept client connections
    """
    server = await asyncio.start_server(handle_client, "0.0.0.0", 8080)

    logger.info(f"Server running on {server.sockets[0].getsockname()}")
    tasks = [
        asyncio.create_task(server.serve_forever()),
        asyncio.create_task(process_queries(con)),
    ]
    _, _ = await asyncio.wait(tasks, return_when=asyncio.ALL_COMPLETED)


if __name__ == "__main__":
    con = connect(database=":memory:")
    asyncio.run(start_server(con))
