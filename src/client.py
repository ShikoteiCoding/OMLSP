import asyncio
import sys


async def send_query(host: str, port: int, client_id: str) -> None:
    """
    Connect to the server and send SELECT queries
    """
    reader, writer = await asyncio.open_connection(host, port)
    print(
        f"Client {client_id} connected to {host}:{port}, write SELECT queries or exit to quit:"
    )

    writer.write(f"{client_id}\n".encode())
    await writer.drain()

    while True:
        query = input(f"Client {client_id} > ").strip()
        if query.lower() == "exit":
            writer.write(b"exit\n")
            await writer.drain()
            break
        writer.write(f"{query}\n".encode())
        await writer.drain()
        response = await reader.readuntil(b"\n\n")
        print(f"Client {client_id} > {response.decode().strip()}")
        reader._buffer.clear()


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python client.py <client_id>")
        sys.exit(1)

    client_id = sys.argv[1]
    asyncio.run(send_query("127.0.0.1", 8080, client_id))
