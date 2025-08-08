import asyncio
import argparse
from prompt_toolkit import PromptSession
from prompt_toolkit.patch_stdout import patch_stdout
from prompt_toolkit.key_binding import KeyBindings


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

    bindings = KeyBindings()
    @bindings.add('enter')
    def _(event):
        text = event.app.current_buffer.text
        if text.strip().endswith(';'):
            event.app.exit(result=text)
        else:
            event.app.current_buffer.insert_text('\n')

    session = PromptSession(f"Client {client_id} > ", multiline=True, key_bindings=bindings)

    with patch_stdout():
        while True:
            query = await session.prompt_async()
            query = query.strip()
            if query.lower() == "exit":
                writer.write(b"exit\n")
                await writer.drain()
                break
            writer.write(f"{query}\n".encode())
            await writer.drain()
            response = await reader.readuntil(b"\n\n")
            print(f"Client {client_id} > {response.decode().strip()}")
            reader._buffer.clear()  # type: ignore


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Client for sending SELECT queries to a server")
    parser.add_argument("client_id", help="Unique client identifier")
    parser.add_argument("--host", default="127.0.0.1", help="Server address (default: 127.0.0.1)")
    parser.add_argument("--port", type=int, default=8080, help="Server port (default: 8080)")

    args = parser.parse_args()

    asyncio.run(send_query(args.host, args.port, args.client_id))
