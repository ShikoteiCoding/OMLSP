import asyncio
import argparse
from prompt_toolkit import PromptSession
from prompt_toolkit.patch_stdout import patch_stdout
from prompt_toolkit.key_binding import KeyBindings
from prompt_toolkit.shortcuts import print_formatted_text
from prompt_toolkit.formatted_text import HTML
from prompt_toolkit.lexers import PygmentsLexer
from prompt_toolkit.styles.pygments import style_from_pygments_cls
from prompt_toolkit.completion import WordCompleter
from pygments.lexers.sql import SqlLexer
from pygments.styles import get_style_by_name


async def show_spinner():
    spinner_chars = [".  ", ".. ", "...", "..."]
    while True:
        for char in spinner_chars:
            print_formatted_text(HTML(f"<b><red>{char}</red></b> "), end="\r")
            await asyncio.sleep(0.1)


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

    sql_lexer = PygmentsLexer(SqlLexer)
    style = style_from_pygments_cls(get_style_by_name("monokai"))
    bindings = KeyBindings()
    sql_completer = WordCompleter(
        ["SELECT", "FROM", "WHERE", "GROUP BY", "ORDER BY", "JOIN"], ignore_case=True
    )

    @bindings.add("enter")
    def _(event):
        text = event.app.current_buffer.text
        if text.strip().endswith(";"):
            event.app.exit(result=text)
        else:
            event.app.current_buffer.insert_text("\n")

    session = PromptSession(
        f"Client {client_id} > ",
        multiline=True,
        key_bindings=bindings,
        lexer=sql_lexer,
        style=style,
        completer=sql_completer,
    )

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
            spinner_task = asyncio.create_task(show_spinner())
            response = await reader.readuntil(b"\n\n")
            spinner_task.cancel()
            print(" " * 10, end="\r")
            print(f"Client {client_id} > {response.decode().strip()}")
            reader._buffer.clear()  # type: ignore


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Client for sending SELECT queries to a server"
    )
    parser.add_argument("client_id", help="Unique client identifier")
    parser.add_argument(
        "--host", default="127.0.0.1", help="Server address (default: 127.0.0.1)"
    )
    parser.add_argument(
        "--port", type=int, default=8080, help="Server port (default: 8080)"
    )

    args = parser.parse_args()

    asyncio.run(send_query(args.host, args.port, args.client_id))
