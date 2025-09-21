import argparse
import trio

from prompt_toolkit import PromptSession
from prompt_toolkit.completion import WordCompleter
from prompt_toolkit.formatted_text import HTML
from prompt_toolkit.key_binding import KeyBindings
from prompt_toolkit.patch_stdout import patch_stdout
from prompt_toolkit.shortcuts import print_formatted_text
from prompt_toolkit.styles.pygments import style_from_pygments_cls
from pygments.styles import get_style_by_name


async def show_spinner(cancel_scope: trio.CancelScope):
    spinner_chars = [".  ", ".. ", "...", "..."]
    while True:
        for char in spinner_chars:
            print_formatted_text(HTML(f"<b><red>{char}</red></b> "), end="\r")
            await trio.sleep(1)


async def spinner_task(cancel_scope: trio.CancelScope):
    """Wrapper task to run spinner and stop it via CancelScope."""
    with cancel_scope:
        await show_spinner(cancel_scope)


def create_prompt_session(client_id: str) -> PromptSession:
    """Create a configured PromptSession with bindings and completer."""
    style = style_from_pygments_cls(get_style_by_name("monokai"))
    bindings = KeyBindings()

    sql_completer = WordCompleter(
        ["SELECT", "FROM", "WHERE", "GROUP BY", "ORDER BY", "JOIN", "SHOW TABLES"],
        ignore_case=True,
    )

    @bindings.add("enter")
    def _(event):
        text = event.app.current_buffer.text
        if text.strip().endswith(";"):
            event.app.exit(result=text)
        else:
            event.app.current_buffer.insert_text("\n")

    return PromptSession(
        f"Client {client_id} > ",
        multiline=True,
        key_bindings=bindings,
        style=style,
        completer=sql_completer,
    )


async def connect(host: str, port: int, client_id: str) -> None:
    """
    Connect to the server and send SELECT queries.
    """
    session = create_prompt_session(client_id)

    async with trio.open_nursery() as nursery:
        async with await trio.open_tcp_stream(host, port) as client_stream:
            print(
                f"Client {client_id} connected to {host}:{port}, type 'exit' to quit."
            )

            with patch_stdout():
                while True:
                    # Run prompt_toolkit prompt in a separate thread
                    query = await trio.to_thread.run_sync(session.prompt)
                    query = query.strip()

                    if query.lower() == "exit":
                        await client_stream.send_all(b"exit\n")
                        break

                    # Send query to server
                    await client_stream.send_all(f"{query}\n".encode())

                    # ---- START SPINNER ----
                    spinner_cancel_scope = trio.CancelScope()
                    nursery.start_soon(spinner_task, spinner_cancel_scope)

                    # Wait for response
                    data = await client_stream.receive_some(4096)

                    # ---- STOP SPINNER ----
                    spinner_cancel_scope.cancel()

                    # Clear spinner line
                    print(" " * 20, end="\r")

                    # Display server response
                    if data:
                        print(f"Client {client_id} > {data.decode().strip()}")
                    else:
                        print(f"Client {client_id} > [Server closed connection]")
                        break


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

    async def main():
        await connect(args.host, args.port, args.client_id)

    trio.run(main)
