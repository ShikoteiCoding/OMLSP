import argparse
import trio

from prompt_toolkit import PromptSession
from prompt_toolkit.formatted_text import HTML
from prompt_toolkit.history import InMemoryHistory
from prompt_toolkit.key_binding import KeyBindings
from prompt_toolkit.patch_stdout import patch_stdout
from prompt_toolkit.shortcuts import print_formatted_text
from prompt_toolkit.styles.pygments import style_from_pygments_cls

from pygments.styles import get_style_by_name

from services import Service


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
    """Create a configured PromptSession with bindings, completer, and history."""
    style = style_from_pygments_cls(get_style_by_name("monokai"))
    bindings = KeyBindings()

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
        # completer=sql_completer,
        history=InMemoryHistory(),
        enable_history_search=True,  # optional: arrow up searches partial match
    )


class Client(Service):
    """
    Trio-based client using lifecycle management.
    """

    def __init__(
        self, host: str, port: int, client_id: str, shutdown_timeout: float = 10.0
    ):
        super().__init__(name="Client", shutdown_timeout=shutdown_timeout)
        self.host = host
        self.port = port
        self.client_id = client_id
        self.session = create_prompt_session(client_id)
        self._stream: trio.SocketStream | None = None

    # --- Lifecycle Callbacks ---

    async def on_start(self) -> None:
        """Called automatically when the service starts."""
        print(f"[Service] Starting client {self.client_id}...")
        await self._connect()

    async def on_stop(self) -> None:
        """Called when service is asked to stop."""
        print(f"[Service] Stopping client {self.client_id}...")
        if self._stream is not None:
            await self._stream.aclose()
            print(f"[Service] Connection closed for {self.client_id}")

    async def on_shutdown(self) -> None:
        """Final cleanup phase."""
        print(f"[Service] Shutdown completed for {self.client_id}")

    # --- Core Logic ---

    async def _connect(self) -> None:
        async with await trio.open_tcp_stream(self.host, self.port) as self._stream:
            print(
                f"Client {self.client_id} connected to {self.host}:{self.port}, type 'exit' to quit."
            )

            with patch_stdout():
                while True:
                    try:
                        query: str = await trio.to_thread.run_sync(
                            self.session.prompt, abandon_on_cancel=True
                        )
                    # TODO: improve thread error propagation
                    # currently PromptSession throws error
                    # when receiving a cancel
                    except KeyboardInterrupt:
                        break

                    self.session.history.append_string(query)
                    query = query.strip()

                    if query.lower() == "exit":
                        await self._stream.send_all(b"exit\n")
                        break

                    # Send query to server
                    await self._stream.send_all(f"{query}\n".encode())

                    # ---- START SPINNER ----
                    spinner_cancel_scope = trio.CancelScope()
                    self._nursery.start_soon(spinner_task, spinner_cancel_scope)

                    # Wait for response
                    data = await self._stream.receive_some(4096)

                    # ---- STOP SPINNER ----
                    spinner_cancel_scope.cancel()
                    print(" " * 20, end="\r")

                    # Display server response
                    if data:
                        print(f"Client {self.client_id} > {data.decode().strip()}")
                    else:
                        print(f"Client {self.client_id} > [Server closed connection]")
                        break

            # When loop exits → trigger stop
            await self.stop()


async def main(args):
    client = Client(args.host, args.port, args.client_id)

    async with trio.open_nursery() as nursery:
        nursery.start_soon(client.start, nursery)

        try:
            await client.wait_until_stopped()
        except KeyboardInterrupt:
            await client.stop()
            await client._shutdown.wait()


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

    trio.run(main, args)
