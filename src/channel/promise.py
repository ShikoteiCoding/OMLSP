import trio
from typing import Generic, TypeVar, Any
from channel.types import ValidResponse, InvalidResponse

T = TypeVar("T")

class Promise(Generic[T]):
    def __init__(self):
        self._event = trio.Event()
        self._result: ValidResponse | InvalidResponse

    def set(self, value: ValidResponse | InvalidResponse) -> None:
        """Called by the consumer to return a success value."""
        self._result = value
        self._event.set()

    async def await_result(self) -> ValidResponse | InvalidResponse:
        """Called by the producer to wait for the reply."""
        with trio.move_on_after(10):
            await self._event.wait()
            return self._result

        return InvalidResponse("No response received within 10 seconds")