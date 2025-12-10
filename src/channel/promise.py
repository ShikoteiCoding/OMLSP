import trio
from typing import Generic, TypeVar, Any

T = TypeVar("T")

class Promise(Generic[T]):
    def __init__(self):
        self._event = trio.Event()
        self._result: Any

    def set(self, value: T) -> None:
        """Called by the consumer to return a success value."""
        self._result = value
        self._event.set()

    async def await_result(self) -> Any:
        """Called by the producer to wait for the reply."""
        await self._event.wait()
        return self._result