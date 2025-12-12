import trio
from typing import Generic, TypeVar
from channel.types import ValidResponse, InvalidResponse

T = TypeVar("T")


class Promise(Generic[T]):
    def __init__(self):
        self._event = trio.Event()
        self._result: ValidResponse | InvalidResponse

    def set(self, value: ValidResponse | InvalidResponse) -> None:
        self._result = value
        self._event.set()

    async def await_result(self) -> T:
        with trio.move_on_after(10):
            await self._event.wait()
            if isinstance(self._result, InvalidResponse):
                raise RuntimeError(self._result.reason)
            return self._result.data

        raise TimeoutError("No response received within 10 seconds")
