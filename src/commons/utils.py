import trio
from typing import Generic, TypeVar

T = TypeVar("T")


class Channel(Generic[T]):
    def __init__(self, size: int = 100):
        """
        If size == 0, the channel becomes blocked till closed.
        """
        self._send_ch, self._recv_ch = trio.open_memory_channel[T](size)
        self.size = size

    async def send(self, data: T) -> None:
        await self._send_ch.send(data)

    async def recv(self) -> T:
        return await self._recv_ch.receive()

    def __aiter__(self):
        return self

    async def __anext__(self) -> T:
        try:
            return await self._recv_ch.receive()
        except trio.EndOfChannel:
            raise StopAsyncIteration
