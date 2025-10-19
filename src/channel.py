from typing import Generic, TypeVar

import trio

T = TypeVar("T")


class Channel(Generic[T]):
    def __init__(self, size: int = 100):
        """
        If size == 0, the channel becomes blocked till closed.
        """
        self._send_ch, self._recv_ch = trio.open_memory_channel[T](size)
        self._subscribers: list[trio.MemorySendChannel] = []  # for clones
        self.size = size

    async def send(self, data: T) -> None:
        await self._send_ch.send(data)

        # TODO broadcast channel instead of clones
        for sub in list(self._subscribers):
            try:
                await sub.send(data)
            except trio.BrokenResourceError:
                self._subscribers.remove(sub)

    async def recv(self) -> T:
        return await self._recv_ch.receive()
    
    async def aclose_sender(self):
            """Asynchronously closes the sender end of the channel."""
            # This closes the primary sending channel
            await self._send_ch.aclose()
            
            # close all subscriber channels!
            for sub in list(self._subscribers):
                try:
                    await sub.aclose()
                except trio.BrokenResourceError:
                    pass # Ignore if already closed/broken

    def __aiter__(self):
        return self

    async def __anext__(self) -> T:
        try:
            return await self._recv_ch.receive()
        except trio.EndOfChannel:
            raise StopAsyncIteration

    def clone(self) -> "Channel[T]":
        send, recv = trio.open_memory_channel[T](self.size)
        self._subscribers.append(send)
        return Channel._from_existing(send, recv, self.size)

    @classmethod
    def _from_existing(cls, send_ch, recv_ch, size):
        ch = cls.__new__(cls)
        ch._send_ch = send_ch
        ch._recv_ch = recv_ch
        ch._subscribers = []
        ch.size = size
        return ch
