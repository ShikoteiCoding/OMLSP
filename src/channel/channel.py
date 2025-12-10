from __future__ import annotations

import trio

from typing import Generic
from loguru import logger


from channel.types import ChannelT, T, _Msg


class Channel(ChannelT, Generic[T]):
    def __init__(self, size: int, broadcast_lock: trio.Lock | None = None):
        """
        If size == 0, the channel becomes blocked till closed.
        """
        self._send_ch, self._recv_ch = trio.open_memory_channel[T](size)
        self._subscribers: list[trio.MemorySendChannel] = []  # for clones
        self._closed = trio.Event()

        self._ack_lock = broadcast_lock or trio.Lock()

    async def send(self, data: T) -> None:
        if self._closed.is_set():
            raise trio.ClosedResourceError("Channel is closed")

        await self._send_ch.send(data)

        # TODO broadcast channel instead of clones
        for sub in list(self._subscribers):
            try:
                await sub.send(data)
            except trio.BrokenResourceError:
                self._subscribers.remove(sub)

    async def recv(self) -> T:
        return await self._recv_ch.receive()

    async def aclose(self):
        if self._closed.is_set():
            return
        self._closed.set()
        logger.debug(f"[Channel] closing {len(self._subscribers)} subscribers")
        for sub in self._subscribers:
            await sub.aclose()
        self._subscribers.clear()
        await self._send_ch.aclose()
        logger.debug("[Channel] send side closed")

    def __aiter__(self):
        return self

    async def __anext__(self) -> T:
        try:
            return await self._recv_ch.receive()
        except trio.EndOfChannel:
            raise StopAsyncIteration

class BroadcastChannel(ChannelT, Generic[T]):
    #: Fan-out channels of this instance
    _subscribers: list[Channel[_Msg[T]]]
    
    #: Close event
    _closed: trio.Event

    #: Channel(s) for spawned subscribed channels
    _size: int

    def __init__(self, size: int = 100):
        self._subscribers: list[Channel[_Msg[T]]] = []
        self._closed = trio.Event()
        self.size = size

        # Lock to be shared with subscribers
        self._ack_lock = trio.Lock()

    async def send(self, data: T) -> None:
        if self._closed.is_set():
            raise trio.ClosedResourceError("Channel is closed")

        if len(self._subscribers) == 0:
            return

        msg = _Msg[T](data, len(self._subscribers), trio.Event())

        for sub in list(self._subscribers):
            try:
                await sub.send(msg)
            except trio.BrokenResourceError:
                self._subscribers.remove(sub)

        # NOTE: might require timeout
        await msg.done.wait()

    async def aclose(self):
        if self._closed.is_set():
            return
        self._closed.set()
        logger.debug(f"[Channel] closing {len(self._subscribers)} subscribers")
        for sub in self._subscribers:
            await sub.aclose()
        self._subscribers.clear()
        logger.debug("[Channel] send side closed")

    def __aiter__(self):
        return self

    def spawn(self) -> Channel[_Msg[T]]:
        channel = Channel[_Msg[T]](self.size, self._ack_lock)
        self._subscribers.append(channel)
        return channel
