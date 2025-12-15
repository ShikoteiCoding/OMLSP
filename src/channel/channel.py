from __future__ import annotations

import trio

from functools import partial
from typing import Callable, Generic, TYPE_CHECKING
from loguru import logger


from channel.types import ChannelT, T, _Msg

if TYPE_CHECKING:
    from task.types import TaskId


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
    #: Name for easier debug
    _name: TaskId

    #: Fan-out channels of this instance
    _subscribers: dict[TaskId, Channel[_Msg[T]]]

    #: Close event
    _closed: trio.Event

    #: Channel(s) for spawned subscribed channels
    _size: int

    def __init__(self, name: str, size: int = 100):
        self._name = name
        self._subscribers: dict[TaskId, Channel[_Msg[T]]] = {}
        self._closed = trio.Event()
        self.size = size

        # Lock to be shared with subscribers
        self._ack_lock = trio.Lock()

    async def send(self, data: T) -> None:
        if self._closed.is_set():
            raise trio.ClosedResourceError(
                f"[BroadcastChannel{{{self._name}}}] is closed"
            )

        if len(self._subscribers) == 0:
            return

        msg = _Msg[T](data, len(self._subscribers), trio.Event())

        for task_id, sub_chan in self._subscribers.items():
            try:
                await sub_chan.send(msg)
            except trio.BrokenResourceError as e:
                logger.error(
                    "[BroadcastChannel{{{}}}] Error broadcasting to subchannel '{}' - {}",
                    self._name,
                    task_id,
                    e,
                )

        logger.debug("[BroadcastChannel{{{}}}] is awaiting ack", self._name)
        # NOTE: might require timeout
        await msg.done.wait()
        logger.debug("[BroadcastChannel{{{}}}] got msg ack", self._name)

    async def aclose(self):
        if self._closed.is_set():
            return
        self._closed.set()
        logger.debug(
            "[BroadcastChannel{{{}}}] closing {} subscribers",
            self._name,
            len(self._subscribers),
        )
        for _, sub in self._subscribers.items():
            await sub.aclose()
        self._subscribers.clear()
        logger.debug("[BroadcastChannel{{{}}}] send side closed", self._name)

    def __aiter__(self):
        return self

    def spawn(self, task_id: TaskId) -> tuple[Channel[_Msg[T]], Callable[[], None]]:
        """
        Spawning a subchannel to existing broadcaster of :class:`task.task.BaseTaskSender`.

        This assume the task_id doesn't already exist.
        """
        channel = Channel[_Msg[T]](self.size, self._ack_lock)
        self._subscribers[task_id] = channel
        logger.debug(
            "[BroadcastChannel{{{}}}] got subscription from task '{}",
            self._name,
            task_id,
        )
        return channel, partial(self.unsubscribe, task_id)

    def unsubscribe(self, task_id: TaskId) -> None:
        try:
            logger.debug(
                "[BroadcastChannel{{{}}}] got unsubscribed by task '{}'",
                self._name,
                task_id,
            )
            del self._subscribers[task_id]
        except Exception as e:
            logger.error(
                "[BroadcastChannel{{{}}}] Attempted to drop '{}' but failed - {}",
                self._name,
                task_id,
                e,
            )
