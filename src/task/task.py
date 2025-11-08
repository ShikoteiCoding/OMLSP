from __future__ import annotations

import polars as pl
import trio

from typing import Protocol
from duckdb import DuckDBPyConnection
from typing import Any, AsyncGenerator, Callable, TypeAlias, Coroutine, TypeVar, Generic
from apscheduler.triggers.cron import CronTrigger
from loguru import logger
from services import Service

from channel import Channel

TaskOutput: TypeAlias = Any
TaskId = str
DEFAULT_CAPACITY = 100

T = TypeVar("T")


class BaseSourceTaskT(Generic[T], Protocol):
    def register(self, executable: Callable[..., Any]) -> BaseSourceTaskT[T]: ...

    def get_sender(self) -> Channel[T]: ...

    async def run(self) -> None: ...

    async def on_start(self) -> None: ...

    async def on_stop(self) -> None: ...


class BaseSourceTask(Service, BaseSourceTaskT):
    """Common base for all tasks."""

    task_id: TaskId
    conn: DuckDBPyConnection
    _executable: Callable[..., Any]
    _cancel_scope: trio.CancelScope
    _sender: Channel
    _cancel_event: trio.Event

    def __init__(self, task_id: TaskId, conn: DuckDBPyConnection):
        super().__init__(name=task_id)
        self.task_id = task_id
        self._conn = conn
        self._sender = Channel[T]()
        self._cancel_event = trio.Event()

    def get_sender(self) -> Channel[T]:
        return self._sender

    def register(self, executable: Callable[..., Any]) -> BaseSourceTask[T]:
        """Attach the executable coroutine or function to this task."""
        self._executable = executable
        return self

    async def run(self) -> None:
        """Override in subclasses."""
        await self._executable()

    async def on_start(self) -> None:
        logger.info(f"[{self.task_id}] task running")

        self._cancel_scope = trio.CancelScope()

        async def _task_runner():
            with self._cancel_scope:
                try:
                    await self.run()
                except trio.Cancelled:
                    pass  # Normal shutdown path
                except Exception as e:
                    logger.exception(f"[{self.task_id}] task crashed: {e}")

        # Start the real task inside the cancel scope
        self._nursery.start_soon(_task_runner)

    async def on_stop(self) -> None:
        logger.info(f"[{self.task_id}] task stopping")
        self._cancel_event.set()
        if hasattr(self, "_sender"):
            await self._sender.aclose()
        self._cancel_scope.cancel()


class ScheduledSourceTask(BaseSourceTask, Generic[T]):
    _sender: Channel
    _executable: Callable

    def __init__(
        self,
        task_id: TaskId,
        conn: DuckDBPyConnection,
        scheduled_executables: Channel[Callable | tuple[Callable, Any]],
        trigger: CronTrigger,
    ):
        super().__init__(task_id, conn)
        self._sender = Channel[T](DEFAULT_CAPACITY)
        self.scheduled_executables = scheduled_executables
        self.trigger = trigger

    async def on_start(self) -> None:
        logger.info(f"[{self.task_id}] task running")
        self._cancel_scope = trio.CancelScope()

        async def _task_runner():
            with self._cancel_scope:
                try:
                    await self.scheduled_executables.send((self.run, self.trigger))
                except trio.Cancelled:
                    pass  # Normal shutdown path

        # Start the real task inside the cancel scope
        self._nursery.start_soon(_task_runner)

    def register(
        self, executable: Callable[[TaskId, DuckDBPyConnection], Coroutine[Any, Any, T]]
    ) -> ScheduledSourceTask[T]:
        self._executable = executable
        return self

    def get_sender(self) -> Channel:
        return self._sender

    async def run(self):
        if not self._cancel_event.is_set():
            result = await self._executable(task_id=self.task_id, conn=self._conn)
            await self._sender.send(result)


class ContinuousSourceTask(BaseSourceTask, Generic[T]):
    _sender: Channel[T]
    _executable: Callable

    def __init__(self, task_id: str, conn: DuckDBPyConnection, nursery: trio.Nursery):
        super().__init__(task_id, conn)
        self._sender = Channel[T](DEFAULT_CAPACITY)
        self.nursery = nursery

    def register(
        self,
        executable: Callable[
            [str, DuckDBPyConnection, trio.Nursery, trio.Event],
            AsyncGenerator[Any, T],
        ],
    ) -> ContinuousSourceTask[T]:
        self._executable = executable
        return self

    def get_sender(self) -> Channel[T]:
        return self._sender

    async def run(self):
        async for result in self._executable(
            task_id=self.task_id,
            conn=self._conn,
            nursery=self.nursery,
            cancel_event=self._cancel_event,
        ):
            await self._sender.send(result)


class SinkTask(BaseSourceTask, Generic[T]):
    _receivers: list[Channel[T]]

    def __init__(self, task_id: str, conn: DuckDBPyConnection):
        super().__init__(task_id, conn)
        self._receivers: list[Channel[T]] = []

    def register(
        self,
        executable: Callable[
            [str, DuckDBPyConnection, pl.DataFrame], Coroutine[Any, Any, T]
        ],
    ) -> SinkTask[T]:
        self._executable = executable
        return self

    def subscribe(self, recv: Channel):
        self._receivers.append(recv.clone())

    async def run(self):
        # TODO: receive many upstreams
        receiver = self._receivers[0]
        async for df in receiver:
            pass
            # await self._executable(self.task_id, self._conn, df)


class TransformTask(BaseSourceTask, Generic[T]):
    _sender: Channel[T]
    _receivers: list[Channel[T]]
    _executable: Callable[[TaskId, DuckDBPyConnection, T], Coroutine[Any, Any, T]]

    def __init__(self, task_id: str, conn: DuckDBPyConnection):
        super().__init__(task_id, conn)
        self._receivers: list[Channel[T]] = []
        self._sender = Channel[T](DEFAULT_CAPACITY)

    def register(
        self,
        executable: Callable[[TaskId, DuckDBPyConnection, T], Coroutine[Any, Any, T]],
    ) -> TransformTask[T]:
        self._executable = executable
        return self

    def subscribe(self, recv: Channel):
        self._receivers.append(recv.clone())

    def get_sender(self) -> Channel:
        return self._sender

    async def run(self):
        receiver = self._receivers[0]
        async for df in receiver:
            result = await self._executable(self.task_id, self._conn, df)
            if hasattr(self, "_sender"):
                await self._sender.send(result)
