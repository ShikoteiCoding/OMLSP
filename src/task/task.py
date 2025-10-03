from __future__ import annotations

import polars as pl
from loguru import logger
from abc import ABC, abstractmethod
from duckdb import DuckDBPyConnection
from typing import Any, AsyncGenerator, Callable, TypeAlias, Coroutine, TypeVar, Generic

from channel import Channel

TaskOutput: TypeAlias = Any
TaskId = str

T = TypeVar("T")


class BaseTaskT(ABC, Generic[T]):
    def __init__(self, task_id: TaskId, conn: DuckDBPyConnection):
        self.task_id = task_id
        self._conn = conn

    @abstractmethod
    def register(self, executable: Callable) -> BaseTaskT[T]:
        pass

    @abstractmethod
    async def run(self):
        pass


class BaseSourceTaskT(BaseTaskT, Generic[T]):
    def __init__(self, task_id: TaskId, conn: DuckDBPyConnection):
        super().__init__(task_id, conn)
        self._sender = Channel[T]()

    @abstractmethod
    def register(self, executable: Callable) -> BaseSourceTaskT[T]:
        self._executable = executable
        return self

    @abstractmethod
    def get_sender(self) -> Channel:
        pass


class ScheduledSourceTask(BaseSourceTaskT, Generic[T]):
    _sender: Channel
    _executable: Callable

    def __init__(self, task_id: TaskId, conn: DuckDBPyConnection):
        super().__init__(task_id, conn)
        self._sender = Channel[T](100)

    def register(
        self, executable: Callable[[TaskId, DuckDBPyConnection], Coroutine[Any, Any, T]]
    ) -> ScheduledSourceTask[T]:
        self._executable = executable
        return self

    def get_sender(self) -> Channel:
        return self._sender

    async def run(self):
        result = await self._executable(task_id=self.task_id, conn=self._conn)
        if hasattr(self, "_sender"):
            await self._sender.send(result)


class ContinuousSourceTask(BaseSourceTaskT, Generic[T]):
    _sender: Channel[T]
    _executable: Callable

    def __init__(self, task_id: str, conn: DuckDBPyConnection):
        super().__init__(task_id, conn)
        self._sender = Channel[T](100)

    def register(
        self, executable: Callable[[str, DuckDBPyConnection], AsyncGenerator[Any, T]]
    ) -> ContinuousSourceTask[T]:
        self._executable = executable
        return self

    def get_sender(self) -> Channel[T]:
        return self._sender

    async def run(self):
        async for result in self._executable(task_id=self.task_id, conn=self._conn):
            await self._sender.send(result)


class SinkTask(BaseTaskT, Generic[T]):
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


class TransformTask(BaseTaskT, Generic[T]):
    _sender: Channel[T]
    _receivers: list[Channel[T]]
    _executable: Callable[[TaskId, DuckDBPyConnection, T], Coroutine[Any, Any, T]]

    def __init__(self, task_id: str, conn: DuckDBPyConnection):
        super().__init__(task_id, conn)
        self._receivers: list[Channel[T]] = []
        self._sender = Channel[T](100)

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
            logger.info(f"[TransformTask{{{self.task_id}}}] got:\n{result}")
            if hasattr(self, "_sender"):
                await self._sender.send(result)
