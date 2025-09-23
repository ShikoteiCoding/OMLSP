import polars as pl

from abc import ABC, abstractmethod
from duckdb import DuckDBPyConnection
from typing import Any, AsyncGenerator, Callable, TypeAlias, Coroutine, TypeVar, Generic
from loguru import logger

from channel import Channel

TaskOutput: TypeAlias = Any
TaskId = int | str

T = TypeVar("T")


class BaseTaskT(ABC, Generic[T]):
    _runing = False

    def __init__(self, task_id: str | int, conn: DuckDBPyConnection | None = None):
        self.task_id = task_id
        self._conn = conn

    @abstractmethod
    def register(self, executable: Callable):
        pass

    @abstractmethod
    async def run(self):
        pass


class BaseSourceTaskT(BaseTaskT, Generic[T]):
    _running = False

    def __init__(self, task_id: str, conn: DuckDBPyConnection | None = None):
        super().__init__(task_id, conn)
        self._sender = Channel[T]()

    @abstractmethod
    def get_sender(self) -> Channel:
        pass


class ScheduledSourceTask(BaseSourceTaskT, Generic[T]):
    _sender: Channel
    _executable: Callable

    def __init__(self, task_id: str, conn: DuckDBPyConnection):
        super().__init__(task_id, conn)
        self._sender = Channel[T](100)

    def register(
        self, executable: Callable[[str, DuckDBPyConnection], Coroutine[Any, Any, T]]
    ):
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
    ):
        self._executable = executable
        return self

    def get_sender(self) -> Channel[T]:
        return self._sender

    async def run(self):
        async for result in self._executable(task_id=self.task_id, conn=self._conn):
            await self._sender.send(result)


class SinkTask(BaseTaskT):
    def __init__(self, task_id: str):
        super().__init__(task_id)
        self._receivers: list[Channel] = []

    def register(
        self,
        executable: Callable[
            [str, DuckDBPyConnection, pl.DataFrame], Coroutine[Any, Any, T]
        ],
    ):
        self._executable = executable
        return self

    def subscribe(self, recv: Channel):
        self._receivers.append(recv)

    async def run(self):
        # TODO: receive many upstreams
        receiver = self._receivers[0]
        async for df in receiver:
            logger.info(f"[SinkTask{{{self.task_id}}}] got:\n{df}")
            # await self._executable(df=df)
