from abc import ABC, abstractmethod
from trio_util import periodic
import trio
from duckdb import DuckDBPyConnection
from typing import Any, Callable, TypeAlias
from loguru import logger

from commons.utils import Channel

TaskOutput: TypeAlias = Any
TaskId = int | str


class BaseTask(ABC):
    def __init__(self, task_id: str | int, conn: DuckDBPyConnection | None = None):
        self.task_id = task_id
        self._conn = conn

    @abstractmethod
    async def run(self, **kwargs) -> Any: ...


class SourceTask(BaseTask):
    def __init__(self, task_id, conn, period: float = 5):
        self.task_id = task_id
        self._conn = conn
        self._sender = Channel()
        self._period = period
        self._executable = None

    def get_sender(self) -> Channel:
        return self._sender

    def register(self, executable: Callable):
        self._executable = executable
        return self

    async def run(self, **kwargs):
        if self._executable is None:
            logger.info(f"No executable registered")
            return

        async for _, _ in periodic(self._period):
            result = self._executable(task_id=self.task_id, con=self._conn)
            await self._sender.send(result)


class SinkTask(BaseTask):
    def __init__(self, task_id, conn):
        self.task_id = task_id
        self._conn = conn
        self._receivers: list[Channel] = []

    def subscribe(self, recv: Channel):
        self._receivers.append(recv)

    async def run(self, **kwargs):
        if not self._receivers:
            logger.info(f"[SinkTask{{{self.task_id}}}] no upstreams")
            return

        while True:
            # TODO: send to kafka
            receiver = await self._receivers[0].recv()
            logger.info(f"[SinkTask{{{self.task_id}}}] got: {receiver}")
