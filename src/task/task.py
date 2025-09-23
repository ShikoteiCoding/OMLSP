from abc import ABC, abstractmethod
from duckdb import DuckDBPyConnection
from typing import Any, Callable, TypeAlias
from loguru import logger

from channel import Channel

TaskOutput: TypeAlias = Any
TaskId = int | str


class BaseTask(ABC):
    def __init__(self, task_id: str | int, conn: DuckDBPyConnection | None = None):
        self.task_id = task_id
        self._conn = conn

    def register(self, executable: Callable):
        self._executable = executable
        return self

    @abstractmethod
    async def run(self):
        pass


class SourceTask(BaseTask):
    _running = False
    _sender: Channel
    _executable: Callable

    def __init__(self, task_id: str, conn: DuckDBPyConnection):
        super().__init__(task_id, conn)
        self._sender = Channel()

    def get_sender(self) -> Channel:
        return self._sender

    async def run(self):
        result = await self._executable(task_id=self.task_id, conn=self._conn)
        if hasattr(self, "_sender"):
            await self._sender.send(result)


class SinkTask(BaseTask):
    def __init__(self, task_id: str):
        super().__init__(task_id)
        self._receivers: list[Channel] = []

    def subscribe(self, recv: Channel):
        self._receivers.append(recv)

    async def run(self):
        # TODO: receive many upstreams
        receiver = self._receivers[0]
        async for df in receiver:
            logger.info(f"[SinkTask{{{self.task_id}}}] got:\n{df}")
            # await self._executable(df=df)

class TransformTask(BaseTask):
    def __init__(self, task_id: str, conn: DuckDBPyConnection):
        super().__init__(task_id, conn)
        self._receivers: list[Channel] = []
        self._sender = Channel()

    def subscribe(self, recv: Channel):
        self._receivers.append(recv)

    def get_sender(self) -> Channel:
        return self._sender

    async def run(self):
        receiver = self._receivers[0]
        async for df in receiver:
            result = await self._executable(df=df, conn=self._conn)
            logger.info(f"[ViewTask{{{self.task_id}}}] got:\n{result}")
            if hasattr(self, "_sender"):
                await self._send_channel.send(result)