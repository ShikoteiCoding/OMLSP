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
    def __init__(self, task_id: str, conn: DuckDBPyConnection, trigger: str):
        super().__init__(task_id, conn)
        self._sender = Channel()
        self._trigger = trigger
        self._executable = None

    def get_sender(self) -> Channel:
        return self._sender

    async def run(self):
        # TODO replace with external scheduler
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
        if not self._receivers:
            logger.info(f"[SinkTask{{{self.task_id}}}] no upstreams")
            return

        receiver = self._receivers[0]
        async for df in receiver:
            if df is None:
                continue

            logger.info(f"[SinkTask{{{self.task_id}}}] got:\n{df}")
            # await self._executable(df=df)
