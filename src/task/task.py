from abc import ABC, abstractmethod
from confluent_kafka import Producer
import json
import trio
import polars as pl
from duckdb import DuckDBPyConnection
from typing import Any, Callable, TypeAlias
from loguru import logger

from commons.utils import Channel
from commons.scheduler import minimal_scheduler

TaskOutput: TypeAlias = Any
TaskId = int | str


class BaseTask(ABC):
    def __init__(self, task_id: str | int, conn: DuckDBPyConnection | None = None):
        self.task_id = task_id
        self._conn = conn

    @abstractmethod
    async def run(self) -> Any: ...


class SourceTask(BaseTask):
    def __init__(self, task_id: str, conn: DuckDBPyConnection, trigger: str):
        super().__init__(task_id, conn)
        self._sender = Channel()
        self._trigger = trigger
        self._executable = None

    def get_sender(self) -> Channel:
        return self._sender

    def register(self, executable: Callable):
        self._executable = executable
        return self

    async def run(self):
        if self._executable is None:
            logger.info(f"No executable registered")
            return

        async for _ in minimal_scheduler(self._trigger):
            result = await self._executable(task_id=self.task_id, con=self._conn)
            await self._sender.send(result)


class SinkTask(BaseTask):
    def __init__(self, task_id: str, properties: dict[str, str]):
        super().__init__(task_id)
        self._receivers: list[Channel] = []
        if properties and properties.get("connector") == "kafka":
            self._producer = Producer({"bootstrap.servers": properties["server"]})
            self._topic = properties["topic"]
        else:
            self._producer, self._topic = None, None

    def subscribe(self, recv: Channel):
        self._receivers.append(recv)

    async def _send_to_kafka(self, df: pl.DataFrame):
        records = df.to_dicts()
        payload = json.dumps(records).encode("utf-8")  # json serialize

        def _produce():
            self._producer.produce(self._topic, value=payload)
            self._producer.poll(0)

        await trio.to_thread.run_sync(_produce)

    async def run(self):
        if not self._receivers:
            logger.info(f"[SinkTask{{{self.task_id}}}] no upstreams")
            return

        while True:
            df = await self._receivers[0].recv()
            if df is None:
                continue
            logger.info(f"[SinkTask{{{self.task_id}}}] got:\n{df}")

            # if self._producer and self._topic:
            #     await self._send_to_kafka(df)
