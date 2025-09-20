from typing import Any, Callable, TypeAlias

from duckdb import DuckDBPyConnection
from loguru import logger

from channel import Channel

TaskOutput: TypeAlias = Any
TaskId = int | str


class Task:
    # TODO: type channels and callables
    _running = False
    _receivers: list[Channel] = []
    _sender: Channel
    _executable: Callable

    def __init__(
        self,
        task_id: TaskId,
        conn: DuckDBPyConnection,
    ):
        self.task_id = task_id
        self._conn = conn

    def get_sender(self):
        if not hasattr(self, "_sender"):
            self._sender = Channel(1)
        return self._sender

    def register(self, executable: Callable):
        self.executable = executable
        return self

    def subscribe(self, recv: Channel):
        self._receivers.append(recv)

    def merge(self):
        pass

    async def run(self, **kwargs):
        # TODO: SourceTask implementation
        # Might be good idea to avoid inputs overhead here
        logger.debug(f"[Task{{{self.task_id}}}] starting...")
        inputs = [await ch.recv() for ch in self._receivers]
        result = await self.executable(task_id=self.task_id, con=self._conn, *inputs)
        if hasattr(self, "_sender"):
            await self._sender.send(result)

    def __repr__(self):
        return f"Node({self.task_id!r})"


# TODO: for later
# class BaseTask:
#     def __init__(self, task_id, conn):
#         self.task_id = task_id
#         self.conn = conn
#         self._receivers = []
#         self._sender = Channel(1)

#     async def process(self, inputs):
#         raise NotImplementedError()

#     async def run(self):
#         inputs = [await ch.recv() for ch in self._receivers]
#         result = await self.process(inputs)
#         await self._sender.send(result)

# class SourceTask(BaseTask):
#     def __init__(self, task_id, conn, trigger, fetch_fn):
#         super().__init__(task_id, conn)
#         self.trigger = trigger
#         self.fetch_fn = fetch_fn

#     async def run(self):
#         result = await self.fetch_fn(conn=self.conn)
#         await self._sender.send(result)

# class SinkTask(BaseTask):
#     async def run(self):
#         inputs = [await ch.recv() for ch in self._receivers]
#         await self.process(inputs)  # no sender
