from __future__ import annotations

import trio

from duckdb import DuckDBPyConnection
from typing import Any, AsyncGenerator, Awaitable, Callable, Generic
from apscheduler.triggers.cron import CronTrigger
from loguru import logger
from services import Service

from channel.broker import ChannelBroker, _get_channel_broker
from channel.channel import Channel
from channel.producer import Producer

from scheduler.types import SchedulerCommand
from task.types import TaskId, T

DEFAULT_CAPACITY = 1


class BaseTask(Service, Generic[T]):
    #: TaskId for now being mostly SQL artifact name
    task_id: TaskId

    #: DuckDB Connection
    conn: DuckDBPyConnection

    #: The executable function, core logic of the Task
    _executable: Callable[..., Awaitable]

    #: Cancel scope for structured concurrency
    _cancel_scope: trio.CancelScope

    #: Cancel event allows other tasks to be notified when this task is being stopped
    _cancel_event: trio.Event

    def __init__(self, task_id: TaskId, conn: DuckDBPyConnection):
        super().__init__(name=task_id)
        self.task_id = task_id
        self.conn = conn
        self._cancel_event = trio.Event()

    def register(self, executable: Callable[..., Any]) -> BaseTask:
        """Attach the executable coroutine or function to this task."""
        self._executable = executable
        return self

    async def on_start(self) -> None:
        logger.info(f"[{self.task_id}] task running")
        self._cancel_scope = trio.CancelScope()

        async def _task_runner():
            with self._cancel_scope:
                await self.run()

        # Start the real task inside the cancel scope
        self._nursery.start_soon(_task_runner)

    async def run(self) -> None:
        """Override in subclasses."""
        await self._executable()


class BaseTaskSender(BaseTask[T]):
    #: Channel to write output for downstream tasks
    _sender: Channel[T]

    #: Sender watchguard
    _has_sender: bool = False

    def get_sender(self) -> Channel[T]:
        """
        Lazily create and return the sender channel
        Get sender when asked. If doesn't exist, creates one.

        NOTE: This implementation isn't very robust, we are
         assuming that any resuester of :meth:`get_sender` will
         succeed and that we indeed instanciate a sender. This
         could lead to stuck Tasks if a sender channel is created
         without anyt true consummer on the other side.
        """
        if not self._has_sender:
            self._sender = Channel[T](1)  # first-time creation
            self._has_sender = True
        return self._sender

    async def on_stop(self) -> None:
        logger.info(f"[{self.task_id}] task stopping")
        self._cancel_event.set()
        if hasattr(self, "_sender"):
            await self._sender.aclose()
        self._cancel_scope.cancel()


class BaseTaskReceiver(BaseTask[T]):
    # Channel to receive data from upstream tasks
    _receivers: list[Channel[T]]

    async def on_stop(self) -> None:
        logger.info(f"[{self.task_id}] task stopping")
        self._cancel_scope.cancel()

    def subscribe(self, sender: BaseTaskSender):
        self._receivers.append(sender.get_sender().clone())


class ScheduledSourceTask(BaseTaskSender, Generic[T]):
    #: Trigger for Scheduler
    trigger: CronTrigger

    #: ChannelBroker ref
    _channel_broker: ChannelBroker

    #: Tasks to be scheduled to Scheduler
    _scheduler_commands_producer: Producer

    def __init__(
        self,
        task_id: TaskId,
        conn: DuckDBPyConnection,
        trigger: CronTrigger,
    ):
        super().__init__(task_id, conn)
        self.trigger = trigger
        self._channel_broker = _get_channel_broker()
        self._scheduler_commands_producer = self._channel_broker.producer(
            "scheduler.commands"
        )

    async def on_start(self) -> None:
        logger.info(f"[{self.task_id}] task running")
        self._cancel_scope = trio.CancelScope()

        # NOTE: for now scheduled task register itself to scheduler
        # to simplify the task lifecycle. This is not proper pattern
        async def _task_runner():
            with self._cancel_scope:
                try:
                    await self._scheduler_commands_producer.produce(
                        (SchedulerCommand.ADD, (self.task_id, self.trigger, self.run))
                    )
                except trio.Cancelled:
                    pass

        # Start the real task inside the cancel scope
        self._nursery.start_soon(_task_runner)

    def register(
        self, executable: Callable[[TaskId, DuckDBPyConnection], Awaitable[T]]
    ) -> ScheduledSourceTask[T]:
        self._executable = executable
        return self

    async def run(self):
        if not self._cancel_event.is_set():
            result = await self._executable(task_id=self.task_id, conn=self.conn)

            if self._has_sender:
                await self._sender.send(result)


class ContinuousSourceTask(BaseTaskSender, Generic[T]):
    def __init__(self, task_id: str, conn: DuckDBPyConnection, nursery: trio.Nursery):
        super().__init__(task_id, conn)
        self.nursery = nursery

    def register(
        self,
        executable: Callable[
            [str, DuckDBPyConnection, trio.Nursery, trio.Event],
            AsyncGenerator[T, None],
        ],
    ) -> ContinuousSourceTask[T]:
        self._executable = executable  # type: ignore
        return self

    async def run(self):
        async for result in self._executable(  # type: ignore
            task_id=self.task_id,
            conn=self.conn,
            nursery=self.nursery,
            cancel_event=self._cancel_event,
        ):
            if self._has_sender:
                await self._sender.send(result)


class SinkTask(BaseTaskReceiver, Generic[T]):
    def __init__(self, task_id: str, conn: DuckDBPyConnection):
        super().__init__(task_id, conn)
        self._receivers: list[Channel[T]] = []

    async def run(self):
        # TODO: receive many upstreams
        receiver = self._receivers[0]
        async for df in receiver:
            await self._executable(self.task_id, self.conn, df)


class TransformTask(BaseTaskSender, BaseTaskReceiver, Generic[T]):
    def __init__(self, task_id: str, conn: DuckDBPyConnection):
        super().__init__(task_id, conn)
        self._receivers: list[Channel[T]] = []
        self._sender = Channel[T](DEFAULT_CAPACITY)

    async def run(self):
        receiver = self._receivers[0]
        async for df in receiver:
            result = await self._executable(self.task_id, self.conn, df)
            if self._has_sender:
                await self._sender.send(result)
