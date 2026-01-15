from __future__ import annotations

import trio

from duckdb import DuckDBPyConnection
from typing import Any, AsyncGenerator, Awaitable, Callable, Generic
from apscheduler.triggers.cron import CronTrigger
from loguru import logger
from services import Service

from channel.registry import ChannelBroker, _get_channel_broker
from channel.channel import BroadcastChannel, Channel
from channel.producer import Producer
from channel.types import _Msg

from scheduler.types import SchedulerCommand
from task.types import TaskId, T

DEFAULT_CAPACITY = 100


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
        logger.info(f"[Task{{{self.task_id}}}] task running")
        self._cancel_scope = trio.CancelScope()

        async def _task_runner():
            with self._cancel_scope:
                await self._loop_runner()

        # Start the real task inside the cancel scope
        self._nursery.start_soon(_task_runner)

    async def _loop_runner(self) -> None:
        """Override in subclasses."""
        await self._executable()


class BaseTaskSender(BaseTask[T]):
    #: Channel to write output for downstream tasks
    _to: BroadcastChannel[T]

    def __init__(self, task_id: TaskId, conn: DuckDBPyConnection) -> None:
        super().__init__(task_id, conn)
        self._to = BroadcastChannel[T](task_id, DEFAULT_CAPACITY)

    def get_sender(self) -> BroadcastChannel[T]:
        """
        Lazily create and return the sender channel
        Get sender when asked. If doesn't exist, creates one.

        NOTE: This implementation isn't very robust, we are
         assuming that any requester of :meth:`get_sender` will
         succeed and that we indeed instanciate a sender. This
         could lead to stuck Task(s) if a sender channel is created
         without any true consummer on the other side.
        """
        return self._to

    async def on_stop(self) -> None:
        logger.info("[Task{{{}}}] sender task stopping", self.task_id)
        self._cancel_event.set()
        await self._to.aclose()
        self._cancel_scope.cancel()


class BaseTaskReceiver(BaseTask[T]):
    #: List of channels to receive upstream data
    _from: list[Channel[_Msg[T]]]

    #: List of channel to unsubscribe during DROP operation
    _callbacks: list[Callable[[], None]]

    def __init__(self, task_id: TaskId, conn: DuckDBPyConnection) -> None:
        super().__init__(task_id, conn)
        self._from = []
        self._callbacks = []

    async def on_stop(self) -> None:
        logger.info("[Task{{{}}}] receiver task stopping", self.task_id)
        self._cancel_scope.cancel()
        for callback in self._callbacks:
            callback()

    def subscribe(self, sender: BaseTaskSender):
        logger.info(
            "[Task{{{}}}] subscribed to task '{}'", self.task_id, sender.task_id
        )
        chan, drop_callback = sender.get_sender().spawn(self.task_id)
        self._from.append(chan)
        self._callbacks.append(drop_callback)


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
        logger.info(f"[Task{{{self.task_id}}}] task running")
        self._cancel_scope = trio.CancelScope()

        # NOTE: for now scheduled task register itself to scheduler
        # to simplify the task lifecycle. This is not proper pattern
        async def _task_runner():
            with self._cancel_scope:
                try:
                    await self._scheduler_commands_producer.produce(
                        (
                            SchedulerCommand.ADD,
                            (self.task_id, self.trigger, self._loop_runner),
                        )
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

    async def _loop_runner(self):
        if not self._cancel_event.is_set():
            result = await self._executable(task_id=self.task_id, conn=self.conn)

            await self._to.send(result)


class ContinuousSourceTask(BaseTaskSender, Generic[T]):
    #: Continuous task is a generator and needs
    # a nursery for internal fan-in
    nursery: trio.Nursery

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

    async def _loop_runner(self):
        async for result in self._executable(  # type: ignore
            task_id=self.task_id,
            conn=self.conn,
            nursery=self.nursery,
            cancel_event=self._cancel_event,
        ):
            await self._to.send(result)


class SinkTask(BaseTaskReceiver, Generic[T]):
    def __init__(self, task_id: str, conn: DuckDBPyConnection):
        super().__init__(task_id, conn)

    async def _loop_runner(self):
        # TODO: receive many upstreams
        receiver: Channel[_Msg[T]] = self._from[0]
        async for msg in receiver:
            df = msg.payload
            await self._executable(self.task_id, self.conn, df)

            async with receiver._ack_lock:
                msg.ack()


class TransformTask(BaseTaskSender, BaseTaskReceiver, Generic[T]):
    def __init__(self, task_id: str, conn: DuckDBPyConnection):
        super().__init__(task_id=task_id, conn=conn)

    async def on_stop(self) -> None:
        """
        Writing custom on_stop as both implementations need to be handled.
        """
        logger.info("[Task{{{}}}] transform task stopping", self.task_id)
        self._cancel_scope.cancel()
        for callback in self._callbacks:
            callback()

    async def _loop_runner(self):
        receiver: Channel[_Msg[T]] = self._from[0]
        async for msg in receiver:
            df = msg.payload
            result = await self._executable(self.task_id, self.conn, df)

            async with receiver._ack_lock:
                msg.ack()

            await self._to.send(result)
