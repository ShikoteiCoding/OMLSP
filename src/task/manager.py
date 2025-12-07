"""
Task Manager managing registration and running of Tasks.
"""

from typing import Callable

from apscheduler.triggers.cron import CronTrigger
from duckdb import DuckDBPyConnection
from loguru import logger

from channel.broker import ChannelBroker, _get_event_bus
from channel.channel import Channel
from channel.consumer import Consumer
from scheduler.scheduler import TrioScheduler
from scheduler.types import SchedulerCommand
from context.context import (
    CreateContext,
)

from context.context import CreateHTTPSourceContext, CreateHTTPTableContext

from services import Service

from task.builder_registry import TASK_REGISTER
from task.supervisor import TaskSupervisor
from task.task import (
    BaseTaskSender,
    ScheduledSourceTask,
)
from task.types import TaskId, TaskManagerCommand


class TaskManager(Service):
    #: Duckdb connections for backend metadata
    backend_conn: DuckDBPyConnection

    #: Duckdb connections for transform
    #: NOTE: to be deprecated
    transform_conn: DuckDBPyConnection

    #: Scheduler (trio compatible) to register
    #: short lived or long lived processes
    scheduler: TrioScheduler

    #: Supervisor to restart tasks
    supervisor: TaskSupervisor

    #: Reference to BaseTaskSender by task id
    _senders: dict[TaskId, BaseTaskSender] = {}

    #: Outgoing channel to send jobs to scheduler
    _scheduled_executables: Channel[
        tuple[SchedulerCommand, TaskId | tuple[TaskId, CronTrigger, Callable]]
    ]

    #: ChannelBroker ref
    _event_bus: ChannelBroker

    #: Consumer for task commands from EntityManager
    _task_commands_consumer: Consumer

    def __init__(
        self, backend_conn: DuckDBPyConnection, transform_conn: DuckDBPyConnection
    ):
        super().__init__(name="TaskManager")
        self.backend_conn = backend_conn
        self.transform_conn = transform_conn
        self._scheduled_executables = Channel[
            tuple[SchedulerCommand, TaskId | tuple[TaskId, CronTrigger, Callable]]
        ](100)
        self.supervisor = TaskSupervisor()
        self._event_bus = _get_event_bus()
        self._task_commands_consumer = self._event_bus.consumer("task.commands")

    def connect_scheduler(self, scheduler: TrioScheduler) -> None:
        """
        Connect TaskManager and Scheduler through one Channel.

        Channel:
            - Executable Channel

        See channel.py for Channel implementation.
        """
        scheduler.add_executable_channel(self._scheduled_executables)

    async def on_start(self):
        """Main loop for the TaskManager, runs forever."""
        await self.supervisor.start(self._nursery)
        self._nursery.start_soon(self._process)

    async def on_stop(self):
        """Close channel."""
        await self.supervisor.stop()
        await self._scheduled_executables.aclose()

    async def _process(self):
        async for cmd, ctx in self._task_commands_consumer.channel:
            if cmd is TaskManagerCommand.CREATE:
                await self._create_task(ctx)
            elif cmd is TaskManagerCommand.DELETE:
                await self._delete_task(ctx)

    async def _create_task(self, ctx: CreateContext):
        """Create a task from context, register it, start to supervise."""

        builder = TASK_REGISTER.get(type(ctx))
        if builder is None:
            return
        task = builder(self, ctx)
        # Start supervised
        if task:
            # Scheduled tasks run unsupervised
            if isinstance(task, ScheduledSourceTask):
                self._nursery.start_soon(task.start, self._nursery)
            else:
                await self.supervisor.start_supervising(task)
            logger.success(f"[TaskManager] registered task '{ctx.name}'")

    async def _delete_task(self, ctx: CreateContext):
        if isinstance(ctx, CreateHTTPSourceContext | CreateHTTPTableContext):
            # If task is a ScheduledSourceTask, evict it from the scheduler
            await self._scheduled_executables.send((SchedulerCommand.EVICT, ctx.name))
        else:
            await self.supervisor.stop_supervising(ctx.name)
