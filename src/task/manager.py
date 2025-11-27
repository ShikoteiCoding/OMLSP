"""
Task Manager managing registration and running of Tasks.
"""

from duckdb import DuckDBPyConnection

from channel import Channel
from scheduler.scheduler import TrioScheduler
from scheduler.types import SchedulerCommand
from context.context import (
    CreateContext,
)
from apscheduler.triggers.cron import CronTrigger

from task.task import (
    BaseTaskSender,
    ScheduledSourceTask,
)
from task.types import TaskId


from task.supervisor import TaskSupervisor
from task.builder_registry import TASK_REGISTER

from services import Service

from typing import Callable

from loguru import logger

__all__ = ["TaskManager"]


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

    #: Reference to all sources by task id
    #: TODO: to deprecate for below mapping
    _sources: dict[TaskId, BaseTaskSender] = {}

    #: Outgoing Task context to be orchestrated
    _task_events: Channel[CreateContext | TaskId]

    #: Outgoing channel to send jobs to scheduler
    _scheduled_executables: Channel[
        tuple[SchedulerCommand, TaskId | tuple[TaskId, CronTrigger, Callable]]
    ]

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

    def add_taskctx_channel(self, channel: Channel[CreateContext | TaskId]):
        self._task_events = channel

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
        async for payload in self._task_events:
            if isinstance(payload, CreateContext):
                ctx = payload
                await self._create_task(ctx)
            elif isinstance(payload, TaskId):
                task_id = payload
                await self._delete_task(task_id)

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

    async def _delete_task(self, task_id: TaskId):
        await self.supervisor.stop_supervising(task_id)
