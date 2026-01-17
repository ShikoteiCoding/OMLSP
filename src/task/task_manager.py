"""
Task Manager managing registration and running of Tasks.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from duckdb import DuckDBPyConnection
from loguru import logger

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

if TYPE_CHECKING:
    from scheduler.scheduler import TrioScheduler


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
    senders: dict[TaskId, BaseTaskSender] = {}

    def __init__(
        self, backend_conn: DuckDBPyConnection, transform_conn: DuckDBPyConnection
    ):
        super().__init__(name="TaskManager")
        self.backend_conn = backend_conn
        self.transform_conn = transform_conn
        self.supervisor = TaskSupervisor()

    async def on_start(self):
        """Main loop for the TaskManager, runs forever."""
        await self.supervisor.start(self._nursery)

    async def on_stop(self):
        """Stop supervisor strategy."""
        await self.supervisor.stop()

    async def on_receive(self, cmd: TaskManagerCommand, ctx: CreateContext) -> None:
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
        # Scheduled Task rely on the Scheduler for supervising
        if isinstance(ctx, CreateHTTPSourceContext | CreateHTTPTableContext):
            await self.channel_registry.publish(
                "TrioScheduler", (SchedulerCommand.EVICT, ctx.name)
            )
        # Other Task(s) rely on the Supervisor
        else:
            await self.supervisor.stop_supervising(ctx.name)
