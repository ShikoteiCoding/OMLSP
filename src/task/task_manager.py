"""
Task Manager managing registration and running of Tasks.
"""

from duckdb import DuckDBPyConnection

from channel import Channel
from scheduler.scheduler import TrioScheduler
from scheduler.types import SchedulerCommand
from context.context import (
    CreateContext,
    DropContext,
)
from apscheduler.triggers.cron import CronTrigger

from task.task import (
    BaseTask,
    BaseTaskSender,
    ScheduledSourceTask,
)
from task.types import TaskId, SupervisorCommand


from task.task_supervisor import TaskSupervisor
from task.task_graph import TaskGraph
from task.task_catalog import TaskCatalog
from task.task_builders import TASK_BUILDERS

from metadata import delete_metadata

from services import Service

from typing import Callable

from loguru import logger

__all__ = ["TaskManager"]


class TaskManager(Service):
    #: Duckdb connections
    backend_conn: DuckDBPyConnection
    transform_conn: DuckDBPyConnection

    #: Scheduler (trio compatible) to register
    #: short lived or long lived processes
    scheduler: TrioScheduler
    #: Supervisor to restart tasks
    supervisor: TaskSupervisor
    #: Catalog of running tasks
    catalog: TaskCatalog
    #: Graph of dependancy between running taks
    graph: TaskGraph

    #: Reference to all sources by task id
    #: TODO: to deprecate for below mapping
    _sources: dict[TaskId, BaseTaskSender] = {}

    #: Reference to all tasks by task id
    _task_id_to_task: dict[TaskId, tuple[BaseTask, bool]] = {}

    #: Outgoing Task context to be orchestrated
    _task_events: Channel[CreateContext | DropContext]

    #: Outgoing channel to send jobs to scheduler
    _scheduled_executables: Channel[
        tuple[SchedulerCommand, TaskId | tuple[TaskId, CronTrigger, Callable]]
    ]

    #: Outgoing channel to send task to supervisor
    _tasks_to_supervise: Channel[tuple[SupervisorCommand, BaseTask]]

    def __init__(
        self, backend_conn: DuckDBPyConnection, transform_conn: DuckDBPyConnection
    ):
        super().__init__(name="TaskManager")
        self.backend_conn = backend_conn
        self.transform_conn = transform_conn
        self._scheduled_executables = Channel[
            tuple[SchedulerCommand, TaskId | tuple[TaskId, CronTrigger, Callable]]
        ](100)
        self._tasks_to_supervise = Channel[tuple[SupervisorCommand, BaseTask]](100)
        self.catalog = TaskCatalog()
        self.graph = TaskGraph()

    def add_taskctx_channel(self, channel: Channel[CreateContext | DropContext]):
        self._task_events = channel

    def connect_scheduler(self, scheduler: TrioScheduler) -> None:
        """
        Connect TaskManager and Scheduler through one Channel.

        Channel:
            - Executable Channel

        See channel.py for Channel implementation.
        """
        scheduler.add_executable_channel(self._scheduled_executables)

    def connect_supervisor(self, supervisor: TaskSupervisor) -> None:
        """
        Connect TaskManager and TaskSupervisor through one Channel.

        Channel:
            - Task Channel

        See channel.py for Channel implementation.
        """
        supervisor.add_tasks_to_supervise_channel(self._tasks_to_supervise)

    async def on_start(self):
        """Main loop for the TaskManager, runs forever."""
        self._nursery.start_soon(self._create)

    async def on_stop(self):
        """Close channel."""
        await self._scheduled_executables.aclose()
        await self._tasks_to_supervise.aclose()

    async def _create(self):
        async for ctx in self._task_events:
            if isinstance(ctx, CreateContext):
                await self._register_one_task(ctx)
            elif isinstance(ctx, DropContext):
                await self._delete(ctx)

    async def _register_one_task(self, ctx: CreateContext):
        """Create a task from context, register it, add graph deps, start supervisor."""
        builder = TASK_BUILDERS.get(type(ctx))

        if not builder:
            logger.error(f"No task builder for context type: {type(ctx).__name__}")
            return
        # Register the task
        task = builder(self, ctx)

        self.graph.ensure_vertex(ctx.name)
        # Register dependencies in the graph
        for parent in getattr(ctx, "upstreams", []):
            self.graph.add_vertex(parent, ctx.name)

        # Start supervised
        if task:
            self.catalog.add(task, ctx.has_data)
            # Scheduled tasks run unsupervised
            if isinstance(task, ScheduledSourceTask):
                self._nursery.start_soon(task.start, self._nursery)
            else:
                await self._tasks_to_supervise.send((SupervisorCommand.START, task))
            logger.success(f"[TaskManager] registered task '{ctx.name}'")

    async def _delete(self, ctx: DropContext):
        name = ctx.name

        dropped_from_graph = self.graph.drop_recursive(name)
        if not dropped_from_graph:
            logger.warning(f"[TaskManager] nothing to drop for '{name}'")
            return

        for n in dropped_from_graph:
            task, has_data = self.catalog.get(n)
            if task:
                # remove from scheduler or supervisor
                if isinstance(task, ScheduledSourceTask):
                    await self._scheduled_executables.send(
                        (SchedulerCommand.EVICT, task.task_id)
                    )
                else:
                    await self._tasks_to_supervise.send((SupervisorCommand.STOP, task))
                await task.on_stop()
                self.catalog.remove(task)
                del task
            # TODO: refactor later, need to delete dependencies metadata
            delete_metadata(self.backend_conn, ctx.metadata, ctx.metadata_column, n)
            # if has_data:
            #     self.backend_conn.sql(ctx.user_query)

        logger.success(f"[TaskManager] dropped tasks: {dropped_from_graph}")
