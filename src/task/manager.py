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
    DropSimpleContext,
    DropCascadeContext,
)
from apscheduler.triggers.cron import CronTrigger

from task.task import (
    BaseTask,
    BaseTaskSender,
    ScheduledSourceTask,
)
from task.types import TaskId


from task.supervisor import TaskSupervisor
from task.dependency_graph import TaskGraph
from task.catalog import TaskCatalog
from task.builder_registry import TASK_REGISTER

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
    _tasks_to_supervise: Channel[BaseTask]

    def __init__(
        self, backend_conn: DuckDBPyConnection, transform_conn: DuckDBPyConnection
    ):
        super().__init__(name="TaskManager")
        self.backend_conn = backend_conn
        self.transform_conn = transform_conn
        self._scheduled_executables = Channel[
            tuple[SchedulerCommand, TaskId | tuple[TaskId, CronTrigger, Callable]]
        ](100)
        self._tasks_to_supervise = Channel[BaseTask](100)
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
        self._nursery.start_soon(self._process)

    async def on_stop(self):
        """Close channel."""
        await self._scheduled_executables.aclose()
        await self._tasks_to_supervise.aclose()

    async def _process(self):
        async for ctx in self._task_events:
            if isinstance(ctx, CreateContext):
                await self._create_task(ctx)
            elif isinstance(ctx, DropContext):
                await self._delete_task(ctx)

    async def _create_task(self, ctx: CreateContext):
        """Create a task from context, register it, add graph deps, start supervisor."""
        builder = TASK_REGISTER.get(type(ctx))

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
            self.catalog.add(task, ctx.has_data, type(ctx))
            # Scheduled tasks run unsupervised
            if isinstance(task, ScheduledSourceTask):
                self._nursery.start_soon(task.start, self._nursery)
            else:
                await self._tasks_to_supervise.send(task)
            logger.success(f"[TaskManager] registered task '{ctx.name}'")

    async def _delete_task(self, ctx: DropContext):
        name = ctx.name

        if isinstance(ctx, DropSimpleContext):
            is_leaf = self.graph.is_a_leaf(name)
            if not is_leaf:
                logger.warning(f"[TaskManager] task is not a leaf '{name}'")
                return
            self.graph.drop_leaf(name)
            task, has_data, metadata_table, metadata_column = self.catalog.get(name)
            if task:
                await self._delete_task_from_system(
                    task, name, has_data, metadata_table, metadata_column
                )
            logger.success(f"[TaskManager] dropped task: {name}")

        if isinstance(ctx, DropCascadeContext):
            dropped_from_graph = self.graph.drop_recursive(name)
            if not dropped_from_graph:
                logger.warning(f"[TaskManager] nothing to drop for '{name}'")
                return

            for n in dropped_from_graph:
                task, has_data, metadata_table, metadata_column = self.catalog.get(n)
                if task:
                    await self._delete_task_from_system(
                        task, n, has_data, metadata_table, metadata_column
                    )

            logger.success(f"[TaskManager] dropped cascade tasks: {dropped_from_graph}")

    async def _delete_task_from_system(
        self,
        task: BaseTask,
        name: str,
        has_data: bool,
        metadata_table: str,
        metadata_column: str,
    ):
        if isinstance(task, ScheduledSourceTask):
            # If task is a ScheduledSourceTask, evict it from the scheduler
            await self._scheduled_executables.send(
                (SchedulerCommand.EVICT, task.task_id)
            )
        # Stop and clean up the task
        self.catalog.remove(task)
        await task.on_stop()
        del task
        # Delete associated metadata
        # TODO: DROP SECRET, MACRO etc
        delete_metadata(self.backend_conn, metadata_table, metadata_column, name)
        if has_data:
            sql_query = f"DROP TABLE {name}"
            self.backend_conn.sql(sql_query)
