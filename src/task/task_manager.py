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

from task.task import (
    BaseTask,
    BaseTaskSender,
    ScheduledSourceTask,
)
from task.types import TaskId


from task.task_supervisor import TaskSupervisor
from task.task_graph import TaskGraph
from task.task_registry import TaskRegistry
from task.task_builders import TASK_BUILDERS

from services import Service

from typing import Any

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

    #: Reference to all sources by task id
    #: TODO: to deprecate for below mapping
    _sources: dict[TaskId, BaseTaskSender] = {}
    _sinks: dict[TaskId, Any] = {}

    #: Reference to all tasks by task id
    _task_id_to_task: dict[TaskId, BaseTask] = {}

    #: Outgoing Task context to be orchestrated
    _tasks_to_deploy: Channel[CreateContext]
    _tasks_to_cancel: Channel[DropContext]

    #: Outgoing channel to send jobs to scheduler
    _scheduled_executables: Channel[tuple[SchedulerCommand, Any]]

    def __init__(
        self, backend_conn: DuckDBPyConnection, transform_conn: DuckDBPyConnection
    ):
        super().__init__(name="TaskManager")
        self.backend_conn = backend_conn
        self.transform_conn = transform_conn
        self._scheduled_executables = Channel[tuple[SchedulerCommand, Any]](100)
        self.registry = TaskRegistry()
        self.graph = TaskGraph()
        self.supervisor = TaskSupervisor()

    def add_taskctx_channel(self, channel: Channel[CreateContext]):
        self._tasks_to_deploy = channel

    def add_task_cancel_channel(self, channel: Channel[DropContext]):
        self._tasks_to_cancel = channel

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
        # TODO: change to inheritance later
        self.supervisor = TaskSupervisor()
        self._nursery.start_soon(self._process)
        self._nursery.start_soon(self._drop)

    async def on_stop(self):
        """Close channel."""
        await self._scheduled_executables.aclose()

    async def _process(self):
        async for ctx in self._tasks_to_deploy:
            if isinstance(ctx, CreateContext):
                await self._register_one_task(ctx)

    async def _drop(self):
        async for ctx in self._tasks_to_cancel:
            if isinstance(ctx, DropContext):
                await self._handle_drop(ctx)

    async def _register_one_task(self, ctx: CreateContext):
        """Create a task from context, register it, add graph deps, start supervisor."""
        builder = TASK_BUILDERS.get(type(ctx))

        if not builder:
            logger.error(f"No task builder for context type: {type(ctx).__name__}")
            return
        # Register the task
        task = builder(self, ctx)

        # TODO: include scheduled task
        self.graph.ensure_vertex(ctx.name)
        # Register dependencies in the graph
        for parent in getattr(ctx, "upstreams", []):
            self.graph.add_vertex(parent, ctx.name)

        # Start supervised
        # Scheduled tasks return None → nothing to supervise
        if task:
            self.registry.register(task)
            self._nursery.start_soon(self.supervisor.supervise, task)
            self._task_id_to_task[ctx.name] = task

            logger.success(f"[TaskManager] registered task '{ctx.name}'")

    async def _handle_drop(self, ctx: DropContext):
        name = ctx.name

        to_drop = self.graph.drop_recursive(name)
        if not to_drop:
            logger.warning(f"[TaskManager] nothing to drop for '{name}'")
            return

        for n in to_drop:
            task = self.registry.get(n)
            if task:
                task.stopped = True
                self.registry.unregister(task)
                await task.on_stop()
                # remove from scheduler
                if isinstance(task, ScheduledSourceTask):
                    await self._scheduled_executables.send(
                        (SchedulerCommand.EVICT, task.task_id)
                    )

        # TODO: implement later
        # Cleanup metadata
        # for n in to_drop:
        #     if ctx.has_metadata:
        #         delete_metadata(self.backend_conn, ctx.metadata, ctx.metadata_column, n)
        #         self.backend_conn.sql(ctx.user_query)

        logger.success(f"[TaskManager] dropped tasks: {to_drop}")
