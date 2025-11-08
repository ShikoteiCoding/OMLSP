"""
Task Manager managing registration and running of Tasks.
"""

from duckdb import DuckDBPyConnection
from apscheduler.triggers.base import BaseTrigger

from channel import Channel
from scheduler import TrioScheduler
from context.context import (
    CreateHTTPLookupTableContext,
    SinkTaskContext,
    ScheduledTaskContext,
    ContinousTaskContext,
    TransformTaskContext,
    TaskContext,
    DropContext,
)
from engine.engine import (
    build_lookup_table_prehook,
    build_continuous_source_executable,
    build_scheduled_source_executable,
    build_sink_executable,
    build_transform_executable,
)

from metadata import (
    delete_table_metadata,
    delete_source_metadata,
    delete_view_metadata,
    delete_sink_metadata,
    delete_secret_metadata,
)

from task.task import (
    TaskId,
    BaseSourceTaskT,
    ContinuousSourceTask,
    ScheduledSourceTask,
    SinkTask,
    TransformTask,
)
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

    #: Reference to all sources by task id
    #: TODO: to deprecate for below mapping
    _sources: dict[TaskId, BaseSourceTaskT] = {}

    #: Reference to all tasks by task id
    _task_id_to_task: dict[TaskId, BaseSourceTaskT] = {}

    #: Outgoing Task context to be orchestrated
    _tasks_to_deploy: Channel[TaskContext | DropContext]

    #: Outgoing channel to send jobs to scheduler
    _scheduled_executables: Channel[Callable | tuple[Callable, BaseTrigger]]

    def __init__(
        self, backend_conn: DuckDBPyConnection, transform_conn: DuckDBPyConnection
    ):
        super().__init__(name="TaskManager")
        self.backend_conn = backend_conn
        # created once and lives until tasks stop
        self.transform_conn = transform_conn
        self._scheduled_executables = Channel[Callable | tuple[Callable, BaseTrigger]](
            100
        )

    def add_taskctx_channel(self, channel: Channel[TaskContext | DropContext]):
        self._tasks_to_deploy = channel

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
        self._nursery.start_soon(self._process)

    async def on_stop(self):
        """Close channel."""
        await self._scheduled_executables.aclose()

    async def _process(self):
        async for ctx in self._tasks_to_deploy:
            if isinstance(ctx, TaskContext):
                await self._register_one_task(ctx)
            elif isinstance(ctx, DropContext):
                await self._handle_drop(ctx)

    async def _handle_drop(self, ctx: DropContext):
        name = getattr(ctx, "name", None)
        if not name:
            logger.warning("[TaskManager] DROP received without name: {}", ctx)
            return

        # Stop task if running
        stopped = False
        task = self._task_id_to_task.get(name)
        if task:
            try:
                await task.on_stop()
                stopped = True
            except Exception as e:
                logger.warning(f"[TaskManager] Failed to stop task '{name}': {e}")

        # Drop from DuckDB backend
        try:
            # TODO remove from metadata
            self.backend_conn.sql(ctx.user_query)

            # Delete metadata entry
            drop_type = ctx.drop_type.lower()
            if drop_type == "table":
                delete_table_metadata(self.backend_conn, name)
            elif drop_type == "source":
                delete_source_metadata(self.backend_conn, name)
            elif drop_type == "view":
                delete_view_metadata(self.backend_conn, name)
            elif drop_type == "sink":
                delete_sink_metadata(self.backend_conn, name)
            elif drop_type == "secret":
                delete_secret_metadata(self.backend_conn, name)

            msg = f"Dropped {ctx.drop_type.lower()} '{name}'"
            if stopped:
                msg += " (task stopped)"
            logger.success(f"[TaskManager] {msg}")
        except Exception as e:
            logger.warning(f"[TaskManager] Failed to drop '{name}': {e}")

    async def _register_one_task(self, ctx: TaskContext) -> None:
        task_id = ctx.name
        task = None

        if isinstance(ctx, SinkTaskContext):
            # TODO: add Transform task to handle subqueries
            # TODO: subscribe to many upstreams
            task = SinkTask(task_id, self.transform_conn)
            for name in ctx.upstreams:
                task.subscribe(self._sources[name].get_sender())
            self._task_id_to_task[task_id] = task.register(
                build_sink_executable(ctx, self.backend_conn)
            )
            self._nursery.start_soon(task.start, self._nursery)
            logger.success(f"[TaskManager] registered sink task '{task_id}'")

        elif isinstance(ctx, ScheduledTaskContext):
            # Executable could be attached to context
            # But we might want it dynamic later (i.e built at run time)
            task = ScheduledSourceTask[ctx._out_type](
                task_id, self.backend_conn, self._scheduled_executables, ctx.trigger
            )
            self._sources[task_id] = task.register(
                build_scheduled_source_executable(ctx)
            )
            self._nursery.start_soon(task.start, self._nursery)
            logger.success(
                f"[TaskManager] registered scheduled source task '{task_id}'"
            )

        elif isinstance(ctx, ContinousTaskContext):
            # Executable could be attached to context
            # But we might want it dynamic later (i.e built at run time)
            task = ContinuousSourceTask[ctx._out_type](
                task_id, self.backend_conn, self._nursery
            )

            # TODO: make WS Task dynamic by registering the on_start function
            # design idea, make the continuous source executable return
            # on_start func and on_run func. on_start will have "waiters"
            # and timeout logic
            self._sources[task_id] = task.register(
                build_continuous_source_executable(ctx, self.backend_conn)
            )
            self._nursery.start_soon(task.start, self._nursery)
            logger.success(
                f"[TaskManager] registered continuous source task '{task_id}'"
            )

        elif isinstance(ctx, CreateHTTPLookupTableContext):
            # TODO: is this the place to build lookup ? grr
            build_lookup_table_prehook(ctx, self.backend_conn)
            logger.success(f"[TaskManager] registered lookup executables '{task_id}'")

        elif isinstance(ctx, TransformTaskContext):
            task = TransformTask[ctx._out_type](task_id, self.transform_conn)
            for name in ctx.upstreams:
                task.subscribe(self._sources[name].get_sender())

            self._task_id_to_task[task_id] = task.register(
                build_transform_executable(ctx, self.backend_conn)
            )
            self._nursery.start_soon(task.start, self._nursery)
            logger.success(f"[TaskManager] registered transform task '{task_id}'")

        if task is not None:
            self.add_dependency(task)
