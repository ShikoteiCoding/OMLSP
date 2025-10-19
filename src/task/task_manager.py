"""
Task Manager managing registration and running of Tasks.
"""

from duckdb import DuckDBPyConnection, connect
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
)
from engine.engine import (
    build_lookup_table_prehook,
    build_continuous_source_executable,
    build_scheduled_source_executable,
    build_sink_executable,
    build_transform_executable,
)
from task.task import (
    TaskId,
    BaseTaskT,
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
    registry_conn: DuckDBPyConnection
    exec_conn: DuckDBPyConnection

    #: Scheduler (trio compatible) to register
    #: short lived or long lived processes
    scheduler: TrioScheduler

    #: Reference to all sources by task id
    #: TODO: to deprecate for below mapping
    _sources: dict[TaskId, BaseSourceTaskT] = {}

    #: Reference to all tasks by task id
    _task_id_to_task: dict[TaskId, BaseTaskT] = {}

    #: Outgoing Task context to be orchestrated
    _tasks_to_deploy: Channel[TaskContext]

    #: Outgoing channel to send jobs to scheduler
    _scheduled_executables: Channel[Callable | tuple[Callable, BaseTrigger]]

    def __init__(self, registry_conn: DuckDBPyConnection):
        super().__init__(name="TaskManager")
        self.registry_conn = registry_conn
        # created once and lives until tasks stop
        self.exec_conn = connect(database=":memory:")
        self._scheduled_executables = Channel[Callable | tuple[Callable, BaseTrigger]](
            100
        )

    def add_taskctx_channel(self, channel: Channel[TaskContext]):
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

    async def _process(self):
        async for taskctx in self._tasks_to_deploy:
            await self._register_one_task(taskctx)

    async def _register_one_task(self, ctx: TaskContext) -> None:
        task_id = ctx.name
        task = None

        if isinstance(ctx, SinkTaskContext):
            # TODO: add Transform task to handle subqueries
            # TODO: subscribe to many upstreams
            task = SinkTask(task_id, self.exec_conn)
            for name in ctx.upstreams:
                task.subscribe(self._sources[name].get_sender())
            self._task_id_to_task[task_id] = task.register(
                build_sink_executable(ctx, self.registry_conn)
            )
            await self._scheduled_executables.send(task.run)
            logger.success(f"[TaskManager] registered sink task '{task_id}'")

        elif isinstance(ctx, ScheduledTaskContext):
            # Executable could be attached to context
            # But we might want it dynamic later (i.e built at run time)
            task = ScheduledSourceTask[ctx._out_type](task_id, self.registry_conn)
            self._sources[task_id] = task.register(
                build_scheduled_source_executable(ctx)
            )
            await self._scheduled_executables.send((task.run, ctx.trigger))
            logger.success(
                f"[TaskManager] registered scheduled source task '{task_id}'"
            )

        elif isinstance(ctx, ContinousTaskContext):
            # Executable could be attached to context
            # But we might want it dynamic later (i.e built at run time)
            task = ContinuousSourceTask[ctx._out_type](
                task_id, self.registry_conn, self._nursery
            )

            # TODO: make WS Task dynamic by registering the on_start function
            # design idea, make the continuous source executable return
            # on_start func and on_run func. on_start will have "waiters"
            # and timeout logic
            self._sources[task_id] = task.register(
                build_continuous_source_executable(ctx, self.registry_conn)
            )
            await self._scheduled_executables.send((task.run))
            logger.success(
                f"[TaskManager] registered continuous source task '{task_id}'"
            )

        elif isinstance(ctx, CreateHTTPLookupTableContext):
            # TODO: is this the place to build lookup ? grr
            build_lookup_table_prehook(ctx, self.exec_conn, self.registry_conn)
            logger.success(f"[TaskManager] registered lookup executables '{task_id}'")

        elif isinstance(ctx, TransformTaskContext):
            task = TransformTask[ctx._out_type](task_id, self.exec_conn)
            for name in ctx.upstreams:
                task.subscribe(self._sources[name].get_sender())

            self._task_id_to_task[task_id] = task.register(
                build_transform_executable(ctx, self.registry_conn)
            )
            await self._scheduled_executables.send(task.run)
            logger.success(f"[TaskManager] registered transform task '{task_id}'")
