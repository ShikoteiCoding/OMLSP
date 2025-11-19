"""
Task Manager managing registration and running of Tasks.
"""

from duckdb import DuckDBPyConnection
from apscheduler.triggers.cron import CronTrigger

from channel import Channel
from scheduler import TrioScheduler
from context.context import (
    CreateContext,
    CreateHTTPSourceContext,
    CreateHTTPTableContext,
    CreateHTTPLookupTableContext,
    CreateSinkContext,
    CreateViewContext,
    CreateWSSourceContext,
    CreateWSTableContext,
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
    BaseTask,
    BaseSourceTask,
    ContinuousSourceTask,
    ScheduledSourceTask,
    SinkTask,
    TransformTask,
)
from task.task_supervisor import TaskSupervisor

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

    #: Reference to all sources by task id
    #: TODO: to deprecate for below mapping
    _sources: dict[TaskId, BaseSourceTask] = {}

    #: Reference to all tasks by task id
    _task_id_to_task: dict[TaskId, BaseTask] = {}

    #: Outgoing Task context to be orchestrated
    _tasks_to_deploy: Channel[CreateContext]

    #: Outgoing channel to send jobs to scheduler
    _scheduled_executables: Channel[tuple[Callable, CronTrigger]]

    def __init__(
        self, backend_conn: DuckDBPyConnection, transform_conn: DuckDBPyConnection
    ):
        super().__init__(name="TaskManager")
        self.backend_conn = backend_conn
        self.transform_conn = transform_conn
        self._scheduled_executables = Channel[tuple[Callable, CronTrigger]](100)

    def add_taskctx_channel(self, channel: Channel[CreateContext]):
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
        # TODO: change to inheritance later
        self.supervisor = TaskSupervisor(self._nursery)
        self._nursery.start_soon(self._process)

    async def on_stop(self):
        """Close channel."""
        await self._scheduled_executables.aclose()

    async def _process(self):
        async for taskctx in self._tasks_to_deploy:
            await self._register_one_task(taskctx)

    async def _register_one_task(self, ctx: CreateContext) -> None:
        task_id = ctx.name
        task = None

        if isinstance(ctx, CreateSinkContext):
            # TODO: add Transform task to handle subqueries
            # TODO: subscribe to many upstreams
            task = SinkTask[ctx._out_type](task_id, self.transform_conn)
            for name in ctx.upstreams:
                task.subscribe(self._sources[name].get_sender())
            self._task_id_to_task[task_id] = task.register(
                build_sink_executable(ctx, self.backend_conn)
            )
            self._nursery.start_soon(self.supervisor.supervise, task)
            logger.success(f"[TaskManager] registered sink task '{task_id}'")

        elif isinstance(ctx, CreateHTTPTableContext | CreateHTTPSourceContext):
            # Executable could be attached to context
            # But we might want it dynamic later (i.e built at run time)
            task = ScheduledSourceTask[ctx._out_type](
                task_id, self.backend_conn, self._scheduled_executables, ctx.trigger
            )
            self._sources[task_id] = task.register(
                build_scheduled_source_executable(ctx)
            )

            # Scheduled task is unsupervised. When one batch fails, the
            # Scheduler will re instanciate in next trigger time
            self._nursery.start_soon(task.start, self._nursery)
            logger.success(
                f"[TaskManager] registered scheduled source task '{task_id}'"
            )

        elif isinstance(ctx, CreateWSTableContext | CreateWSSourceContext):
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
            self._nursery.start_soon(self.supervisor.supervise, task)
            logger.success(
                f"[TaskManager] registered continuous source task '{task_id}'"
            )

        elif isinstance(ctx, CreateHTTPLookupTableContext):
            # TODO: is this the place to build lookup ? grr
            build_lookup_table_prehook(ctx, self.backend_conn)
            logger.success(f"[TaskManager] registered lookup executables '{task_id}'")

        elif isinstance(ctx, CreateViewContext):
            task = TransformTask[ctx._out_type](task_id, self.transform_conn)
            for name in ctx.upstreams:
                task.subscribe(self._sources[name].get_sender())

            self._task_id_to_task[task_id] = task.register(
                build_transform_executable(ctx, self.backend_conn)
            )
            self._nursery.start_soon(self.supervisor.supervise, task)
            logger.success(f"[TaskManager] registered transform task '{task_id}'")

        if task is not None:
            self.add_dependency(task)
