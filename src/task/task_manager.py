"""
Task Manager managing registration and running of Tasks.
"""

import trio

from duckdb import DuckDBPyConnection

from channel import Channel
from scheduler import TrioScheduler
from context.context import (
    CreateHTTPLookupTableContext,
    SinkTaskContext,
    ScheduledTaskContext,
    ContinousTaskContext,
    TransformTaskContext,
    TaskContext,
    CreateMaterializedViewContext,
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


from loguru import logger

__all__ = ["TaskManager"]


class TaskManager(Service):
    #: Duckdb connection
    conn: DuckDBPyConnection

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

    def __init__(self, conn: DuckDBPyConnection, scheduler: TrioScheduler):
        super().__init__(name="TaskManager")
        self.conn = conn
        self.scheduler = scheduler
        self._token = trio.lowlevel.current_trio_token()

    def add_taskctx_channel(self, channel: Channel[TaskContext]):
        self._tasks_to_deploy = channel

    async def on_stop(self):
        """"""
        self.scheduler.shutdown(False)

    async def on_start(self):
        """Main loop for the TaskManager, runs forever."""

        self.scheduler._configure(
            {"_nursery": self._nursery, "_trio_token": self._token}
        )
        self.scheduler.start()
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
            task = SinkTask(task_id, self.conn)
            for name in ctx.upstreams:
                task.subscribe(self._sources[name].get_sender())
            self._task_id_to_task[task_id] = task.register(build_sink_executable(ctx))
            _ = self.scheduler.add_job(func=task.run)
            logger.success(f"[TaskManager] registered sink task '{task_id}'")

        elif isinstance(ctx, ScheduledTaskContext):
            # Executable could be attached to context
            # But we might want it dynamic later (i.e built at run time)
            task = ScheduledSourceTask[ctx._out_type](task_id, self.conn)
            self._sources[task_id] = task.register(
                build_scheduled_source_executable(ctx)
            )
            _ = self.scheduler.add_job(
                func=task.run,
                trigger=ctx.trigger,
                misfire_grace_time=20,
            )
            logger.success(
                f"[TaskManager] registered scheduled source task '{task_id}'"
            )

        elif isinstance(ctx, ContinousTaskContext):
            # Executable could be attached to context
            # But we might want it dynamic later (i.e built at run time)
            task = ContinuousSourceTask[ctx._out_type](task_id, self.conn)

            # TODO: make WS Task dynamic by registering the on_start function
            # design idea, make the continuous source executable return
            # on_start func and on_run func. on_start will have "waiters"
            # and timeout logic
            self._sources[task_id] = task.register(
                build_continuous_source_executable(ctx)
            )
            _ = self.scheduler.add_job(
                func=task.run,
            )
            logger.success(
                f"[TaskManager] registered continuous source task '{task_id}'"
            )

        elif isinstance(ctx, CreateHTTPLookupTableContext):
            # TODO: is this the place to build lookup ? grr
            build_lookup_table_prehook(ctx, self.conn)
            logger.success(f"[TaskManager] registered lookup executables '{task_id}'")

        elif isinstance(ctx, TransformTaskContext):
            task = TransformTask[ctx._out_type](task_id, self.conn)
            for name in ctx.upstreams:
                task.subscribe(self._sources[name].get_sender())

            is_materialized = False
            if isinstance(ctx, CreateMaterializedViewContext):
                is_materialized = True

            self._task_id_to_task[task_id] = task.register(
                build_transform_executable(ctx, is_materialized)
            )
            _ = self.scheduler.add_job(
                func=task.run,
            )
            logger.success(f"[TaskManager] registered transform task '{task_id}'")
