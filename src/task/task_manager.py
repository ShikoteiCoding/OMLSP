import trio

from duckdb import DuckDBPyConnection

from channel import Channel
from scheduler import TrioScheduler
from context.context import (
    CreateLookupTableContext,
    CreateSinkContext,
    SourceTaskContext,
    TaskContext,
)
from engine.engine import (
    build_lookup_table_prehook,
    build_source_executable,
)
from sink.sink import build_sink_connector
from task.task import TaskId, BaseTask, SourceTask, SinkTask


from loguru import logger


class TaskManager:
    conn: DuckDBPyConnection
    scheduler: TrioScheduler

    _sources: dict[TaskId, SourceTask] = {}
    _task_id_to_task: dict[TaskId, BaseTask] = {}
    _nursery: trio.Nursery
    _taskctx_channel: Channel[TaskContext]

    def __init__(self, conn: DuckDBPyConnection, scheduler: TrioScheduler):
        self.conn = conn
        self.scheduler = scheduler
        self._token = trio.lowlevel.current_trio_token()

    async def add_taskctx_channel(self, channel: Channel[TaskContext]):
        self._taskctx_channel = channel

    async def run(self):
        """Main loop for the TaskManager, runs forever."""
        self._running = True

        async with trio.open_nursery() as nursery:
            self._nursery = nursery
            self.scheduler._configure({"_nursery": nursery, "_trio_token": self._token})
            self.scheduler.start()
            nursery.start_soon(self._process)
            nursery.start_soon(self._watch_for_shutdown)

    async def _process(self):
        async for taskctx in self._taskctx_channel:
            await self._register_one_task(taskctx)

    async def _register_one_task(self, ctx: TaskContext) -> None:
        task_id = ctx.name
        task = None

        if isinstance(ctx, CreateSinkContext):
            # TODO: add Transform task to handle subqueries
            # TODO: subscribe to many upstreams
            task = SinkTask(task_id)
            for name in ctx.upstreams:
                task.subscribe(self._sources[name].get_sender())
            self._task_id_to_task[task_id] = task.register(
                build_sink_connector(ctx.properties)
            )
            _ = self.scheduler.add_job(func=task.run)
            logger.info(f"[TaskManager] registered sink task '{task_id}'")

        elif isinstance(ctx, SourceTaskContext):
            # Executable could be attached to context
            # But we might want it dynamic later (i.e built at run time)
            task = SourceTask(task_id, self.conn)
            self._sources[task_id] = task.register(build_source_executable(ctx))
            _ = self.scheduler.add_job(func=task.run, trigger=ctx.trigger)
            logger.info(f"[TaskManager] registered source task '{task_id}'")

        elif isinstance(ctx, CreateLookupTableContext):
            # TODO: is this the place to build lookup ? grr
            build_lookup_table_prehook(ctx, self.conn)
            logger.info(f"[TaskManager] built lookup table'{task_id}'")

    async def _watch_for_shutdown(self):
        try:
            while self._running:
                await trio.sleep(1)
        finally:
            logger.info("[Runner] Shutdown initiated.")
            self._running = False

    def __repr__(self) -> str:
        s = ["sources:"]
        for _, task in self._sources.items():
            s.append(str(task))
        s.append("dags:")
        # for _, task in self._task_id_to_task.items():
        #     s.append(str(task))
        return "\n".join(s)
