import trio

from duckdb import DuckDBPyConnection
from loguru import logger

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

from duckdb import DuckDBPyConnection


from loguru import logger


class TaskManager:
    conn: DuckDBPyConnection
    scheduler: TrioScheduler

    _sources: dict[TaskId, SourceTask] = {}
    _nursery: trio.Nursery
    _taskctx_channel: Channel[TaskContext]

    def __init__(self, conn: DuckDBPyConnection, scheduler: TrioScheduler):
        self.conn = conn
        self.scheduler = scheduler

    async def add_taskctx_channel(self, channel: Channel[TaskContext]):
        self._taskctx_channel = channel

    async def run(self):
        """Main loop for the TaskManager, runs forever."""
        self._running = True

        async with trio.open_nursery() as nursery:
            # TODO: Reverse dependency injection
            # TaskManager & scheduler should not be linked together
            token = trio.lowlevel.current_trio_token()
            self.scheduler._configure({"_nursery": nursery, "_trio_token": token})
            self.scheduler.start()

            self._nursery = nursery

            nursery.start_soon(self._process)
            nursery.start_soon(self._watch_for_shutdown)

            logger.info("[TaskManager] Started.")
            while self._running:
                await trio.sleep(1)

        logger.info("[TaskManager] Stopped.")

    def _register_one_task(self, ctx: TaskContext):
        task_id = ctx.name

        if isinstance(ctx, CreateSinkContext):
            # TODO: add Transform task to handle subqueries
            # TODO: subscribe to many upstreams
            sink = SinkTask(task_id)
            # sink.register(build_sink_connector(ctx.properties))
            for upstream in ctx.upstreams:
                sink.subscribe(self._sources[upstream].get_sender())

            return sink

        elif isinstance(ctx, SourceTaskContext):
            source = SourceTask(task_id, self.conn, ctx.trigger)
            self._sources[task_id] = source.register(build_source_executable(ctx))
            _ = self.scheduler.add_job(func=source.run, trigger=ctx.trigger)
            return None

        elif isinstance(ctx, CreateLookupTableContext):
            build_lookup_table_prehook(ctx, self.conn)
            return None

        else:
            logger.warning(f"Unknown task context: {ctx}")
            return None

    async def _process(self):
        async for taskctx in self._taskctx_channel:
            task = self._register_one_task(taskctx)
            # logger.info(
            #     f'[TaskManager] registered {task.__class__.__name__} "{task.task_id}"'
            # )
            if task is None:
                continue
            self._nursery.start_soon(task.run)

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
        for _, task in self._task_id_to_task.items():
            s.append(str(task))
        return "\n".join(s)
