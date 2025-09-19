import trio

from commons.utils import Channel
from context.context import (
    TaskContext,
    SourceTaskContext,
    CreateLookupTableContext,
    CreateSinkContext,
)
from engine.engine import (
    build_source_executable,
    build_lookup_table_prehook,
)
from task.task import TaskId, SourceTask, SinkTask

from duckdb import DuckDBPyConnection


from loguru import logger


class TaskManager:
    conn: DuckDBPyConnection

    _sources: dict[TaskId, SourceTask] = {}
    _nursery: trio.Nursery
    _taskctx_channel: Channel[TaskContext]

    def __init__(self, conn: DuckDBPyConnection):
        self.conn = conn

    async def add_taskctx_channel(self, channel: Channel[TaskContext]):
        self._taskctx_channel = channel

    async def run(self):
        """Main loop for the TaskManager, runs forever."""
        self._running = True

        async with trio.open_nursery() as nursery:
            self._nursery = nursery
            nursery.start_soon(self._process)
            nursery.start_soon(self._watch_for_shutdown)

    async def _register_one_task(self, ctx: TaskContext):
        task_id = ctx.name

        if isinstance(ctx, CreateSinkContext):
            # TODO: add Transform task to handle subqueries
            sink = SinkTask(task_id, self.conn)
            for upstream in ctx.upstreams:
                sink.subscribe(self._sources[upstream].get_sender())

            self._nursery.start_soon(sink.run)  # use parent nursery
            logger.info(f'[TaskManager] registered sink task "{task_id}"')

        elif isinstance(ctx, SourceTaskContext):
            # TODO: add trigger
            source = SourceTask(task_id, self.conn)
            self._sources[task_id] = source.register(build_source_executable(ctx))

            self._nursery.start_soon(self._sources[task_id].run)  # use parent nursery
            logger.info(f'[TaskManager] registered source task "{task_id}"')

        elif isinstance(ctx, CreateLookupTableContext):
            build_lookup_table_prehook(ctx, self.conn)
            logger.info(f'[TaskManager] registered lookup task "{task_id}"')
            return

    async def _process(self):
        async for taskctx in self._taskctx_channel:
            await self._register_one_task(taskctx)

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
