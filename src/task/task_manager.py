import trio

from commons.utils import Channel
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

    def _register_task(self, ctx: TaskContext) -> BaseTask | None:
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

            return source

        elif isinstance(ctx, CreateLookupTableContext):
            build_lookup_table_prehook(ctx, self.conn)
            return None

        else:
            logger.warning(f"Unknown task context: {ctx}")
            return None

    async def _process(self):
        async for taskctx in self._taskctx_channel:
            task = self._register_task(taskctx)
            logger.info(
                f'[TaskManager] registered {task.__class__.__name__} "{task.task_id}"'
            )
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
