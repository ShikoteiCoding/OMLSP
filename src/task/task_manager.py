import trio
import trio_asyncio


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
from metadata import (
    create_table
)
from sink.sink import run_kafka_sink
from task.task import TaskId, Task

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from duckdb import DuckDBPyConnection


from loguru import logger


class TaskManager:
    conn: DuckDBPyConnection
    scheduler: AsyncIOScheduler

    _sources: dict[TaskId, Task] = {}
    _task_id_to_task: dict[TaskId, Task] = {}
    _nursery: trio.Nursery
    _taskctx_channel: Channel[TaskContext]

    def __init__(self, conn: DuckDBPyConnection, scheduler: AsyncIOScheduler):
        self.conn = conn
        self.scheduler = scheduler

    async def add_taskctx_channel(self, channel: Channel[TaskContext]):
        self._taskctx_channel = channel

    async def run(self):
        """Main loop for the TaskManager, runs forever."""
        self._running = True

        async with trio.open_nursery() as nursery:
            async with trio_asyncio.open_loop() as loop:  # type: ignore
                # TODO: create our own scheduler class to wrap event loop logic
                # Or create our own event loop class
                self.scheduler._eventloop = loop
                self.scheduler.start()

                self._nursery = nursery

                nursery.start_soon(self._process)
                nursery.start_soon(self._watch_for_shutdown)

                logger.info("[TaskManager] Started.")
                while self._running:
                    await trio.sleep(1)

        logger.info("[TaskManager] Stopped.")

    async def _register_one_task(self, ctx: TaskContext):
        task_id = ctx.name

        task = Task(task_id=task_id, conn=self.conn)

        # TODO: build according properties
        if isinstance(ctx, CreateSinkContext):
            props = ctx.properties
            self._nursery.start_soon(
                run_kafka_sink,
                self.conn,
                ctx.upstreams,
                props["topic"],
                props["server"],
                ctx.name,
            )

        elif isinstance(ctx, CreateLookupTableContext):
            create_table(self.conn, ctx)
            build_lookup_table_prehook(ctx, self.conn)
            logger.info(f'[TaskManager] registered lookup task "{task_id}"')
            return

        elif isinstance(ctx, SourceTaskContext):  # register to scheduler
            # Executable could be attached to context
            # But we might want it dynamic later (i.e built at run time)
            self._sources[task_id] = task.register(build_source_executable(ctx))
            _ = self.scheduler.add_job(func=task.run, trigger=ctx.trigger)
            logger.info(f'[TaskManager] registered source task "{task_id}"')

        else:
            for upstream in ctx.upstreams:
                upstream_task = self._task_id_to_task[upstream]
                task.subscribe(upstream_task.get_sender())

        self._task_id_to_task[task_id] = task

    async def _process(self):
        async for taskctx in self._taskctx_channel:
            await self._register_one_task(taskctx)

    async def _run_task(self, task: Task):
        task._running = True
        logger.info(f"[TaskManager] Task '{task.task_id}' started.")

        try:
            await task.run()
        except Exception as e:
            logger.info(f"[TaskManager] Task '{task.task_id}' failed: {e}")
        else:
            logger.info(f"[TaskManager] Task '{task.task_id}' completed successfully.")
        finally:
            task._running = False

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
