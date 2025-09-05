import asyncio
import trio

from commons.utils import Channel
from context.context import TaskContext, SourceTaskContext, CreateLookupTableContext
from engine.engine import source_executable
from task.task import TaskId, TaskOutput, Task

from apscheduler import AsyncScheduler
from duckdb import DuckDBPyConnection


from loguru import logger

class TaskManager:
    conn: DuckDBPyConnection
    scheduler: AsyncScheduler

    _sources: dict[TaskId, Task] = {}
    _task_id_to_task: dict[TaskId, Task] = {}
    _nursery: trio.Nursery
    _taskctx_channel: Channel[TaskContext]

    def __init__(self, conn: DuckDBPyConnection, scheduler: AsyncScheduler):
        self.scheduler = scheduler

    async def add_taskctx_channel(self, channel: Channel[TaskContext]):
        self._taskctx_channel = channel

    def appoint(self, nursery: trio.Nursery):
        if hasattr(self, "nursery"):
            raise Exception(f"TaskManager has already been appointed.")
        self._nursery = nursery

    def register_task(self, ctx: TaskContext):
        task_id = ctx.name

        task = Task(
            task_id=task_id,
            receivers=[],
            sender=Channel(0),
        )

        if isinstance(ctx, CreateLookupTableContext):
            logger.warning("not handling CreateLookupTableContext in task manager yet")
            return

        if isinstance(ctx, SourceTaskContext):
            # TODO: should it be register to scheduler instead ?
            self._sources[task_id] = task.register(source_executable)
        
        else:
            for upstream in ctx.upstreams:
                upstream_task = self._task_id_to_task[upstream]
                task.subscribe(upstream_task.sender)
        
        self._task_id_to_task[task_id] = task

    async def process(self):
        async for taskctx in self._taskctx_channel:
            pass

    async def schedule(self):
        pass


    async def run(self):
        """Main loop for the TaskManager, runs forever."""
        self._running = True

        async with trio.open_nursery() as nursery:
            self._nursery = nursery

            async with self.scheduler as scheduler:
                await self.scheduler.start_in_background()
                nursery.start_soon(self.process)
                nursery.start_soon(self.schedule)

            print("[TaskManager] Started.")
            while self._running:
                await trio.sleep(1)

        print("[TaskManager] Stopped.")
        
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