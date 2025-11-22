import trio
from loguru import logger
from task.task import BaseTask
from channel import Channel
from services import Service

from task.task_catalog import TaskCatalog


class TaskSupervisor(Service):
    """
    Lightweight supervisor for tasks.
    Receives tasks to supervise from a Channel
    """

    _catalog: TaskCatalog
    _tasks_to_supervise: Channel[BaseTask]

    def __init__(self, catalog: TaskCatalog):
        super().__init__(name="TaskSupervisor")
        self._catalog: TaskCatalog = catalog

    def add_tasks_to_supervise_channel(self, channel: Channel[BaseTask]):
        self._tasks_to_supervise = channel

    async def on_start(self):
        self._nursery.start_soon(self._command_loop)

    async def _command_loop(self):
        async for task in self._tasks_to_supervise:
            self._start_supervising_task(task)

        logger.debug("[Supervisor] command channel closed")

    def _start_supervising_task(self, task: BaseTask):
        logger.info(f"[Supervisor] Supervising task '{task.task_id}'")
        # Start the supervision worker
        self._nursery.start_soon(self._supervise_task, task)

    async def _supervise_task(self, task: BaseTask):
        task_id = task.task_id
        max_retries = 3
        attempt = 1
        backoff_base = 2.0

        while self._catalog.has_task(task_id):
            try:
                async with trio.open_nursery() as n:
                    task._nursery = n  # rebind nursery each cycle
                    await task.on_start()
            except trio.Cancelled:
                # Normal cancellation -> evicted by user
                logger.debug(f"[{task.task_id}] cancelled by supervisor")
                break
            except Exception as e:
                # Crash handling
                if attempt > max_retries:
                    logger.error(f"[{task.task_id}] exceeded max retries: {e}")
                    break
                logger.warning(
                    f"[{task.task_id}] crashed (attempt {attempt}/{max_retries}): {e}"
                )
                backoff = backoff_base * attempt
                logger.debug(f"[{task.task_id}] retrying in {backoff:.1f}s")
                await trio.sleep(backoff)
                attempt += 1
            finally:
                await task.on_stop()
