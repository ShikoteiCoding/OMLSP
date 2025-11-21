import trio
from loguru import logger
from task.task import BaseTask
from channel import Channel
from services import Service

from task.types import SupervisorCommand


class TaskSupervisor(Service):
    """
    Lightweight supervisor for tasks.
    Receives (cmd, payload) from a Channel
    """

    _active: dict[str, BaseTask]
    _tasks_to_supervise: Channel[tuple[SupervisorCommand, BaseTask]]

    def __init__(self):
        super().__init__(name="TaskSupervisor")
        # Map of active supervised tasks: task_id → BaseTask instance
        self._active: dict[str, BaseTask] = {}

    def add_tasks_to_supervise_channel(
        self, channel: Channel[tuple[SupervisorCommand, BaseTask]]
    ):
        self._tasks_to_supervise = channel

    async def on_start(self):
        self._nursery.start_soon(self._command_loop)

    async def _command_loop(self):
        async for cmd, payload in self._tasks_to_supervise:
            task: BaseTask = payload
            match cmd:
                case SupervisorCommand.START:
                    self._start_supervising_task(task)

                case SupervisorCommand.STOP:
                    self._stop_supervising_task(task)

        logger.info("[Supervisor] command channel closed")

    def _start_supervising_task(self, task: BaseTask):
        if task.task_id in self._active:
            logger.warning(f"[Supervisor] Task '{task.task_id}' already supervised")
            return

        self._active[task.task_id] = task

        logger.info(f"[Supervisor] Supervising task '{task.task_id}'")

        # Start the supervision worker
        self._nursery.start_soon(self._supervise_task, task)

    def _stop_supervising_task(self, task: BaseTask):
        self._active.pop(task.task_id, None)

    async def _supervise_task(self, task: BaseTask):
        task_id = task.task_id
        max_retries = 3
        attempt = 1
        backoff_base = 2.0

        while task_id in self._active:
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
        logger.info(f"[{task.task_id}] supervisor stopped")
