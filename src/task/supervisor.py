from __future__ import annotations

import trio
from loguru import logger
from typing import TYPE_CHECKING

from services import Service

if TYPE_CHECKING:
    from task.types import TaskId
    from task.task import BaseTask


class TaskSupervisor(Service):
    """
    Supervisor owned by TaskManager.
    """

    #: cancel scope per supervised task (controls supervision loop)
    _task_id_to_supervised_task: dict[TaskId, tuple[BaseTask, trio.CancelScope]]

    def __init__(self):
        super().__init__(name="TaskSupervisor")
        self._task_id_to_supervised_task: dict[
            TaskId, tuple[BaseTask, trio.CancelScope]
        ] = {}

    async def start_supervising(self, task: BaseTask):
        task_id = task.task_id
        # already supervising
        if task_id in self._task_id_to_supervised_task:
            logger.debug(f"[Supervisor] already supervising '{task_id}'")
            return

        logger.info(f"[Supervisor] start supervising '{task_id}'")
        # each supervised task runs under the TaskManager nursery
        self._nursery.start_soon(self._supervise_task, task)

    async def stop_supervising(self, task_id: str):
        task, scope = self._task_id_to_supervised_task.get(task_id, (None, None))
        if not task or not scope:
            logger.debug(f"[Supervisor] '{task_id}' not supervised")
            return

        logger.info(f"[Supervisor] stop supervising '{task_id}'")

        # Stop supervising first then stop task
        scope.cancel()
        await task.stop()

        # Clean resources
        del self._task_id_to_supervised_task[task_id]

    async def _supervise_task(self, task: BaseTask):
        task_id = task.task_id
        max_retries = 3
        attempt = 1
        backoff_base = 2.0

        with trio.CancelScope() as supervising_scope:
            self._task_id_to_supervised_task[task_id] = (task, supervising_scope)

            while True:
                try:
                    async with trio.open_nursery() as n:
                        task._nursery = n  # rebind nursery each cycle
                        await task.on_start()
                except trio.Cancelled:
                    # Normal cancellation -> evicted by user
                    logger.debug(f"[{task.task_id}] cancelled by supervisor")
                    break
                # When failing in nursery, it is always an exception group
                # trio high level + sub-exceptions
                except ExceptionGroup as e:
                    # Crash handling
                    if attempt > max_retries:
                        logger.error(f"[{task.task_id}] exceeded max retries: {e}")
                        break
                    logger.warning(
                        "[{}] crashed (attempt {}/{}). {}",
                        task.task_id,
                        attempt,
                        max_retries,
                        "\n".join(str(sub) for sub in list(e.exceptions)),
                    )
                    backoff = backoff_base * attempt
                    logger.debug(f"[{task.task_id}] retrying in {backoff:.1f}s")
                    await trio.sleep(backoff)
                    attempt += 1

        logger.info(f"[Supervisor] supervision ended for '{task_id}'")
