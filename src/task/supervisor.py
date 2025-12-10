import trio
from loguru import logger
from task.task import BaseTask
from services import Service


class TaskSupervisor(Service):
    """
    Supervisor owned by TaskManager.
    """

    def __init__(self):
        super().__init__(name="TaskSupervisor")
        # cancel scope per supervised task (controls supervision loop)
        self._supervised: dict[str, trio.CancelScope] = {}

    async def start_supervising(self, task: BaseTask):
        task_id = task.task_id
        # already supervising
        if task_id in self._supervised:
            logger.debug(f"[Supervisor] already supervising '{task_id}'")
            return

        logger.info(f"[Supervisor] start supervising '{task_id}'")
        # each supervised task runs under the TaskManager nursery
        self._nursery.start_soon(self._supervise_task, task)

    async def stop_supervising(self, task_id: str):
        scope = self._supervised.get(task_id)
        if not scope:
            logger.debug(f"[Supervisor] '{task_id}' not supervised")
            return

        logger.info(f"[Supervisor] stop supervising '{task_id}'")

        scope.cancel()
        del self._supervised[task_id]

    async def _supervise_task(self, task: BaseTask):
        task_id = task.task_id
        max_retries = 3
        attempt = 1
        backoff_base = 2.0

        with trio.CancelScope() as scope:
            self._supervised[task_id] = scope

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
