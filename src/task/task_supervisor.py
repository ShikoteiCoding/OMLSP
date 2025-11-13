import trio
from loguru import logger
from task.task import BaseTask


class TaskSupervisor:
    """Lightweight supervisor to restart failed tasks"""

    def __init__(self, nursery: trio.Nursery):
        self.nursery = nursery

    async def supervise(self, task: BaseTask):
        max_retries = 3
        backoff_base = 2.0
        attempt = 1

        while True:
            if getattr(task, "stopped", False):
                logger.info(f"[{task.task_id}] dropped intentionally — not restarting")
                break
            try:
                async with trio.open_nursery() as n:
                    task._nursery = n  # rebind nursery each cycle
                    await task.on_start()

            except trio.Cancelled:
                logger.debug(f"[{task.task_id}] cancelled by supervisor")
                break

            except Exception as e:
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
