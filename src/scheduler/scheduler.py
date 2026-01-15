from __future__ import annotations

import sys
import trio

from apscheduler.schedulers.base import BaseScheduler
from apscheduler.executors.base import BaseExecutor, run_coroutine_job, run_job
from apscheduler.util import iscoroutinefunction_partial
from loguru import logger

from services import Service

from scheduler.types import SchedulerCommand
from task.types import TaskId


class TrioExecutor(BaseExecutor):
    """
    Runs jobs using Trio's structured concurrency.

    - Coroutine jobs run directly inside Trio.
    - Regular synchronous jobs are offloaded to threads via trio.to_thread.run_sync.

    Plugin alias: ``trio``
    """

    def __init__(self):
        super().__init__()
        self._nursery: trio.Nursery | None = None
        self._pending_tasks: set[trio.CancelScope] = set()

    # ----------------------------------------------------
    # Lifecycle
    # ----------------------------------------------------

    def start(self, scheduler, alias):
        """
        Called when the executor is started.
        """
        super().start(scheduler, alias)
        if not hasattr(scheduler, "_nursery") or scheduler._nursery is None:
            raise RuntimeError(
                "TrioExecutor requires the scheduler to be configured with a Trio nursery"
            )
        self._nursery = scheduler._nursery

    def shutdown(self, wait=True):
        """Cancel all pending Trio jobs gracefully."""
        for cancel_scope in list(self._pending_tasks):
            cancel_scope.cancel()
        self._pending_tasks.clear()

        if wait:
            # Give Trio a chance to reach cancellation points
            try:
                trio.lowlevel.current_task()

                # We're inside Trio → yield once
                async def _wait():
                    await trio.sleep(0)

                trio.lowlevel.spawn_system_task(_wait)
            except RuntimeError:
                # Outside Trio context → ignore
                pass

    # ----------------------------------------------------
    # Core job submission
    # ----------------------------------------------------

    def _do_submit_job(self, job, run_times):
        """
        Submits a job to run inside Trio.
        """
        if not self._nursery:
            raise RuntimeError("TrioExecutor has no active nursery")

        # Choose coroutine or thread execution
        if iscoroutinefunction_partial(job.func):

            async def coro_runner():
                return await run_coroutine_job(
                    job, job._jobstore_alias, run_times, self._logger.name
                )
        else:

            async def coro_runner():
                return await trio.to_thread.run_sync(
                    run_job, job, job._jobstore_alias, run_times, self._logger.name
                )

        # Launch the job inside the nursery
        async def task_wrapper():
            cancel_scope = trio.CancelScope()
            self._pending_tasks.add(cancel_scope)
            try:
                with cancel_scope:
                    events = await coro_runner()
            except BaseException:
                # Capture the exception and notify scheduler
                self._run_job_error(job.id, *sys.exc_info()[1:])
            else:
                # Successful run
                self._run_job_success(job.id, events)
            finally:
                self._pending_tasks.discard(cancel_scope)

        self._nursery.start_soon(task_wrapper)


class TrioScheduler(Service, BaseScheduler):
    """
    A scheduler that runs using Trio's structured concurrency model.
    """

    #: Cancel scope for the wait timer
    _timer_cancel_scope: trio.CancelScope | None = None

    #: Trio token to keep track of threaded tasks
    _trio_token: trio.lowlevel.TrioToken | None

    #: Trio event for shutdown
    _is_shutting_down: bool

    def __init__(self, *args, **kwargs):
        Service.__init__(self, name="TrioScheduler")
        BaseScheduler.__init__(self, *args, **kwargs)
        self._trio_token = trio.lowlevel.current_trio_token()
        self._is_shutting_down = False

    async def on_receive(self, cmd: SchedulerCommand, payload) -> None:
        if cmd is SchedulerCommand.ADD:
            job_id, trigger, func = payload
            job = self.add_job(func=func, trigger=trigger, id=job_id)
            logger.info(f"[Scheduler] Added job {job.id}")
        # TODO EVICT should not be included in on_start
        elif cmd is SchedulerCommand.EVICT:
            job_id = payload
            existed = self.evict_job_by_id(job_id)  # type: ignore
            if existed:
                logger.info(f"[Scheduler] Evicted job {job_id}")
            else:
                logger.warning(f"[Scheduler] Evict failed — unknown job_id '{job_id}'")

    async def on_start(self):
        self._configure({"_nursery": self._nursery, "_trio_token": self._trio_token})
        BaseScheduler.start(self, paused=False)

    async def on_stop(self) -> None:
        """Gracefully stop scheduler timer and executors."""
        self._is_shutting_down = True
        self._stop_timer()

        # Stop executor tasks
        for executor in self._executors.values():
            if hasattr(executor, "shutdown"):
                executor.shutdown(wait=False)

        logger.success(f"[{self.name}] stopping.")

    async def on_shutdown(self) -> None:
        """Final cleanup before service exits."""
        self._is_shutting_down = True
        logger.success("[{}] shutdown completed.", self.name)

    @staticmethod
    def require_running(func):
        """
        Decorator to skip method execution when the scheduler is shutting down.

        This version only supports synchronous methods because TrioScheduler’s
        control methods are synchronous
        """

        def wrapper(self: TrioScheduler, *args, **kwargs):
            if self._is_shutting_down:
                logger.debug(f"[{self.name}] Ignoring {func.__name__} during shutdown.")
                return
            return func(self, *args, **kwargs)

        return wrapper

    @require_running
    def _start_timer(self, wait_seconds):
        self._stop_timer()

        if wait_seconds is None or not self._nursery:
            return

        async def timer_task():
            with trio.CancelScope() as scope:
                self._timer_cancel_scope = scope
                try:
                    logger.debug(
                        f"[Scheduler] Sleeping for {wait_seconds}s before wakeup..."
                    )
                    await trio.sleep(wait_seconds)
                    if not self._is_shutting_down:
                        await self._async_wakeup()
                except trio.Cancelled:
                    logger.debug("[Scheduler] Timer cancelled.")
                finally:
                    self._timer_cancel_scope = None

        self._nursery.start_soon(timer_task)

    async def _async_wakeup(self):
        self._stop_timer()
        wait_seconds = self._process_jobs()
        self._start_timer(wait_seconds)

    @require_running
    def wakeup(self):
        if not self._nursery:
            raise RuntimeError("TrioScheduler has no nursery configured")

        try:
            trio.lowlevel.current_task()
            inside_trio = True
        except RuntimeError:
            inside_trio = False

        if inside_trio:
            self._nursery.start_soon(self._async_wakeup)
        else:
            if not self._trio_token:
                raise RuntimeError("TrioScheduler has no Trio token configured")
            trio.from_thread.run(self._async_wakeup, trio_token=self._trio_token)

    def _stop_timer(self):
        """Cancels any currently running timer task."""

        if self._timer_cancel_scope:
            if not self._timer_cancel_scope.cancel_called:
                self._timer_cancel_scope.cancel()
            self._timer_cancel_scope = None

    def _create_default_executor(self):
        return TrioExecutor()

    def shutdown(self):
        """
        Minimal implementation to satisfy BaseScheduler abstract method.
        Do NOT call stop() here to avoid loops; the Service lifecycle handles it.
        """

    def evict_job_by_id(self, job_id: TaskId):
        """
        Immediately remove a job and cancel any pending executions.
        """
        job = self.get_job(job_id)
        if not job:
            return False

        # Remove from jobstore
        self.remove_job(job_id)

        logger.debug(f"[{self.name}] Evicted job {job_id}")
        return True


if __name__ == "__main__":

    async def main():
        async with trio.open_nursery() as nursery:
            token = trio.lowlevel.current_trio_token()

            # Create and configure the scheduler
            scheduler = TrioScheduler()
            scheduler._configure({"_nursery": nursery, "_trio_token": token})

            await scheduler.start(nursery)  # Calls on_start internally

            # Add a job to print "Tick" every 1 second
            scheduler.add_job(print, "interval", seconds=1, args=["Tick"])

            # Let the scheduler run for 4 seconds
            await trio.sleep(4)

            await scheduler.stop()  # Calls on_stop internally

    trio.run(main)
