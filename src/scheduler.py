import sys
import trio

from apscheduler.schedulers.base import BaseScheduler
from apscheduler.util import maybe_ref


from apscheduler.executors.base import BaseExecutor, run_coroutine_job, run_job
from apscheduler.util import iscoroutinefunction_partial

from loguru import logger


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
        """
        Cancel all pending Trio jobs.
        """
        for cancel_scope in self._pending_tasks:
            cancel_scope.cancel()
        self._pending_tasks.clear()

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


class TrioScheduler(BaseScheduler):
    """
    A scheduler that runs using Trio's structured concurrency model.
    """

    _nursery: trio.Nursery | None = None
    _timer_cancel_scope: trio.CancelScope | None = None

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._trio_token: trio.lowlevel.TrioToken | None = None

    def start(self, paused: bool = False):
        if not self._nursery or not self._trio_token:
            raise RuntimeError(
                "TrioScheduler.start() must be configured with a nursery and token. "
                "Use scheduler._configure({'_nursery': nursery, '_trio_token': token})."
            )
        super().start(paused)

    async def _async_shutdown(self, wait: bool = True):
        super().shutdown(wait)
        self._stop_timer()

    def shutdown(self, wait: bool = True):
        """
        Sync shutdown wrapper: schedules async shutdown inside Trio.

        Can be safely called:
        - From outside Trio (other threads) using trio.from_thread.run().
        - From inside Trio by directly scheduling the async task.
        """
        if not self._nursery:
            raise RuntimeError("TrioScheduler has no nursery configured")

        try:
            # Detect if we're inside Trio
            trio.lowlevel.current_task()
            inside_trio = True
        except RuntimeError:
            inside_trio = False

        if inside_trio:
            # Already running in Trio, schedule shutdown directly
            self._nursery.start_soon(self._async_shutdown, wait)
        else:
            # External thread calling shutdown
            if not self._trio_token:
                raise RuntimeError("TrioScheduler has no Trio token configured")
            trio.from_thread.run(
                self._async_shutdown, wait, trio_token=self._trio_token
            )

    def _configure(self, config):
        self._nursery = maybe_ref(config.pop("_nursery", None))
        self._trio_token = maybe_ref(config.pop("_trio_token", None))
        super()._configure(config)

    def _start_timer(self, wait_seconds):
        logger.debug(f"[Scheduler] Starting timer with wait_seconds={wait_seconds}")
        self._stop_timer()

        if wait_seconds is None:
            return

        if not self._nursery:
            return

        async def timer_task():
            logger.debug(f"[Scheduler] Sleeping for {wait_seconds}s before wakeup...")
            try:
                await trio.sleep(wait_seconds)
                await self._async_wakeup()
            except trio.Cancelled:
                pass

        # Launch the timer as a background task
        self._timer_cancel_scope = (
            self._nursery.cancel_scope.__class__()
        )  # separate cancel scope
        self._nursery.start_soon(timer_task)

    async def _async_wakeup(self):
        self._stop_timer()
        wait_seconds = self._process_jobs()
        self._start_timer(wait_seconds)

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
        """
        Cancels any currently running timer task.
        """
        if self._timer_cancel_scope:
            self._timer_cancel_scope.cancel()
            self._timer_cancel_scope = None

    def _create_default_executor(self):
        return TrioExecutor()


if __name__ == "__main__":

    async def main():
        async with trio.open_nursery() as nursery:
            token = trio.lowlevel.current_trio_token()
            scheduler = TrioScheduler()
            scheduler._configure({"_nursery": nursery, "_trio_token": token})

            scheduler.start()

            scheduler.add_job(print, "interval", seconds=1, args=["Tick"])
            await trio.sleep(4)

            # Can be called from inside Trio
            scheduler.shutdown()

    trio.run(main)
