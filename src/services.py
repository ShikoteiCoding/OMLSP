"""
Service base instantiable class.
"""

from __future__ import annotations

import trio

from loguru import logger


class Service:
    """
    Uses Trio Events and Nurseries for lifecycle management.

    Kudos: https://github.com/ask/mode/tree/master
    """

    #: Default timeout for graceful shutdown after stop()
    shutdown_timeout = 1.0

    #: Main nursery to spawn childs
    _nursery: trio.Nursery

    #: Waitable event for start loop
    _started: trio.Event

    #: Waitable event for stop loop
    _stopped: trio.Event

    #: Waitable event for shutdown loop
    _shutdown: trio.Event

    #: List of chiled services
    _dependencies: list[Service]

    def __init__(self, name: str) -> None:
        self._started = trio.Event()
        self._stopped = trio.Event()
        self._shutdown = trio.Event()
        self._dependencies = []

        self.name = name

        # TODO: Not implemented yet
        self._polling_started = False
        self._pollers = []

    async def on_start(self) -> None:
        """Called right after service is started, before pollers run."""
        logger.success("[{}] starting.", self.name)

    async def on_stop(self) -> None:
        """Called when the service is asked to stop but before shutdown is complete."""
        logger.success("[{}] stopping.", self.name)

    async def on_shutdown(self) -> None:
        """Called when the service has fully shut down."""
        logger.success("[{}] shutdown.", self.name)

    async def start(self, nursery: trio.Nursery) -> None:
        """
        Starts the service and attaches it to the provided nursery.
        """
        self._nursery = nursery
        await self.on_start()
        self._started.set()

        # start child dependencies
        for dep in self._dependencies:
            nursery.start_soon(dep.start, nursery)

        logger.success("[{}] started.", self.name)

    async def stop(self) -> None:
        """
        Initiates stopping of the service.
        """
        self._stopped.set()
        await self.on_stop()

        # Stop dependencies in reverse (provided it matters)
        for dep in reversed(self._dependencies):
            await dep.stop()

        if self._polling_started:
            with trio.move_on_after(self.shutdown_timeout) as cancel_scope:
                await self._shutdown.wait()

            if cancel_scope.cancelled_caught:
                logger.warning(
                    f"Operation timed out after {self.shutdown_timeout} seconds."
                )

        await self.on_shutdown()

    async def wait_until_stopped(self) -> None:
        """Wait until the service is signalled to stop."""
        await self._stopped.wait()

    def add_dependency(self, service: Service) -> None:
        """
        Register another service as a dependency of this one.
        The parent service will manager start/stop/shutdown/restarts.
        """
        self._dependencies.append(service)

    # XXX: Not useful for now but might solve later the hang task problems
    # we will face when we trigger change upon events
    # Example:
    # we do Window agregations, but if no new event then we need
    # to close the window, this is easier to achieve with a self poller

    # def add_poller(self, callback: Callable[[], None]) -> None:
    #     """
    #     Adds a polling callback to run repeatedly in the background.
    #     """
    #     self._pollers.append(callback)
    #     if not self._polling_started:
    #         self._polling_started = True
    #         assert self._nursery, "Service must be started before adding pollers"
    #         self._nursery.start_soon(self._polling_loop)

    # async def _polling_loop(self) -> None:
    #     # Runs all poller callbacks repeatedly until stopped.
    #     try:
    #         while not self._stopped.is_set():
    #             for poller in self._pollers:
    #                 await poller()

    #             await trio.lowlevel.checkpoint()
    #     finally:
    #         # Signal that the polling loop is done
    #         self._shutdown.set()

    @property
    def state(self) -> str:
        if not self._started.is_set():
            return "init"
        if not self._stopped.is_set():
            return "running"
        if not self._shutdown.is_set():
            return "stopping"
        return "shutdown"

    def __repr__(self) -> str:
        return f"<{type(self).__name__}: {self.state}>"
