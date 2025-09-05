from typing import Any, Callable, TypeAlias

from commons.utils import Channel

TaskOutput: TypeAlias = Any
TaskId = int | str

class Task:
    def __init__(
        self,
        task_id: TaskId,
        executable: Callable[..., TaskOutput],
        receivers: list[Channel],
        sender: Channel,
    ):
        self.task_id = task_id
        self.executable = executable
        self._receivers = receivers
        self._sender = sender

    @property
    def sender(self):
        return self._sender

    def subscribe(self, recv: Channel):
        self._receivers.append(recv)

    def merge(self):
        pass

    async def run(self):
        inputs = [await ch.recv() for ch in self._receivers]
        result = await self.executable(self.task_id, *inputs)

        await self._sender.send(result)
