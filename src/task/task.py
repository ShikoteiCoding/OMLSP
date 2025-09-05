from typing import Any, Callable, TypeAlias

from commons.utils import Channel

TaskOutput: TypeAlias = Any
TaskId = int | str

class Task:
    _running = False

    def __init__(
        self,
        task_id: TaskId,
        receivers: list[Channel],
        sender: Channel,
    ):
        self.task_id = task_id
        
        self._receivers = receivers
        self._sender = sender
        self._executable = None

    @property
    def sender(self):
        return self._sender
    
    def register(self, executable: Callable):
        self.executable = executable
        return self

    def subscribe(self, recv: Channel):
        self._receivers.append(recv)

    def merge(self):
        pass

    async def run(self):
        inputs = [await ch.recv() for ch in self._receivers]
        result = await self.executable(self.task_id, *inputs)

        await self._sender.send(result)

    def __repr__(self):
        return f"Node({self.task_id!r})"
