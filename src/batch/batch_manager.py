import asyncio

from typing import Callable

TaskId = int | str


class Channel:
    # TODO: improve method typing

    def __init__(self):
        self.queue = asyncio.Queue()

    async def send(self, data):
        await self.queue.put(data)

    async def recv(self):
        return await self.queue.get()


class TaskOutput:
    pass


class Task:
    def __init__(
        self,
        task_id: TaskId,
        executable: Callable,
        receivers: list[Channel],
        senders: list[Channel],
    ):
        self.task_id = task_id
        self.executable = executable
        self.receivers = receivers
        self.senders = senders

    async def run(self):
        inputs = [await ch.recv() for ch in self.receivers]
        result = await self.executable(self.task_id, *inputs)

        for ch in self.senders:
            await ch.send(result)


class BatchManager:
    tasks: dict[TaskId, Task]


if __name__ == "__main__":
    async def fetch_service_a(task_id):
        await asyncio.sleep(1)
        data = {"a": 1}
        print(f"[{task_id}]: {data}")
        return data
    async def fetch_service_b(task_id):
        await asyncio.sleep(1)
        data = {"b": 2}
        print(f"[{task_id}]: {data}")
        return data
    async def join_data(task_id, a, b):
        await asyncio.sleep(1)
        data = {**a, **b}
        print(f"[{task_id}]: {data}")
        return data
    async def write_to_kafka(task_id, c):
        print(f"[{task_id}]: saving kafka {c}")
        return
    async def write_to_db(task_id, d):
        print(f"[{task_id}]: saving db {d}")
        return
    chan_a = Channel()
    chan_b = Channel()
    chan_join_out = Channel()
    chan_save_out = Channel()
    taska = Task("fetchA", fetch_service_a, [], [chan_a])
    taskb = Task("fetchB", fetch_service_b, [], [chan_b])
    taskjoin = Task("join", join_data, [chan_a, chan_b], [chan_join_out, chan_save_out])
    taskkafka = Task("kafka", write_to_kafka, [chan_join_out], [])
    taskdb = Task("db", write_to_db, [chan_save_out], [])

    async def run_tasks(tasks):
        await asyncio.gather(*(task.run() for task in tasks))

    asyncio.run(run_tasks([taska, taskb, taskjoin, taskkafka, taskdb]))
    
