import asyncio

from typing import Any, Callable, TypeAlias
from sqlparser.dag import DataFlowDAG

TaskId = int | str


class Channel:
    # TODO: improve method typing

    def __init__(self):
        self.queue = asyncio.Queue()

    async def send(self, data):
        await self.queue.put(data)

    async def recv(self):
        return await self.queue.get()


TaskOutput: TypeAlias = Any


class Task:
    def __init__(
        self,
        task_id: TaskId,
        executable: Callable[..., TaskOutput],
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


class TaskManager:
    def __init__(
        self, dag: DataFlowDAG, executables: dict[TaskId, Callable[..., TaskOutput]]
    ):
        self.dag = dag
        self.executables = executables
        self.tasks: dict[TaskId, Task] = {}

    def build(self):
        # Create channels for every edge
        channels: dict[tuple[TaskId, TaskId], Channel] = {}

        for src, dests in self.dag._adj_list.items():
            for dest in dests:
                channels[(src.name, dest.name)] = Channel()

        # Build tasks
        for node, neighbors in self.dag._adj_list.items():
            task_id = node.name
            recv_chs = [
                channels[(src.name, node.name)]
                for src, dests in self.dag._adj_list.items()
                if node in dests
            ]
            send_chs = [channels[(node.name, dest.name)] for dest in neighbors]

            task = Task(
                task_id=task_id,
                executable=self.executables[task_id],
                receivers=recv_chs,
                senders=send_chs,
            )
            self.tasks[task_id] = task

    async def run(self, entry_points: dict[TaskId, TaskOutput]):
        """Run DAG: seed entrypoint tasks, then run everything concurrently."""
        # seed entrypoints (sources) with their initial data
        for task_id, data in entry_points.items():
            for ch in self.tasks[task_id].senders:
                await ch.send(data)

        # run all tasks concurrently
        await asyncio.gather(*(t.run() for t in self.tasks.values()))


if __name__ == "__main__":

    async def fetch_service_a(task_id) -> TaskOutput:
        await asyncio.sleep(1)
        data = {"a": 1}
        print(f"[{task_id}]: {data}")
        return data

    async def fetch_service_b(task_id) -> TaskOutput:
        await asyncio.sleep(1)
        data = {"b": 2}
        print(f"[{task_id}]: {data}")
        return data

    async def join_data(task_id, a, b) -> TaskOutput:
        await asyncio.sleep(1)
        data = {**a, **b}
        print(f"[{task_id}]: {data}")
        return data

    async def write_to_kafka(task_id, c) -> TaskOutput:
        print(f"[{task_id}]: saving kafka {c}")
        return None

    async def write_to_db(task_id, d) -> TaskOutput:
        print(f"[{task_id}]: saving db {d}")
        return None

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
