import asyncio

from typing import Any, Callable, TypeAlias
from sql.sqlparser.dag import DataFlowDAG
from commons.utils import Channel
from task.task import TaskId, TaskOutput, Task
from context.context import TaskContext, SourceTaskContext


class TaskManager:
    sources: dict[TaskId, Task]
    task_id_to_task: dict[TaskId, Task]

    def append(self, ctx: TaskContext):
        task_id = ctx.name

        task = Task(
            task_id=task_id,
            executable=lambda x: x, # TODO: plug executable here
            receivers=[],
            sender=Channel(0),
        )

        if isinstance(ctx, SourceTaskContext):
            self.sources[task_id] = task
        
        else:
            for upstream in ctx.upstreams:
                upstream_task = self.task_id_to_task[upstream]
                task.subscribe(upstream_task.sender)
        
        self.task_id_to_task[task_id] = task


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
