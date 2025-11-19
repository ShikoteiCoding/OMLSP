from task.task import BaseTask


class TaskRegistry:
    """
    Registry of running tasks.
    Stores BaseTask objects by their task_id.
    """

    def __init__(self):
        self._tasks: dict[str, BaseTask] = {}

    def register(self, task: BaseTask) -> None:
        """Add a task to the registry."""
        self._tasks[task.task_id] = task

    def unregister(self, task: BaseTask) -> None:
        """Remove a task (e.g. intentionally stopped)."""
        self._tasks.pop(task.task_id, None)

    def get(self, task_id: str) -> BaseTask | None:
        """Retrieve a task by ID."""
        return self._tasks.get(task_id)

    def all(self) -> list[BaseTask]:
        """Return all currently registered tasks."""
        return list(self._tasks.values())

    def mark_dropped(self, task_id: str) -> None:
        """
        Soft-delete a task: mark it to prevent supervisor restarts.
        """
        task = self._tasks.get(task_id)
        if task:
            task.stopped = True
