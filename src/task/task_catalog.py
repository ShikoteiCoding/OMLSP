from task.task import BaseTask
from task.types import TaskId, HasMetadata


class TaskCatalog:
    """
    Catalog of running tasks.
    Stores BaseTask objects and HasMetadata bool by their task_id.
    """

    _task_id_to_task: dict[TaskId, tuple[BaseTask, HasMetadata]] = {}

    def __init__(self):
        self._task_id_to_task: dict[TaskId, tuple[BaseTask, HasMetadata]] = {}

    def add(self, task: BaseTask, has_metadata: HasMetadata) -> None:
        """Add a task to the catalog."""
        self._task_id_to_task[task.task_id] = task, has_metadata

    def remove(self, task: BaseTask) -> None:
        """Remove a task (e.g. intentionally stopped)."""
        self._task_id_to_task.pop(task.task_id, None)

    def get(self, task_id: str) -> tuple[BaseTask | None, HasMetadata]:
        """Retrieve a task by ID."""
        return self._task_id_to_task.get(task_id, (None, False))
