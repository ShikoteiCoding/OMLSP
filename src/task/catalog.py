from task.task import BaseTask
from task.types import TaskId, HasData
from context.context import CreateContext
from metadata.db import (
    METADATA_TABLE_TABLE_NAME,
    METADATA_VIEW_TABLE_NAME,
    METADATA_SINK_TABLE_NAME,
    METADATA_SOURCE_TABLE_NAME,
)
from context.context import (
    CreateSinkContext,
    CreateHTTPSourceContext,
    CreateHTTPTableContext,
    CreateWSSourceContext,
    CreateWSTableContext,
    CreateViewContext,
)


class TaskCatalog:
    """
    Catalog of running tasks.
    Stores BaseTask objects and HasData bool by their task_id.
    """

    #: Flag to detect init
    _instanciated: bool = False

    #: Singleton instance
    _instance: TaskCatalog

    CTX_METADATA_MAP: dict[type, tuple[str, str]] = {
        CreateHTTPTableContext: (METADATA_TABLE_TABLE_NAME, "table_name"),
        CreateWSTableContext: (METADATA_TABLE_TABLE_NAME, "table_name"),
        CreateViewContext: (METADATA_VIEW_TABLE_NAME, "view_name"),
        CreateSinkContext: (METADATA_SINK_TABLE_NAME, "sink_name"),
        CreateHTTPSourceContext: (METADATA_SOURCE_TABLE_NAME, "source_name"),
        CreateWSSourceContext: (METADATA_SOURCE_TABLE_NAME, "source_name"),
    }

    _task_id_to_task: dict[TaskId, tuple[BaseTask, HasData, str, str]]

    def __init__(self):
        raise NotImplementedError("Singleton — use TaskCatalog.get_instance()")

    def init(self):
        self._task_id_to_task: dict[TaskId, tuple[BaseTask, HasData, str, str]] = {}

    @classmethod
    def get_instance(cls) -> TaskCatalog:
        if cls._instanciated:
            return cls._instance

        cls._instance = cls.__new__(cls)
        cls._instance.init()
        cls._instanciated = True

        return cls._instance

    def resolve_metadata(self, ctx_type: type) -> tuple[str, str]:
        return self.CTX_METADATA_MAP.get(
            ctx_type,
            (METADATA_TABLE_TABLE_NAME, "table_name"),  # default
        )

    def add(
        self, task: BaseTask, has_metadata: HasData, ctx_type: type[CreateContext]
    ) -> None:
        """Add a task to the catalog."""
        metadata_table, metadata_column = self.resolve_metadata(ctx_type)
        self._task_id_to_task[task.task_id] = (
            task,
            has_metadata,
            metadata_table,
            metadata_column,
        )

    def remove(self, task: BaseTask) -> None:
        """Remove a task (e.g. intentionally stopped)."""
        self._task_id_to_task.pop(task.task_id, None)

    def get(self, task_id: str) -> tuple[BaseTask | None, HasData, str, str]:
        """Retrieve a task by ID."""
        return self._task_id_to_task.get(task_id, (None, False, "", ""))

    def has_task(self, task_id: str) -> bool:
        """Check if the task is in the catalog."""
        return task_id in self._task_id_to_task

task_catalog = TaskCatalog.get_instance()