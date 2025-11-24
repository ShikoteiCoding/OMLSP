from dataclasses import dataclass

from typing import Any, TypeAlias, TypeVar, Callable, Optional
from task.task import BaseTask

TaskOutput: TypeAlias = Any
TaskId = str
HasData = bool
T = TypeVar("T")


@dataclass
class TaskInfo:
    task: BaseTask | None
    has_data: bool
    metadata_table: str
    metadata_column: str
    cleanup_callback: Optional[Callable] = None
