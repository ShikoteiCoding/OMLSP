from dataclasses import dataclass

from typing import Any, TypeAlias, TypeVar, Callable, Optional

TaskOutput: TypeAlias = Any
TaskId = str
HasData = bool
T = TypeVar("T")


@dataclass
class TaskInfo:
    task: Any | None  # avoid circular type
    has_data: bool
    metadata_table: str
    metadata_column: str
    cleanup_callback: Optional[Callable] = None
