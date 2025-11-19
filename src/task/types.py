from typing import Any, TypeAlias, TypeVar

TaskOutput: TypeAlias = Any
TaskId = str
HasData = bool
T = TypeVar("T")


class TaskCancelled(Exception):
    """Raised when a task is intentionally stopped or dropped."""

    pass
