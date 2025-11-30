from typing import Any, TypeAlias, TypeVar
from enum import Enum

TaskOutput: TypeAlias = Any
TaskId = str
T = TypeVar("T")


class TaskManagerCommand(Enum):
    CREATE = "create"
    DELETE = "delete"
