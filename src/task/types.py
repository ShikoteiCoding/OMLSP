from typing import Any, TypeAlias, TypeVar
from enum import Enum

TaskOutput: TypeAlias = Any
TaskId = str
HasData = bool
T = TypeVar("T")


class SupervisorCommand(Enum):
    START = "start"
    STOP = "stop"
