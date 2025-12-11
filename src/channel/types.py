from typing import Any
from dataclasses import dataclass

type Address = str
type Message = Any
type Response = Any
type DeliveryOptions = str

@dataclass
class ValidResponse:
    data: Any

@dataclass
class InvalidResponse:
    reason: str