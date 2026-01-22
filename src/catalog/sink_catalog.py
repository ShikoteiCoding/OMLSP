from dataclasses import dataclass
from typing import Protocol


class Properties(Protocol):
    pass


@dataclass
class SinkCatalog:
    name: str
    properties: Properties
    upstreams: list[str]
    sql: str
    definition: str
