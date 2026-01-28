from dataclasses import dataclass
from typing import Protocol


class Properties(Protocol):
    pass


@dataclass
class SourceCatalog:
    name: str
    properties: Properties
    sql: str
    definition: str
