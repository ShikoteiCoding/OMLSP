from dataclasses import dataclass
from typing import Protocol


class Properties(Protocol):
    pass


@dataclass
class TableCatalog:
    name: str
    properties: Properties
    sql: str
    definition: str
    lookup: bool
