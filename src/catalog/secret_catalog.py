from dataclasses import dataclass
from typing import Any


@dataclass
class SecretCatalog:
    name: str
    properties: dict[str, Any]
    value: str
    sql: str
    definition: str
