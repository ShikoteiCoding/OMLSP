from dataclasses import dataclass


@dataclass
class ViewCatalog:
    name: str
    upstreams: list[str]
    sql: str
    definition: str
    materialized: bool
