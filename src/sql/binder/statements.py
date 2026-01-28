from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Literal, TYPE_CHECKING

if TYPE_CHECKING:
    from sqlglot import exp

__all__ = [
    "BoundStatement",
    "BoundDDL",
    "BoundDML",
    "BoundCreateTable",
    "BoundDropTable",
    "BoundCreateSource",
    "BoundDropSource",
    "BoundCreateView",
    "BoundDropView",
    "BoundCreateSink",
    "BoundDropSink",
    "BoundCreateSecret",
    "BoundDropSecret",
    "BoundSelect",
    "BoundSet",
    "BoundShow",
    "BoundCommand",
]


@dataclass
class BoundStatement:
    #: Original SQL string
    raw_sql: str


@dataclass
class BoundDDL(BoundStatement):
    #: Entity name
    name: str


@dataclass
class BoundDML(BoundStatement):
    pass


@dataclass
class BoundCreateTable(BoundDDL):
    #: Column name -> type mapping
    column_types: dict[str, str]
    #: Validated properties from WITH clause
    properties: dict[str, Any]
    #: Generated column expressions (AST nodes, not compiled)
    generated_column_exprs: list[exp.Expression] = field(default_factory=list)
    #: Lookup fields for $-prefixed columns
    lookup_fields: list[str] = field(default_factory=list)
    #: Transport type
    transport_type: Literal["http", "ws"] = "http"
    #: Whether this is a lookup table
    is_lookup: bool = False
    #: DuckDB-compatible SQL for catalog storage
    duckdb_sql: str = ""


@dataclass
class BoundDropTable(BoundDDL):
    #: Whether to cascade drop dependent objects
    cascade: bool = False


@dataclass
class BoundCreateSource(BoundDDL):
    #: Column name -> type mapping
    column_types: dict[str, str]
    #: Validated properties from WITH clause
    properties: dict[str, Any]
    #: Transport type
    transport_type: Literal["http", "ws"] = "http"
    #: DuckDB-compatible SQL for catalog storage
    duckdb_sql: str = ""


@dataclass
class BoundDropSource(BoundDDL):
    #: Whether to cascade drop dependent objects
    cascade: bool = False


@dataclass
class BoundCreateView(BoundDDL):
    #: List of upstream table/view/source names
    upstreams: list[str] = field(default_factory=list)
    #: Transform SQL (DuckDB-compatible SELECT)
    transform_sql: str = ""
    #: Whether the view is materialized
    materialized: bool = False
    #: DuckDB-compatible SQL for catalog storage
    duckdb_sql: str = ""


@dataclass
class BoundDropView(BoundDDL):
    #: Whether to cascade drop dependent objects
    cascade: bool = False


@dataclass
class BoundCreateSink(BoundDDL):
    #: List of upstream table/view/source names
    upstreams: list[str] = field(default_factory=list)
    #: Transform SQL (DuckDB-compatible SELECT)
    transform_sql: str = ""
    #: Validated properties from WITH clause
    properties: dict[str, Any] = field(default_factory=dict)
    #: DuckDB-compatible SQL for catalog storage
    duckdb_sql: str = ""


@dataclass
class BoundDropSink(BoundDDL):
    #: Whether to cascade drop dependent objects
    cascade: bool = False


@dataclass
class BoundCreateSecret(BoundDDL):
    #: Secret type (e.g., "bearer", "basic")
    secret_type: str = ""
    #: Secret value
    secret_value: str = ""


@dataclass
class BoundDropSecret(BoundDDL):
    #: Whether to cascade drop dependent objects
    cascade: bool = False


@dataclass
class BoundSelect(BoundDML):
    #: Selected column names
    columns: list[str] = field(default_factory=list)
    #: Table names in FROM clause
    from_tables: list[str] = field(default_factory=list)
    #: Join information: table -> join type
    joins: dict[str, str] = field(default_factory=dict)
    #: WHERE clause as string (if any)
    where: str | None = None
    #: DuckDB-compatible SQL
    duckdb_sql: str = ""


@dataclass
class BoundSet(BoundDML):
    #: Configuration key
    key: str = ""
    #: Configuration value
    value: str = ""


@dataclass
class BoundShow(BoundDML):
    #: Target to show (e.g., "tables", "views", "sources")
    target: str = ""


@dataclass
class BoundCommand(BoundDML):
    #: Command type
    command: str = ""
    #: Target entity name (if applicable)
    target: str | None = None
