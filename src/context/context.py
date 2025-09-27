import polars as pl

from dataclasses import dataclass, field
from typing import Any, Union, Type
from apscheduler.triggers.cron import CronTrigger
from duckdb.typing import DuckDBPyType

# --- Context definitions ---


# ---------- Table Contexts ----------
@dataclass
class CreateHTTPTableContext:
    name: str
    properties: dict[str, Any]
    query: str
    trigger: CronTrigger

    # executable of http table context
    # returns a polars dataframe
    _out_type: Type = field(default=pl.DataFrame)


@dataclass
class CreateWSTableContext:
    name: str
    properties: dict[str, Any]
    query: str

    # executable of ws table context
    # returns a polars dataframe
    _out_type: Type = field(default=pl.DataFrame)


@dataclass
class CreateHTTPLookupTableContext:
    name: str
    properties: dict[str, Any]
    query: str
    dynamic_columns: list[str]
    columns: dict[str, DuckDBPyType]


# ---------- View / Sink Contexts ----------
@dataclass
class CreateViewContext:
    name: str
    upstreams: list[str]
    columns: list[str]
    query: str


@dataclass
class CreateMaterializedViewContext:
    name: str
    upstreams: list[str]
    columns: list[str]
    query: str


@dataclass
class CreateSinkContext:
    name: str
    upstreams: list[str]
    properties: dict[str, Any]
    query: str


# ---------- Query / Command Contexts ----------
@dataclass
class SelectContext:
    columns: list[str]
    table: str
    alias: str
    where: str
    joins: dict[str, str]
    query: str


@dataclass
class SetContext:
    query: str


@dataclass
class CommandContext:
    query: str


@dataclass
class InvalidContext:
    reason: str


# ----------  Unions for type hints ----------
# Context part of task flow
TaskContext = Union[
    CreateHTTPLookupTableContext,
    CreateHTTPTableContext,
    CreateWSTableContext,
    CreateViewContext,
    CreateMaterializedViewContext,
    CreateSinkContext,
]

EvaluableContext = Union[
    CreateHTTPLookupTableContext,
    CreateHTTPTableContext,
    CreateWSTableContext,
    CreateSinkContext,
    CreateViewContext,
    CreateMaterializedViewContext,
    SetContext,
    CommandContext,
    SelectContext,
]

# Everything except Invalid
QueryContext = Union[
    CreateHTTPLookupTableContext,
    CreateHTTPTableContext,
    CreateWSTableContext,
    CreateSinkContext,
    CreateViewContext,
    SetContext,
    CommandContext,
    SelectContext,
]

# Table contexts of different connector type
CreateTableContext = Union[
    CreateHTTPLookupTableContext, CreateHTTPTableContext, CreateWSTableContext
]

ScheduledTaskContext = Union[CreateHTTPTableContext]
ContinousTaskContext = Union[CreateWSTableContext]

SinkTaskContext = Union[CreateSinkContext]
TransformTaskContext = Union[CreateMaterializedViewContext, CreateViewContext]
