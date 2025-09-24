from dataclasses import dataclass
from typing import Any, Union
from apscheduler.triggers.cron import CronTrigger

# ---------- Table Contexts ----------
@dataclass
class CreateTableContext:
    name: str
    properties: dict[str, Any]
    query: str
    trigger: CronTrigger


@dataclass
class CreateLookupTableContext:
    name: str
    properties: dict[str, Any]
    query: str
    dynamic_columns: list[str]
    columns: dict[str, str]


# ---------- View / Sink Contexts ----------
@dataclass
class CreateViewContext:
    name: str
    upstreams: list[str]
    query: str


@dataclass
class CreateMaterializedViewContext:
    name: str
    upstreams: list[str]


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
    CreateLookupTableContext,
    CreateTableContext,
    CreateViewContext,
    CreateMaterializedViewContext,
    CreateSinkContext,
]

EvaluableContext = Union[
    CreateLookupTableContext,
    CreateTableContext,
    CreateSinkContext,
    CreateViewContext,
    SetContext,
    CommandContext,
    SelectContext,
]

# Everything except Invalid
QueryContext = Union[
    CreateLookupTableContext,
    CreateTableContext,
    CreateSinkContext,
    CreateViewContext,
    SetContext,
    CommandContext,
    SelectContext,
]

# Sub type of TaskContext
SourceTaskContext = Union[CreateTableContext]

SinkTaskContext = Union[CreateSinkContext]
