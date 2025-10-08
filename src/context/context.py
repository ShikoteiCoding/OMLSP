from __future__ import annotations

import polars as pl

from dataclasses import dataclass, field
from typing import Any, Union, Type
from apscheduler.triggers.cron import CronTrigger

# --- Context definitions ---


# ---------- Table Contexts ----------
@dataclass
class CreateHTTPTableContext:
    name: str
    properties: dict[str, Any]
    query: str
    column_types: dict[str, str]
    trigger: CronTrigger
    lookup: bool = False

    # executable of http table context
    # returns a polars dataframe
    _out_type: Type = field(default=pl.DataFrame)


@dataclass
class CreateWSTableContext:
    name: str
    properties: dict[str, Any]
    column_types: dict[str, str]
    query: str
    on_start_query: str
    lookup: bool = False

    # executable of ws table context
    # returns a polars dataframe
    _out_type: Type = field(default=pl.DataFrame)


@dataclass
class CreateHTTPLookupTableContext:
    name: str
    properties: dict[str, Any]
    query: str
    dynamic_columns: list[str]
    columns: dict[str, str]
    lookup: bool = True


# ---------- View / Sink Contexts ----------
@dataclass
class CreateViewContext:
    name: str
    upstreams: list[str]
    query: str

    # Transform is ultimately just a select applied
    # on upcoming data
    transform_ctx: SelectContext
    materialized: bool = False

    _out_type: Type = field(default=pl.DataFrame)


@dataclass
class CreateMaterializedViewContext:
    name: str
    upstreams: list[str]
    query: str

    # Transform is ultimately just a select applied
    # on upcoming data
    transform_ctx: SelectContext
    materialized: bool = True

    _out_type: Type = field(default=pl.DataFrame)


@dataclass
class CreateSinkContext:
    name: str
    upstreams: list[str]
    properties: dict[str, Any]
    subquery: str


@dataclass
class CreateSecretContext:
    name: str
    properties: dict[str, Any]
    value: str


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
class ShowContext:
    user_query: str
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
    CommandContext,
    CreateHTTPLookupTableContext,
    CreateHTTPTableContext,
    CreateWSTableContext,
    CreateMaterializedViewContext,
    CreateSecretContext,
    CreateSinkContext,
    CreateViewContext,
    SetContext,
    SelectContext,
    ShowContext,
]

OnStartContext = Union[CreateWSTableContext]

# Everything except Invalid and CreateSecret
QueryContext = Union[
    CreateHTTPLookupTableContext,
    CreateHTTPTableContext,
    CreateWSTableContext,
    CreateSinkContext,
    CreateViewContext,
    CreateMaterializedViewContext,
    SetContext,
    CommandContext,
    SelectContext,
    ShowContext,
]

NonQueryContext = Union[CreateSecretContext, InvalidContext]

# Table contexts of different connector type
CreateTableContext = Union[
    CreateHTTPLookupTableContext, CreateHTTPTableContext, CreateWSTableContext
]

ScheduledTaskContext = Union[CreateHTTPTableContext]
ContinousTaskContext = Union[CreateWSTableContext]

SinkTaskContext = Union[CreateSinkContext]
TransformTaskContext = Union[CreateMaterializedViewContext, CreateViewContext]
