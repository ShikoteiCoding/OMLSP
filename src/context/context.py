from __future__ import annotations

import polars as pl

from dataclasses import dataclass, field
from typing import Any, Callable, Type, Union
from apscheduler.triggers.cron import CronTrigger

# --- Context definitions ---


# ---------- Table Contexts ----------
@dataclass
class CreateHTTPTableContext:
    name: str
    properties: dict[str, Any]
    query: str
    column_types: dict[str, str]
    generated_columns: dict[str, Callable]
    trigger: CronTrigger
    lookup: bool = False
    source: bool = False

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
    source: bool = False

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


# ---------- Source Contexts ----------
@dataclass
class CreateHTTPSourceContext:
    name: str
    properties: dict[str, Any]
    query: str
    column_types: dict[str, str]
    generated_columns: dict[str, Callable]
    trigger: CronTrigger
    lookup: bool = False
    source: bool = True

    # executable of http table context
    # returns a polars dataframe
    _out_type: Type = field(default=pl.DataFrame)


# ---------- View / Sink Contexts ----------
@dataclass
class CreateViewContext:
    name: str
    upstreams: list[str]
    query: str

    # Transform is ultimately just a select applied
    # on upcoming data
    transform_ctx: SelectContext
    materialized: bool

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
EvaluableContext = Union[
    CommandContext,
    CreateHTTPLookupTableContext,
    CreateHTTPSourceContext,
    CreateHTTPTableContext,
    CreateWSTableContext,
    CreateSecretContext,
    CreateSinkContext,
    CreateViewContext,
    SetContext,
    SelectContext,
    ShowContext,
]

OnStartContext = CreateWSTableContext

# Everything except Invalid and CreateSecret
QueryContext = Union[
    CreateHTTPLookupTableContext,
    CreateHTTPSourceContext,
    CreateHTTPTableContext,
    CreateWSTableContext,
    CreateSinkContext,
    CreateViewContext,
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

# Source contexts of different connector type
CreateSourceContext = Union[CreateHTTPSourceContext]

# Context part of task flow
ScheduledTaskContext = Union[CreateHTTPSourceContext, CreateHTTPTableContext]
ContinousTaskContext = Union[CreateWSTableContext]
SinkTaskContext = Union[CreateSinkContext]
TransformTaskContext = Union[CreateViewContext]
TaskContext = Union[
    ScheduledTaskContext, ContinousTaskContext, SinkTaskContext, TransformTaskContext
]
