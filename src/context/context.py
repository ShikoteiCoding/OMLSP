from __future__ import annotations

import polars as pl

from dataclasses import dataclass, field
from typing import Any, Callable, Type, Union
from apscheduler.triggers.cron import CronTrigger


class ValidContext:
    # Valid context are valid statements according to OMLSP Dialect
    # as opposed to InvalidContext
    ...


class EvaluableContext(ValidContext):
    # Evaluable context are supposed to be run directly by the app
    ...


# --- Context definitions ---
class CreateContext(EvaluableContext):
    name: str


# ---------- Table Contexts ----------
class CreateTableContext(CreateContext):
    name: str
    query: str
    lookup: bool


@dataclass
class CreateHTTPTableContext(CreateTableContext):
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
class CreateWSTableContext(CreateTableContext):
    name: str
    properties: dict[str, Any]
    column_types: dict[str, str]
    generated_columns: dict[str, Callable]
    query: str
    on_start_query: str
    lookup: bool = False
    source: bool = False

    # executable of ws table context
    # returns a polars dataframe
    _out_type: Type = field(default=pl.DataFrame)


@dataclass
class CreateHTTPLookupTableContext(CreateTableContext):
    name: str
    properties: dict[str, Any]
    query: str
    dynamic_columns: list[str]
    columns: dict[str, str]
    lookup: bool = True


# ---------- Source Contexts ----------
class CreateSourceContext(CreateContext):
    name: str
    query: str


@dataclass
class CreateHTTPSourceContext(CreateSourceContext):
    name: str
    properties: dict[str, Any]
    query: str
    column_types: dict[str, str]
    generated_columns: dict[str, Callable]
    trigger: CronTrigger
    source: bool = True

    # executable of http table context
    # returns a polars dataframe
    _out_type: Type = field(default=pl.DataFrame)


@dataclass
class CreateWSSourceContext(CreateSourceContext):
    name: str
    properties: dict[str, Any]
    column_types: dict[str, str]
    generated_columns: dict[str, Callable]
    query: str
    on_start_query: str
    source: bool = True

    # executable of ws table context
    # returns a polars dataframe
    _out_type: Type = field(default=pl.DataFrame)


# ---------- View / Sink Contexts ----------
@dataclass
class CreateViewContext(CreateContext):
    name: str
    upstreams: list[str]
    query: str

    # Transform is ultimately just a select applied
    # on upcoming data
    transform_ctx: SelectContext
    materialized: bool

    _out_type: Type = field(default=pl.DataFrame)


@dataclass
class CreateSinkContext(CreateContext):
    name: str
    upstreams: list[str]
    properties: dict[str, Any]
    subquery: str


@dataclass
class CreateSecretContext(CreateContext):
    name: str
    properties: dict[str, Any]
    value: str


# ---------- Query / Command Contexts ----------
@dataclass
class SelectContext(EvaluableContext):
    columns: list[str]
    table: str
    alias: str
    where: str
    joins: dict[str, str]
    query: str


@dataclass
class SetContext(EvaluableContext):
    query: str


@dataclass
class ShowContext(EvaluableContext):
    user_query: str
    query: str


@dataclass
class CommandContext(EvaluableContext):
    query: str


@dataclass
class InvalidContext:
    reason: str


OnStartContext = CreateWSTableContext

# Context part of task flow
# TODO: change union for mixin dependencies
ScheduledTaskContext = Union[CreateHTTPSourceContext, CreateHTTPTableContext]
ContinousTaskContext = Union[CreateWSSourceContext, CreateWSTableContext]
SinkTaskContext = Union[CreateSinkContext]
TransformTaskContext = Union[CreateViewContext]
TaskContext = Union[
    ScheduledTaskContext,
    ContinousTaskContext,
    SinkTaskContext,
    TransformTaskContext,
    # TODO: I don't think this should be considered a TaskContext
    # Move the lookup registration to the app._eval_ctx and make it
    # register macro + scalar func from there
    CreateHTTPLookupTableContext,
]
