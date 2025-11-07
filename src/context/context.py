from __future__ import annotations

import polars as pl

from dataclasses import dataclass, field
from typing import Any, Callable, Type, Union
from apscheduler.triggers.cron import CronTrigger
from sql.types import (
    Properties,
    SourceHttpProperties,
    SourceWSProperties,
)


class ValidContext:
    # Valid context are valid statements according to OMLSP Dialect
    # as opposed to InvalidContext
    ...


class EvaluableContext(ValidContext):
    # Evaluable context are supposed to be run directly by the app
    query: str


# ------- Context definitions -------
class CreateContext(EvaluableContext):
    name: str


# ---------- Table Contexts ----------
class CreateTableContext(CreateContext):
    properties: Properties
    lookup: bool


@dataclass
class CreateHTTPTableContext(CreateTableContext):
    name: str
    properties: SourceHttpProperties
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
    properties: SourceWSProperties
    query: str
    column_types: dict[str, str]
    generated_columns: dict[str, Callable]
    on_start_query: str
    lookup: bool = False
    source: bool = False

    # executable of ws table context
    # returns a polars dataframe
    _out_type: Type = field(default=pl.DataFrame)


@dataclass
class CreateHTTPLookupTableContext(CreateTableContext):
    name: str
    properties: SourceHttpProperties
    query: str
    dynamic_columns: list[str]
    columns: dict[str, str]
    lookup: bool = True


# ---------- Source Contexts ----------
class CreateSourceContext(CreateContext):
    properties: Properties


@dataclass
class CreateHTTPSourceContext(CreateSourceContext):
    name: str
    properties: SourceHttpProperties
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
    properties: SourceWSProperties
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

    # executable of ws table context
    # returns a polars dataframe
    _out_type: Type = field(default=pl.DataFrame)


@dataclass
class CreateSinkContext(CreateContext):
    name: str
    upstreams: list[str]
    properties: dict[str, Any]
    subquery: str

    # executable of ws table context
    # returns a polars dataframe
    _out_type: Type = field(default=pl.DataFrame)


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
