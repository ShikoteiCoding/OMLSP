from collections import namedtuple
from typing import Union

# --- Context definitions ---
CreateHTTPTableContext = namedtuple(
    "CreateTableContext", ["name", "properties", "query", "trigger"]
)
CreateHTTPLookupTableContext = namedtuple(
    "CreateLookupTableContext",
    ["name", "properties", "query", "dynamic_columns", "columns"],
)
CreateWSTableContext = namedtuple(
    "CreateWSTableContext", ["name", "properties", "query"]
)

# TODO: implement those new contexts
CreateViewContext = namedtuple("CreateViewContext", ["name", "upstreams", "query"])
CreateMaterializedViewContext = namedtuple(
    "CreateMaterializedViewContext", ["name", "upstreams"]
)
CreateSinkContext = namedtuple(
    "CreateSinkContext", ["name", "upstreams", "properties", "query"]
)

SelectContext = namedtuple(
    "SelectContext", ["columns", "table", "alias", "where", "joins", "query"]
)
SetContext = namedtuple("SetContext", ["query"])
CommandContext = namedtuple("CommandContext", ["query"])

InvalidContext = namedtuple("InvalidContext", ["reason"])


# --- Unions for type hints ---
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

# Sub type of TaskContext
SourceTaskContext = Union[CreateHTTPTableContext]
