from collections import namedtuple
from typing import Union


# --- Context definitions ---
CreateTableContext = namedtuple(
    "CreateTableContext", ["name", "properties", "query", "trigger"]
)
CreateLookupTableContext = namedtuple(
    "CreateLookupTableContext",
    ["name", "properties", "query", "dynamic_columns", "columns"],
)

# TODO: implement those new contexts
CreateViewContext = namedtuple("CreateViewContext", ["name", "upstreams"])
CreateMaterializedViewContext = namedtuple(
    "CreateMaterializedViewContext", ["name", "upstreams"]
)
CreateSinkContext = namedtuple("CreateSinkContext", ["name", "upstreams", "properties"])

SelectContext = namedtuple(
    "SelectContext", ["columns", "table", "alias", "where", "joins", "query"]
)
SetContext = namedtuple("SetContext", ["query"])
CommandContext = namedtuple("CommandContext", ["query"])

InvalidContext = namedtuple("InvalidContext", ["reason"])


# --- Unions for type hints ---
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
    SetContext,
    CommandContext,
    SelectContext,
]

# Everything except Invalid
QueryContext = Union[
    CreateLookupTableContext,
    CreateTableContext,
    CreateSinkContext,
    SetContext,
    CommandContext,
    SelectContext,
]

# Sub type of TaskContext
SourceTaskContext = Union[CreateTableContext]
