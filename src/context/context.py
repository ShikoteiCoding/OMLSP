from collections import namedtuple
from typing import Union


# --- Context definitions ---
CreateTableContext = namedtuple("CreateTableContext", ["name", "properties", "query"])
CreateLookupTableContext = namedtuple(
    "CreateLookupTableContext",
    ["name", "properties", "query", "dynamic_columns", "columns"],
)

# TODO: implement those new contexts
CreateViewContext = namedtuple("CreateViewContext", ["name", "upstreams"])
CreateMaterializedViewContext = namedtuple(
    "CreateMaterializedViewContext", ["name", "upstreams"]
)
CreateSinkContext = namedtuple("CreateSinkContext", ["name", "upstreams"])

SelectContext = namedtuple(
    "SelectContext", ["columns", "table", "alias", "where", "joins", "query"]
)
SetContext = namedtuple("SetContext", ["query"])

InvalidContext = namedtuple("InvalidContext", ["reason"])


# --- Unions for type hints ---
# Context to eval once
EvalContext = Union[
    SelectContext,
    SetContext
]
# Context part of task flow
TaskContext = Union[
    CreateLookupTableContext,
    CreateTableContext,
    CreateViewContext,
    CreateMaterializedViewContext,
]
QueryContext = Union[
   EvalContext, TaskContext
]

# Sub type of TaskContext
SourceTaskContext = Union[
    CreateTableContext
]
