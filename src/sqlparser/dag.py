import networkx as nx # to replace by owned one ?

from collections import defaultdict, OrderedDict, deque

from sqlparser.parser import SQLParams, CreateTableParams, CreateLookupTableParams, SelectParams, SetParams

from typing import Any
from loguru import logger

class OrderedDefaultDict(OrderedDict, defaultdict):
    def __init__(self, default_factory=None, *args, **kwargs):
        super(OrderedDefaultDict, self).__init__(*args, **kwargs)
        self.default_factory = default_factory

class DataFlowDAG:
    _adj_list: OrderedDict[Any, list[Any]] = OrderedDefaultDict(list)

    def add_edge(self, source: Any, dest: Any):
        self._adj_list[source].append(dest)

    def _bfs(self) -> list[Any]:
        vertices = len(self._adj_list)

        res = []
        s = 0
        q = deque()
        
        visited = [False] * vertices
        visited[s] = True
        q.append(s)
        
        while q:
            curr = q.popleft()
            res.append(curr)
            
            for x in self._adj_list[curr]:
                if not visited[x]:
                    visited[x] = True
                    q.append(x)
                    
        return res

    def __repr__(self):
        bfs = self._bfs()
        return bfs
        

#def find_tables_in_expression(expression):
#    """Recursively finds all table/view references in a sqlglot expression."""
#    tables = set()
#    # Use find_all(Table) to capture all tables, views, etc.
#    for sub_expression in expression.find_all(Table):
#        tables.add(sub_expression.name)
#    return tables

def build_dataflows(sql_params: list[SQLParams]) -> list[DataFlowDAG]:
    """
    """
    dags: list[DataFlowDAG]  = []
    sources: list[CreateTableParams] = []

    for sql_param in sql_params:

        if isinstance(sql_param, CreateTableParams):
            sources.append(sql_param)

        if isinstance(sql_param, )

    return dags

if __name__ == "__main__":
    from pathlib import Path
    from sqlparser.parser import build_sql_params
    import json

    file = "examples/basic.sql"
    prop_schema_file = "src/properties.schema.json"

    sql_filepath = Path(file)
    prop_schema_filepath = Path(prop_schema_file)
    sql_content: str

    with open(sql_filepath, "rb") as fo:
        sql_content = fo.read().decode("utf-8")
    with open(prop_schema_filepath, "rb") as fo:
        properties_schema = json.loads(fo.read().decode("utf-8"))

    ordered_statements = build_sql_params(sql_content, properties_schema)

    