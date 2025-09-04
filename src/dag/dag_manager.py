import trio

from collections import defaultdict, deque

from context.context import (
    QueryContext,
    CreateTableContext,
    CreateViewContext,
    CreateMaterializedViewContext,
    TaskContext,
    InvalidContext,
)

class ContextNode:
    def __init__(self, context: TaskContext):
        self.context = context
        self.name = context.name

    def __getitem__(self):
        return self.context.name

    def __repr__(self):
        return f"Node({self.context.name!r})"

    def __hash__(self):
        return hash(self.context.name)

    def __eq__(self, other):
        return (
            isinstance(other, ContextNode) and self.context.name == other.context.name
        )


class DataFlowDAG:
    def __init__(self):
        self._adj_list: dict[ContextNode, list[ContextNode]] = defaultdict(list)

    def add_edge(self, source: ContextNode, dest: ContextNode):
        self._adj_list[source].append(dest)
        # ensure dest also exists in adj_list (even if no outgoing edges)
        if dest not in self._adj_list:
            self._adj_list[dest] = []

    def _bfs(self, start: ContextNode) -> list[ContextNode]:
        res = []
        visited = set()
        q = deque()

        visited.add(start)
        q.append(start)

        while q:
            curr = q.popleft()
            res.append(curr)

            for neighbor in self._adj_list[curr]:
                if neighbor not in visited:
                    visited.add(neighbor)
                    q.append(neighbor)

        return res

    def merge(self, other: "DataFlowDAG") -> None:
        """Merge another DAG into this one."""
        for node, neighbors in other._adj_list.items():
            if node not in self._adj_list:
                self._adj_list[node] = []
            self._adj_list[node].extend(neighbors)

    def __repr__(self):
        out = []
        for src, dests in self._adj_list.items():
            out.append(f"{src} -> {dests}")
        return "\n".join(out)


class DagManager:
    node_to_dag: dict[ContextNode, DataFlowDAG] = {}
    dags: list[DataFlowDAG] = []
    sources: list[ContextNode] = []

    def update(self, query_ctx: TaskContext, nursery: trio.Nursery):
        node = ContextNode(query_ctx)

        if isinstance(query_ctx, CreateTableContext):
            dag = DataFlowDAG()
            dag._adj_list[node] = []
            dags.append(dag)
            self.node_to_dag[node] = dag

        elif isinstance(query_ctx, (CreateViewContext, CreateMaterializedViewContext)):
            upstream_nodes = [ContextNode(up) for up in query_ctx.upstreams]
            new_node = node

            # collect all dags involved
            involved_dags = {
                self.node_to_dag[up] for up in upstream_nodes if up in self.node_to_dag
            }

            if not involved_dags:
                # no upstream in existing DAGs → create fresh DAG
                dag = DataFlowDAG()
                for up in upstream_nodes:
                    dag._adj_list[up] = []
                    dag.add_edge(up, new_node)
                dags.append(dag)
                self.node_to_dag[new_node] = dag
                for up in upstream_nodes:
                    self.node_to_dag[up] = dag

            else:
                # merge all dags if >1
                dag = involved_dags.pop()
                while involved_dags:
                    other = involved_dags.pop()
                    dag.merge(other)
                    dags.remove(other)
                    # update node→dag mapping
                    for n in other._adj_list.keys():
                        self.node_to_dag[n] = dag

                # now add edges from upstreams
                for up in upstream_nodes:
                    if up not in dag._adj_list:
                        dag._adj_list[up] = []
                    dag.add_edge(up, new_node)
                    self.node_to_dag[up] = dag

                self.node_to_dag[new_node] = dag
                
        self.run(nursery)

    def run(self, nursery: trio.Nursery):
        pass
        


def build_dataflows(query_contexts: list[QueryContext]) -> list[DataFlowDAG]:
    """
    Build DAGs from SQLContext (sources and derived views).
    """
    # Node -> DAG mapping
    node_to_dag: dict[ContextNode, DataFlowDAG] = {}
    dags: list[DataFlowDAG] = []

    for query_ctx in query_contexts:
        if not isinstance(query_ctx, TaskContext):
            continue

        node = ContextNode(query_ctx)

        if isinstance(query_ctx, CreateTableContext):
            dag = DataFlowDAG()
            dag._adj_list[node] = []
            dags.append(dag)
            node_to_dag[node] = dag

        elif isinstance(query_ctx, (CreateViewContext, CreateMaterializedViewContext)):
            upstream_nodes = [ContextNode(up) for up in query_ctx.upstreams]
            new_node = node

            # collect all dags involved
            involved_dags = {
                node_to_dag[up] for up in upstream_nodes if up in node_to_dag
            }

            if not involved_dags:
                # no upstream in existing DAGs → create fresh DAG
                dag = DataFlowDAG()
                for up in upstream_nodes:
                    dag._adj_list[up] = []
                    dag.add_edge(up, new_node)
                dags.append(dag)
                node_to_dag[new_node] = dag
                for up in upstream_nodes:
                    node_to_dag[up] = dag

            else:
                # merge all dags if >1
                dag = involved_dags.pop()
                while involved_dags:
                    other = involved_dags.pop()
                    dag.merge(other)
                    dags.remove(other)
                    # update node→dag mapping
                    for n in other._adj_list.keys():
                        node_to_dag[n] = dag

                # now add edges from upstreams
                for up in upstream_nodes:
                    if up not in dag._adj_list:
                        dag._adj_list[up] = []
                    dag.add_edge(up, new_node)
                    node_to_dag[up] = dag

                node_to_dag[new_node] = dag

    return dags


if __name__ == "__main__":
    from pathlib import Path
    from sql.sqlparser.parser import extract_query_contexts
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

    query_contexts = extract_query_contexts(sql_content, properties_schema)

    dags = build_dataflows(
        [
            query_context
            for query_context in query_contexts
            if not isinstance(query_context, InvalidContext)
        ]
    )
    print(dags)
