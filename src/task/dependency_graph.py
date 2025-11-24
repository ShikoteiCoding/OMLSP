from __future__ import annotations

from collections import defaultdict


class DependancyGraph:
    """
    Graph of dependancy between running taks
    """

    #: Flag to detect init
    _instanciated: bool = False

    #: Singleton instance
    _instance: DependancyGraph

    nodes: set
    #: parent → {children}
    children: defaultdict
    #: child  → {parents}
    parents: defaultdict
    leaves: set

    def __init__(self):
        raise NotImplementedError("Singleton — use TaskGraph.get_instance()")

    def init(self):
        self.nodes = set()
        self.children = defaultdict(set)
        self.parents = defaultdict(set)
        self.leaves = set()

    @classmethod
    def get_instance(cls) -> DependancyGraph:
        if cls._instanciated:
            return cls._instance

        cls._instance = cls.__new__(cls)
        cls._instance.init()
        cls._instanciated = True

        return cls._instance

    def ensure_vertex(self, node_id: str):
        """Make sure a node exists even without edges."""
        if node_id not in self.nodes:
            self.nodes.add(node_id)
            self.leaves.add(node_id)

    def add_edge(self, parent: str, child: str):
        """Add an edge parent → child, creating missing vertices."""
        self.ensure_vertex(parent)
        self.ensure_vertex(child)

        self.children[parent].add(child)
        self.parents[child].add(parent)

        # parent is no longer a leaf
        self.leaves.discard(parent)

    def add_vertex(self, parent: str | None, child: str):
        if parent is None:
            self.ensure_vertex(child)
        else:
            self.add_edge(parent, child)

    def is_a_leaf(self, node_id: str) -> bool:
        return node_id in self.leaves

    def drop_leaf(self, node_id: str):
        """Drop a single leaf."""
        # remove node
        self.nodes.discard(node_id)
        self.leaves.discard(node_id)

        # remove from parents
        for p in list(self.parents.get(node_id, [])):
            self.children[p].discard(node_id)
            if not self.children[p]:
                self.leaves.add(p)

        # cleanup
        if node_id in self.parents:
            del self.parents[node_id]
        if node_id in self.children:
            del self.children[node_id]

    def drop_recursive(self, node_id: str) -> list[str]:
        """
        Drop node and ALL its descendants.
        Returns the list of nodes dropped.
        """

        if node_id not in self.nodes:
            return []

        dropped_from_graph = []
        self._collect_descendants(node_id, dropped_from_graph)

        # reverse ensures children dropped first (safe order)
        dropped_from_graph = list(reversed(dropped_from_graph))
        for n in dropped_from_graph:
            self.drop_leaf(n)
        return dropped_from_graph

    def _collect_descendants(self, node_id: str, acc: list[str]):
        """DFS to collect all nodes reachable from node_id (including node)."""
        acc.append(node_id)
        for child in self.children.get(node_id, []):
            self._collect_descendants(child, acc)


dependency_grah = DependancyGraph.get_instance()
