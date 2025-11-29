from __future__ import annotations

from collections import defaultdict


class DependencyGraph:
    """
    Graph of dependancy between context
    """

    #: Flag to detect init
    _instanciated: bool = False

    #: Singleton instance
    _instance: DependencyGraph

    nodes: set[str]
    #: child  → {parents}
    children: dict[str, set[str]]
    #: parent → {children}
    parents: dict[str, set[str]]
    leaves: set[str]

    def __init__(self):
        raise NotImplementedError("Singleton — use DependencyGraph.get_instance()")

    def init(self):
        self.nodes = set()
        self.children = defaultdict(set)
        self.parents = defaultdict(set)
        self.leaves = set()

    @classmethod
    def get_instance(cls) -> DependencyGraph:
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

    def remove(self, node_id: str):
        """Remove a single leaf."""
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

    def remove_recursive(self, node_id: str) -> list[str]:
        """
        Remove node and ALL its descendants.
        Returns the list of nodes removed.
        """

        if node_id not in self.nodes:
            return []

        dropped_from_graph = []
        self._collect_descendants(node_id, dropped_from_graph)

        # reverse ensures children dropped first (safe order)
        dropped_from_graph = list(reversed(dropped_from_graph))
        for n in dropped_from_graph:
            self.remove(n)
        return dropped_from_graph

    def _collect_descendants(self, node_id: str, acc: list[str]):
        """DFS to collect all nodes reachable from node_id (including node)."""
        acc.append(node_id)
        for child in self.children.get(node_id, []):
            self._collect_descendants(child, acc)


dependency_grah = DependencyGraph.get_instance()
