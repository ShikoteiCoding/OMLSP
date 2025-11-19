from collections import defaultdict


class TaskGraph:
    def __init__(self):
        self.nodes = set()
        self.children = defaultdict(set)  # parent → {children}
        self.parents = defaultdict(set)  # child  → {parents}
        self.leaves = set()

    # ──────────────────────────────────────────────
    # Basic vertex / edge construction
    # ──────────────────────────────────────────────

    def ensure_vertex(self, name: str):
        """Make sure a node exists even without edges."""
        if name not in self.nodes:
            self.nodes.add(name)
            self.leaves.add(name)

    def add_edge(self, parent: str, child: str):
        """Add an edge parent → child, creating missing vertices."""
        self.ensure_vertex(parent)
        self.ensure_vertex(child)

        self.children[parent].add(child)
        self.parents[child].add(parent)

        # parent is no longer a leaf
        self.leaves.discard(parent)

    def add_vertex(self, parent: str | None, child: str):
        """Backward-compatible with your current API."""
        if parent is None:
            self.ensure_vertex(child)
        else:
            self.add_edge(parent, child)

    def can_drop(self, name: str) -> bool:
        """Only leaves may be dropped."""
        return name in self.leaves

    # ──────────────────────────────────────────────
    # RECURSIVE DROP
    # ──────────────────────────────────────────────

    def drop_recursive(self, name: str) -> list[str]:
        """
        Drop `name` and ALL its descendants.
        Returns the list of nodes dropped (in topological order).
        """

        if name not in self.nodes:
            return []

        to_drop = []
        self._collect_descendants(name, to_drop)

        # reverse ensures children dropped first (safe order)
        for n in reversed(to_drop):
            self._drop_single(n)

        return to_drop

    def _collect_descendants(self, name: str, acc: list[str]):
        """DFS to collect all nodes reachable from name (including name)."""
        acc.append(name)
        for child in self.children.get(name, []):
            self._collect_descendants(child, acc)

    def _drop_single(self, name: str):
        """Drop a single leaf-like vertex (children already removed)."""
        # Remove node
        self.nodes.discard(name)
        self.leaves.discard(name)

        # Remove from parents
        for p in list(self.parents.get(name, [])):
            self.children[p].discard(name)
            if not self.children[p]:
                self.leaves.add(p)

        # Cleanup maps
        if name in self.parents:
            del self.parents[name]
        if name in self.children:
            del self.children[name]
