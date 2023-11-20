import collections
import heapq
from abc import ABC, abstractmethod
from collections import deque
from typing import List, Any, Dict, TypeVar, Generic

from engine.exceptions import ValidationException


class Node(ABC):

    def __init__(self, task_id: str):
        self._task_id = task_id
        self.in_degree = 0

    @property
    def task_id(self) -> str:
        return self._task_id

    @abstractmethod
    def add_leaf(self, node):
        pass

    @abstractmethod
    def add_parent(self, node):
        pass

    @abstractmethod
    def remove_parent(self, node):
        pass


class DNode(Node):

    def __init__(self, task_id: str):
        super().__init__(task_id)
        self._parents = list()
        self._leaves = list()

    def __lt__(self, other):
        """Comparison rule to < operator."""
        return self.in_degree < other.in_degree

    def __repr__(self):
        return self.task_id

    @property
    def leaves(self) -> List[Node]:
        return self._leaves

    @property
    def parents(self) -> List[Node]:
        return self._parents

    def add_leaf(self, node: Node):
        self._leaves.append(node)

    def add_parent(self, node: Node):
        self._parents.append(node)

    def remove_parent(self, node: Node):
        self._parents.remove(node)

    def __rshift__(self, other: Node):
        self._leaves.append(other)
        other.add_parent(self)


C_WHITE = 0
C_GRAY = 1
C_BLACK = 2

T = TypeVar("T", bound=DNode)


class Graph(Generic[T]):

    def __init__(self, nodes: List[T]):
        self._edges: Dict[str, str] = {}
        self._nodes = nodes or list()
        self.edges = list()
        for node in self._nodes:
            for child in node.leaves:
                self.edges.append((node.task_id, child.task_id))
        self.adj = self._build_adjacency_list()

    def _build_adjacency_list(self) -> Dict[Any, List]:
        adj = collections.defaultdict(list)
        for edge in self.edges:
            adj[edge[0]].append(edge[1])  # directed graph
        return adj

    def _dfs(self, u: str, color: dict) -> bool:
        # GRAY :  This vertex is being processed (DFS for this vertex has started, but not finished yet
        color[u] = C_GRAY

        for v in self.adj[u]:
            if color[v] == C_GRAY:
                return True
            if color[v] == C_WHITE and self._dfs(v, color):
                return True

        color[u] = C_BLACK
        return False

    def _check_cycle(self) -> None:
        color = dict()
        for u, v in self.edges:
            color[u] = C_WHITE
            color[v] = C_WHITE
        for node_id in list(self.adj.keys()):
            if color[node_id] == C_WHITE:
                if self._dfs(node_id, color):
                    raise ValidationException(f'cycle detected from node {node_id}')

    def add_node(self, node: T) -> None:
        self._nodes.append(node)

    def prepare(self):
        q = deque()
        for node in self._nodes:
            node.in_degree = len(node.parents)
            if node.in_degree == 0:
                q.append(node)
        heapq.heapify(self._nodes)  # nodes with less in_degree will be on top
        if not q:
            raise ValidationException('cannot find next node due to the cycle')
        self._check_cycle()

    def has_next(self) -> bool:
        return len(self._nodes) > 0

    def next(self) -> T:
        node = heapq.heappop(self._nodes)
        if node.in_degree:
            raise Exception('cannot find next node with in_degree = 0 (cycle is possible)')
        for child in node.leaves:
            child.in_degree -= 1
        heapq.heapify(self._nodes)
        return node
