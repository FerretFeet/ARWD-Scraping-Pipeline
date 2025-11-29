"""Indexed tree structure to store parent child relationships and allow id lookups."""

from __future__ import annotations

import json
from enum import Enum
from pathlib import Path
from typing import Any

from src.utils.custom_exceptions.indexed_tree_exceptions import TreeUpdateError
from src.utils.logger import logger


class PipelineStateEnum(Enum):
    """Enum for pipeline states."""

    CREATED = 0
    AWAITING_FETCH = 1
    FETCHING = 2
    AWAITING_PROCESSING = 3
    PROCESSING = 4
    PROCESSED = 5
    COMPLETED = 6


class Node:
    """Node for IndexedTree."""

    id_counter = 1

    def __init__(
        self,
        *,
        children: list[int] | None = None,
        parent: int | None = None,
        data: dict | None = None,
        state: int | PipelineStateEnum = PipelineStateEnum.CREATED,
        override_id: int | None = None,
    ) -> None:
        """Initialize Node."""
        # Allow ID override for loading from file
        if override_id is not None:
            self.id = override_id
            # Ensure counter is higher than any loaded ID
            if override_id >= Node.id_counter:
                Node.id_counter = override_id + 1
        else:
            self.id = Node.id_counter
            Node.id_counter += 1

        self.children = children or []
        self.parent = parent
        self.data = data or {}

        # Handle state input as Int (from JSON) or Enum
        if isinstance(state, int):
            self.state = PipelineStateEnum(state)
        else:
            self.state = state

    def to_dict(self) -> dict[str, Any]:
        """Serialize node to a dictionary."""
        return {
            "id": self.id,
            "children": self.children,
            "parent": self.parent,
            "data": self.data,
            "state": self.state.value,  # Store Enum as int
        }

    def __repr__(self) -> str:
        """Represent Node as string."""
        return (
            f"Node(id={self.id}, state={self.state.name}, children={self.children}, "
            f"parent={self.parent})"
        )


class IndexedTree:
    """Indexed tree structure to store parent child relationships and allow id lookups."""

    def __init__(self, root: Node | None = None) -> None:
        """Initialize IndexedTree."""
        self.nodes: dict[int, Node] = {}  # Removed | None, better to del key
        self.root: Node | None = None
        if root:
            self.set_root(root)

    def set_root(self, root: Node) -> None:
        """Set root tree."""
        self.nodes[root.id] = root
        self.root = root

    def add_node(
        self,
        *,
        children: list[int] | None = None,
        parent: int | None = None,
        data: dict | None = None,
    ) -> Node:
        """Add new node to tree and link it to parent."""
        new_node = Node(children=children, parent=parent, data=data)
        self.nodes[new_node.id] = new_node

        if parent is not None and parent in self.nodes:
            self.nodes[parent].children.append(new_node.id)

        return new_node

    def remove_parent(self, node_id: int) -> None:
        """Remove parent relationship by child node by id."""
        if node_id not in self.nodes:
            return

        node = self.nodes[node_id]
        parent_id = node.parent

        if parent_id and parent_id in self.nodes and node_id in self.nodes[parent_id].children:
            self.nodes[parent_id].children.remove(node_id)

        node.parent = None
        # No need to re-set self.nodes[node_id], object is mutable

    def __remove_node(self, node_id: int) -> None:
        """Remove node from tree completely."""
        if node_id not in self.nodes:
            return

        # 1. Unlink from parent
        self.remove_parent(node_id)

        # 2. Handle orphaned children (Optional: Decide if children are deleted or promoted)
        # Current logic: Children become orphans (parent=None)
        children = self.nodes[node_id].children
        for child_id in children:
            if child_id in self.nodes:
                self.nodes[child_id].parent = None

        # 3. Delete the node
        del self.nodes[node_id]

    def safe_remove_node(self, node_id: int) -> None:
        """Remove node from tree, raise error if it has children."""
        if node_id not in self.nodes:
            return

        node = self.nodes[node_id]
        if len(node.children) > 0:
            msg = f"Node {node_id} has active children: {node.children}. Cannot delete."
            raise TreeUpdateError(msg)

        self.__remove_node(node_id)

    def find_node(self, node_id: int) -> Node | None:
        """Find node by id."""
        return self.nodes.get(node_id)

    # --- Search Methods ---

    # --- Serialization Methods ---

    def to_JSON(self) -> str:  # noqa: N802
        """Serialize IndexedTree to a JSON string."""
        data = {
            "root_id": self.root.id if self.root else None,
            "nodes": [node.to_dict() for node in self.nodes.values()],
        }
        return json.dumps(data, indent=4)

    def from_JSON(self, json_str: str) -> None:  # noqa: N802
        """Load IndexedTree from a JSON string."""
        data = json.loads(json_str)

        self.nodes.clear()

        # Reconstruct Nodes
        for node_data in data["nodes"]:
            new_node = Node(
                children=node_data["children"],
                parent=node_data["parent"],
                data=node_data["data"],
                state=node_data["state"],
                override_id=node_data["id"],  # Important to restore specific IDs
            )
            self.nodes[new_node.id] = new_node

        # Set Root
        if data["root_id"] is not None:
            self.root = self.nodes.get(data["root_id"])

        # Ensure Global ID counter is synced after loading
        if self.nodes:
            max_id = max(self.nodes.keys())
            Node.id_counter = max_id + 1

    def save_file(self, filepath: Path) -> None:
        """Save tree to a file."""
        try:
            with Path.open(filepath, "w") as f:
                f.write(self.to_JSON())
            logger.info(f"Tree saved to {filepath}")
        except OSError as e:
            logger.error(f"Error saving file: {e}")

    def load_from_file(self, filepath: Path) -> None:
        """Load tree from a file."""
        if not Path(filepath).exists():
            msg = f"{filepath} does not exist."
            raise FileNotFoundError(msg)

        with Path.open(filepath) as f:
            json_str = f.read()

        self.from_JSON(json_str)
        logger.info(f"Tree loaded from {filepath}")

    def __repr__(self) -> str:
        """Represent IndexedTree as a string."""
        return (
            f"IndexedTree(Nodes: {len(self.nodes)}, "
            f"Root: {self.root.id if self.root else 'None'})"
        )
