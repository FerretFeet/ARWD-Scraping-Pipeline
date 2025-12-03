"""Indexed tree structure to store parent child relationships and allow id lookups."""

from __future__ import annotations

import json
import threading
from enum import IntEnum
from pathlib import Path
from typing import TYPE_CHECKING, Any

from src.utils.custom_exceptions.indexed_tree_exceptions import TreeUpdateError
from src.utils.logger import logger

if TYPE_CHECKING:
    from src.structures.registries import PipelineRegistryKeys


class PipelineStateEnum(IntEnum):
    """Enum for pipeline states."""

    ERROR = -1
    CREATED = 0
    AWAITING_FETCH = 1
    FETCHING = 2
    AWAITING_PROCESSING = 3
    PROCESSING = 4
    AWAITING_LOAD = 5
    LOADING = 6
    COMPLETED = 7


class Node:
    """Node for IndexedTree."""

    id_counter = 1

    def __init__(  # noqa: PLR0913
        self,
        *,
        node_type: PipelineRegistryKeys,
        children: list[Node] | None,
        parent: Node | None,
        data: dict | None = None,
        state: PipelineStateEnum = PipelineStateEnum.CREATED,
        override_id: int | None = None,
        url: str | None,
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
        self.url = url
        self.type = node_type
        self.lock = threading.Lock()

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
            f"Node(id={self.id}, state={self.state.name}, "
            f"children={[child.id for child in self.children]}, "
            f"parent={self.parent.id if self.parent else "None"}, type={self.type}, url={self.url})"
            f", data={self.data}"
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

    def add_node(  # noqa: PLR0913
        self,
        *,
        node_type: PipelineRegistryKeys,
        parent: Node | None,
        url: str | None,
        children: list[Node] | None = None,
        data: dict | None = None,
        state: PipelineStateEnum = PipelineStateEnum.CREATED,
    ) -> Node:
        """Add new node to tree and link it to parent. Parent = None to set root."""
        new_node = Node(
            children=children,
            parent=parent,
            data=data,
            state=state,
            url=url,
            node_type=node_type,
        )

        if parent is not None and parent.id in self.nodes:
            parent.children.append(new_node)
        elif parent is None:
            if self.root is None:
                self.root = new_node
            else:
                msg = "Attempted to create Node with no parent and root is occupied"
                err = TreeUpdateError(msg)
                raise err
        else:
            self.nodes[parent.id] = parent
        self.nodes[new_node.id] = new_node

        return new_node

    def remove_parent(self, node: int | Node) -> None:
        """Remove parent relationship by child node by id or by reference."""
        if isinstance(node, int):
            if node not in self.nodes:
                return
            node = self.nodes[node]
        if not node.parent: return
        parent_id = node.parent.id

        if parent_id and parent_id in self.nodes and node in node.parent.children:
            node.parent.children.remove(node)

        node.parent = None
        # No need to re-set self.nodes[node_id], object is mutable

    def __remove_node(self, node: Node) -> None:
        """Remove node from tree completely. By id or reference."""
        # 1. Unlink from parent
        self.remove_parent(node)

        # Current logic: Children become orphans (parent=None)
        for child in node.children:
            if child in self.nodes:
                child.parent = None
        if node == self.root:
            self.root = None

        # 3. Delete the node

        del self.nodes[node.id]
        del node

    def safe_remove_node(self, node: int | Node, *, cascade_up: bool = False) -> None | int:
        """Remove node from tree, raise error if it has children."""
        if not node:
            return None
        if isinstance(node, int):
            if node not in self.nodes:
                return None
            node = self.nodes[node]
        parent = node.parent
        if len(node.children) > 0:
            return None

        self.__remove_node(node)

        if cascade_up:
            self.safe_remove_node(parent, cascade_up=True)

        return 1

    def find_node(self, node_id: int) -> Node | None:
        """Find node by id."""
        return self.nodes.get(node_id)

    # --- Traversal Methods ---
    def __traversal_filter(
        self,
        node: Node,
        *,
        data_attrs: dict | None = None,
        node_attrs: dict | None = None,
    ) -> Node | None:
        should_visit = True
        if not node: return None
        print("Searching for filter")
        if data_attrs and node:
            for key, value in data_attrs.items():
                if key not in node.data:
                    print("key not in")
                    should_visit = False
                    return None
                node_value = node.data[key]
                print(f"nodeval is {node_value}")
                print(f"Value is {value} {value!r} {value is None}")
                if value is not None and node_value != value:
                    print("value fail")
                    should_visit = False
                    return None
        if node_attrs and node:
            print("searching node attrs")
            for key, value in node_attrs.items():
                if key not in node.__dict__:
                    print(f"key not in:: {key},,,, {node.__dict__}")
                    should_visit = False
                    return None
                node_value = getattr(node, key)
                print(f"nodeval = {node_value}")
                if value is not None and node_value != value:
                    print(f"filter set false for nodeattrs {node.__dict__}, {node_attrs}")

                    should_visit = False
                    return None
        return node if should_visit else None

    def reverse_in_order_traversal(
        self,
        node_id: int | None = None,
        *,
        data_attrs: dict | None = None,
        node_attrs: dict | None = None,
    ) -> list[Node]:
        """
        Perform a Reverse In-Order Traversal starting from a given node ID (defaults to root).

        Return a list of nodes in the order they are visited.
        """
        if node_id is None:
            node_id = self.root.id if self.root else None

        if node_id is None:
            return []

        result = []
        node = self.find_node(node_id)
        if node is None:
            return []

        for child in reversed(node.children):
            result.extend(
                self.reverse_in_order_traversal(
                    child.id,
                    data_attrs=data_attrs,
                    node_attrs=node_attrs,
                ),
            )

        if self.__traversal_filter(node, data_attrs=data_attrs, node_attrs=node_attrs):
            # Optionally filter results. returns falsy if target data not in node
            result.append(node)

        return result

    def preorder_traversal(
        self,
        node_id: int | None = None,
        *,
        data_attrs: dict | None = None,
        node_attrs: dict | None = None,
    ) -> list[Node]:
        """
        Perform Pre-order Traversal starting from a given node ID (defaults to root).

        Return a list of nodes in the order they are visited.
        Optionally filter based on a node attribute value
        """
        node = self.root if node_id is None else self.find_node(node_id)

        result = []
        if node is None:
            return result

        if self.__traversal_filter(node, data_attrs=data_attrs, node_attrs=node_attrs):
            # Optionally filter results. returns falsy if target data not in node
            result.append(node)

        # 2. Recurse on Children (Left to Right, following list order)
        for child in node.children:
            result.extend(
                self.preorder_traversal(child.id, data_attrs=data_attrs, node_attrs=node_attrs),
            )

        return result

    def find_val_in_ancestor(
        self,
        node: int | Node,
        data_attrs: dict | None = None,
        node_attrs: dict | None = None,
    ) -> dict | None:
        """
        Find an ancestor with a specified .data attr or value.

        Args:
            node is a IndexedTree node or node id
            data_attrs is a dict of key and optional values to match a node's data attribute.
            node_attrs is a dict of key and optional values to match a node's direct attributes.

        Returns:
            A dict of key:val for all given keys for the first node which matches all given keys
                and optionally values.

        """
        if isinstance(node, int):
            node = self.find_node(node)
        print(f"Finding ancestor: {node}")
        if self.__traversal_filter(node, data_attrs=data_attrs, node_attrs=node_attrs):
            print(f"Found ancestor: {node}")
            dattrs = nattrs = {}
            if data_attrs:

                dattrs = {key: node.data[key] for key in data_attrs}
                print(f"Data attrs: {dattrs} made from {[key for key in dattrs]})")
            if node_attrs:
                print("Should not see this line")
                nattrs = {key: getattr(node, key) for key in node_attrs}
            print(f"nattrs: {nattrs}, dattrs: {dattrs}")
            dattrs.update(nattrs)
            print(f"Result: {dattrs}")
            return dattrs

        if not node.parent: return None
        print("Trying new ancestor")
        return self.find_val_in_ancestor(node.parent, data_attrs, node_attrs)

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
                url=node_data["url"],
                node_type=node_data["node_type"],
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

    def load_from_file(self, filepath: Path) -> None | int:
        """Load tree from a file."""
        if not Path(filepath).exists():
            msg = f"{filepath} does not exist."
            # raise FileNotFoundError(msg)
            return None

        with Path.open(filepath) as f:
            json_str = f.read()

        self.from_JSON(json_str)
        if self.root:
            logger.info(f"Tree loaded from {filepath}")
            return 1
        return None

    def __repr__(self) -> str:
        """Represent IndexedTree as a string."""
        return (
            f"IndexedTree(Nodes: {len(self.nodes)}, "
            f"Root: {self.root.id if self.root else 'None'})"
        )

    def reconstruct_order(self):
        return (self.reverse_in_order_traversal(
                self.root,
                node_attrs={"state": PipelineStateEnum.AWAITING_FETCH},
            )
            + self.reverse_in_order_traversal(
                self.root,
                node_attrs={"state": PipelineStateEnum.CREATED},
            )
            + self.reverse_in_order_traversal(
                self.root,
                node_attrs={"state": PipelineStateEnum.FETCHING},
            )
            + self.reverse_in_order_traversal(
                self.root,
                node_attrs={"state": PipelineStateEnum.AWAITING_PROCESSING},
            )
            + self.reverse_in_order_traversal(
                self.root,
                node_attrs={"state": PipelineStateEnum.PROCESSING},
            )
        )
