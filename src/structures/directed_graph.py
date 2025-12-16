"""Thread-safe directed graph and Node object."""

import json
import threading
from collections import OrderedDict
from html import unescape
from pathlib import Path
from typing import Any
from urllib.parse import unquote, urlparse

from urllib3.util import parse_url

from src.config.pipeline_enums import PipelineRegistryKeys
from src.structures.indexed_tree import PipelineStateEnum
from src.utils.logger import logger
from src.utils.strings.normalize_url import normalize_url


class Node:
    """Node for IndexedTree."""

    id_counter = 1

    def __init__(
        self,
        node_type: PipelineRegistryKeys,
        url: str | None,
        *,
        incoming: set["Node"] | None = None,
        outgoing: set["Node"] | None = None,
        data: dict | None = None,
        state: PipelineStateEnum = PipelineStateEnum.CREATED,
        override_id: int | None = None,
        container: Any = None,
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

        self.outgoing = outgoing or set()
        self.incoming = incoming or set()
        self.data = data or {}
        self.url = unquote(url)
        self.type = node_type
        self.container = container

        # self.lock = threading.Lock()

        # Handle state input as Int (from JSON) or Enum
        if isinstance(state, int):
            self.state = PipelineStateEnum(state)
        else:
            self.state = state

    def set_state(self, state: PipelineStateEnum) -> None:
        """Set state."""
        # with self.lock:
        self.state = state

    def add_container(self, container_ref: Any) -> None:
        """Add/Replace container reference."""
        # with self.lock:
        self.container = container_ref

    def add_incoming(self, incoming_ref: Any) -> None:
        """Add incoming reference."""
        # with self.lock:
        self.incoming.add(incoming_ref)

    def add_outgoing(self, outgoing_ref: Any) -> None:
        """Add outgoing reference."""
        # with self.lock:
        self.outgoing.add(outgoing_ref)

    def remove_container(self, container_ref: Any) -> None:
        """Remove container reference."""
        if self.container == container_ref:
            # with self.lock:
            self.container = None

    def remove_incoming(self, incoming_ref: Any) -> None:
        """Remove incoming reference."""
        self.incoming.remove(incoming_ref)

    def remove_outgoing(self, outgoing_ref: Any) -> None:
        """Remove outgoing reference."""
        self.outgoing.remove(outgoing_ref)

    def _compare(self, val1: Any, val2: Any) -> bool:
        """Compare two values safely, with partial URL match."""
        if isinstance(val1, str) and isinstance(val2, str):
            # Treat strings starting with "/" or "http" as URLs
            if val1.startswith("http") or val2.startswith("/"):
                nval2 = normalize_url(val2)
                nval1 = normalize_url(val1)
                return nval2 in nval1
            return val1 == val2
        if isinstance(val1, (int, float, bool)):
            return val1 == val2
        # Fallback for iterables
        try:
            return val2 in val1
        except TypeError:
            return val1 == val2

    def isMatch(
        self,
        *,
        data_attrs: dict | None = None,
        node_attrs: dict | None = None,
    ) -> bool:
        """Check if this node matches the given attrs."""
        should_visit = True
        if data_attrs:
            for key, value in data_attrs.items():
                if key not in self.data:
                    should_visit = False
                    return False
                    break
                node_value = self.data[key]
                if value is not None and node_value != value:
                    should_visit = False
                    return False
        if should_visit is False:
            return False
        if node_attrs:
            for key, value in node_attrs.items():
                node_value = getattr(self, key, None)

                if value is not None and not self._compare(node_value, value):
                    return False
        return should_visit

    def to_dict(self) -> dict[str, Any]:
        """Serialize node to a dictionary."""
        return {
            "id": self.id,
            "outgoing_ids": [node.id for node in self.outgoing],
            "incoming_ids": [node.id for node in self.incoming],
            "type": self.type.value,  # Store Enum as int
            "url": self.url,
            "data": self.data,
            "state": self.state.value,  # Store Enum as int
            "container": [type(self.container).__name__],
        }

    def __repr__(self) -> str:
        """Represent Node as string."""

        def _shorten(val: Any, max_len: int = 250) -> str:
            """Return a truncated string representation if too long."""
            s = str(val)
            if len(s) > max_len:
                return s[:max_len] + "…"
            return s

        data_repr = {k: _shorten(v) for k, v in self.data.items()} if self.data else None
        return (
            f"\tNode(id={self.id}, state={self.state.name}, "
            f"children={[child.id for child in self.outgoing] if self.outgoing else []}), "
            f"incoming={[incoming.id for incoming in self.incoming] if self.incoming else []}, "
            f"type={self.type}, url={self.url}), "
            f"container={[type(self.container)] if self.container else None}, )"
            f"data={data_repr}, "
        )


class DirectionalGraph:
    """
    Thread-safe directed graph.

    Assumes a node with a state: PipelineStateEnum and a .data attr.
    """

    def __init__(self, nodes: list[Node] | None = None, name: str = "Directional Graph") -> None:
        """
        Initialize Directional Graph.

        Args:
            nodes (list[Node] | None): List of nodes to add to graph
            name (str): Name of graph

        """
        self.name = name
        self.nodes: OrderedDict[str, Node] = OrderedDict()
        self.roots: set[Node] = set()
        if nodes is not None:
            self.load_node_list(nodes)
        self.lock = threading.RLock()

    def add_node(self, node: Node) -> None:
        """Add node to nodes list."""
        with self.lock:
            self.nodes.update({unquote(unescape(node.url)): node})

    def set_nodes(self, nodes: OrderedDict[str, Node] | None) -> None:
        """Set all nodes using an ordered dict of [url, Node]."""
        with self.lock:
            self.nodes = nodes

    def get_nodes(self) -> OrderedDict:
        """Get all nodes."""
        with self.lock:
            return self.nodes.copy()

    def remove_node(self, node: Node) -> Node:
        """Remove node from nodes dict."""
        with self.lock:
            return self.nodes.pop(node.url)

    def get_roots(self) -> set[Node]:
        """Get seed urls."""
        with self.lock:
            return self.roots.copy()

    def remove_root(self, node: Node) -> None:
        """Remove seed url."""
        with self.lock:
            return self.roots.remove(node)

    def add_root(self, node: Node) -> None:
        """Add seed url."""
        with self.lock:
            self.roots.add(node)

    def set_root(self, nodes: set[Node] | None) -> None:
        """Set seed url."""
        with self.lock:
            self.roots = nodes

    def load_node_list(self, nodes: list[Node]) -> None:
        """Input a list of nodes into the graph."""
        with self.lock:
            for node in nodes:
                self.nodes[node.url] = node

    def reset(self) -> None:
        """Reset graph."""
        with self.lock:
            self.nodes = OrderedDict()
            self.roots = set()

    def add_new_node(
        self,
        url: str,
        node_type: PipelineRegistryKeys,
        incoming: list[Node] | None,
        *,
        outgoing: list[Node] | None = None,
        data: dict | None = None,
        state: PipelineStateEnum = PipelineStateEnum.CREATED,
        override_id: int | None = None,
        isRoot: bool = False,
    ) -> Node | None:
        """Create new node and add to graph."""
        new_node = Node(
            node_type,
            url,
            incoming=incoming,
            outgoing=outgoing,
            data=data,
            state=state,
            override_id=override_id,
        )
        if incoming is not None:
            for link in incoming:
                inlink = self.find_node_by_url(link.url)
                if inlink is not None:
                    inlink.add_outgoing(new_node)
        if outgoing is not None:
            for link in outgoing:
                outlink = self.find_node_by_url(link.url)
                if outlink is not None:
                    outlink.add_incoming(new_node)

        return self.add_existing_node(new_node, isRoot=isRoot)

    def set_node_state(self, node: Node, state: PipelineStateEnum) -> None:
        """Set node state."""
        with self.lock:
            node.set_state(state)

    def add_existing_node(self, node: Node, *, isRoot: bool = False) -> Node | None:
        """Add node to graph."""
        node.add_container(self)
        with self.lock:
            if unquote(unescape(node.url)) in self.nodes:
                return None
            self.nodes.update({unquote(unescape(node.url)): node})
            if isRoot:
                if self.roots:
                    self.roots.add(node)
                else:
                    self.roots = {node}
            if len(node.incoming) == 0 and node not in self.roots:
                self.roots.add(node)
            return node

    def find_node_by_url(self, url: str) -> Node | None:
        """Find node by url."""
        return self.nodes.get(unquote(url), None)

    def delete_node(self, node: Node) -> None:
        """Remove node from graph."""
        with self.lock:
            if not self.nodes.get(unquote(unescape(node.url))):
                return
            delnode = self.nodes.pop(unquote(unescape(node.url)))
            if delnode is None:
                return
            if delnode in self.roots:
                self.roots.remove(delnode)
            for outnode in list(delnode.outgoing):
                outnode.remove_incoming(delnode)
            for innode in list(delnode.incoming):
                innode.remove_outgoing(delnode)
            del delnode

    def cleanup(self) -> None:
        """Clean up graph. Remove orphaned nodes."""
        with self.lock:
            copied_nodes = self.get_nodes().values()
            for node in copied_nodes:
                if len(node.outgoing) == 0 and len(node.incoming) == 0:
                    self.delete_node(node)

    def _propagate_completion(self, node: Node) -> bool:
        """
        Recursively checks child states and updates the current node's state
        if all necessary children have COMPLETED their work.

        Returns:
            True: If the node's state is now COMPLETED.
            False: If the node is still busy, either locally or because a descendant is busy.

        """  # noqa: D205
        if node.state not in (PipelineStateEnum.AWAITING_CHILDREN, PipelineStateEnum.COMPLETED):
            return False

        if node.state == PipelineStateEnum.COMPLETED:
            return True

        is_subtree_clean = True
        with self.lock:
            for child in node.outgoing:
                child_is_completed = self._propagate_completion(child)

                if not child_is_completed:
                    is_subtree_clean = False
                    break
            if is_subtree_clean:
                # All children and their descendants are COMPLETED.
                node.set_state(PipelineStateEnum.COMPLETED)

                return True

            return False

    def safe_remove_root(self, root_url: str, known_roots_cache_file: Path | None) -> bool:
        """
        Find the root node associated with the URL and triggers recursive cleanup.

        If the entire subtree is complete and removed, the root key is removed from
        the Orchestrator's internal roots tracking.
        """
        parsed_netloc = parse_url(root_url).netloc
        domain_key = unquote(parsed_netloc)
        root_node = None
        with self.lock:
            for node in self.roots:
                if unquote(urlparse(node.url).netloc).strip() == domain_key.strip():
                    root_node = node
                    break

            if root_node is None:
                return False

            if self._propagate_completion(root_node):
                # True only if all nodes are able to be marked completed
                self.remove_root(root_node)
                self.propogate_downward_deletion(root_node)
                if known_roots_cache_file:
                    self.save_completed_root_url(root_node.url, known_roots_cache_file)
                self.delete_node(root_node)
                logger.info(f"deleting entire graph subtree for domain: {domain_key}")
                return True
            return False

    def propogate_downward_deletion(self, node: Node) -> None:
        """Recursively delete all outgoing nodes."""
        if node.outgoing:
            with self.lock:
                for outnode in node.outgoing.copy():
                    self.propogate_downward_deletion(outnode)
        self.delete_node(node)

    def find_in_graph(
        self,
        data_attrs: dict | None = None,
        node_attrs: dict | None = None,
        *,
        find_single: bool = True,
    ) -> Node | None | list[Node]:
        """Find node by node attrs or node data attrs."""
        result = [
            node
            for node in self.get_nodes().values()
            if node.isMatch(data_attrs=data_attrs, node_attrs=node_attrs)
        ]
        if result:
            return result[0] if find_single else result
        return None

    def search_ancestors(
        self,
        node: Node,
        data_attrs: dict | None = None,
        node_attrs: dict | None = None,
    ) -> Node | None:
        """Search recursively through incoming nodes."""
        return self._directional_search(node, data_attrs, node_attrs, searchUp=True)

    def search_descendants(
        self,
        node: Node,
        data_attrs: dict | None = None,
        node_attrs: dict | None = None,
    ) -> Node | None:
        """Search recursively through outgoing nodes."""
        return self._directional_search(node, data_attrs, node_attrs, searchUp=False)

    def _directional_search(
        self,
        node: Node,
        data_attrs: dict | None = None,
        node_attrs: dict | None = None,
        *,
        searchUp: bool = True,
        searched_nodes: set[Node] | None = None,
    ) -> Node | None:

        searched_nodes = searched_nodes or set()
        if node in searched_nodes:
            return None

        if node.isMatch(data_attrs=data_attrs, node_attrs=node_attrs):
            return node

        searched_nodes.add(node)
        next_search = node.incoming if searchUp else node.outgoing
        for next_node in next_search:
            result = self._directional_search(
                next_node,
                data_attrs,
                node_attrs,
                searched_nodes=searched_nodes,
                searchUp=searchUp,
            )
            if result:
                return result
        return None

    def to_JSON(self) -> str:
        """Serialize DirectionalGraph to a JSON string."""

        def _json_default(obj: Any) -> Any:
            """Convert DirectionalGraph to JSON string."""
            if hasattr(obj, "isoformat"):
                return obj.isoformat()
            return str(obj)

        data = {
            "name": self.name,
            # store nodes as list of dicts
            "nodes": [node.to_dict() for node in self.nodes.values()],
            # store roots as list of node ids (simpler to restore)
            "roots": [node.id for node in self.roots],
        }
        return json.dumps(data, indent=4, default=_json_default)

    def from_JSON(self, json_str: str) -> None:  # noqa: C901
        """Load graph from JSON string (replaces current graph)."""
        data = json.loads(json_str)
        # clear current state
        self.nodes.clear()
        self.roots = set()
        self.name = data.get("name", "Directional Graph")

        node_map: dict[int, Node] = {}
        temp_links = []

        nodes_data = data.get("nodes", [])

        # Pass 1: Create nodes (no links)
        for node_data in nodes_data:
            # Convert stored int type into PipelineRegistryKeys enum
            raw_type = node_data["type"]
            try:
                # if stored as int
                node_type_enum = PipelineRegistryKeys(raw_type)
            except Exception:  # noqa: BLE001
                # if it's already an enum value or string, try to handle gracefully
                try:
                    node_type_enum = PipelineRegistryKeys(node_data["type"])
                except Exception:  # noqa: BLE001
                    # fallback: leave as-is (but this is not ideal)
                    node_type_enum = node_data["type"]

            new_node = Node(
                node_type=node_type_enum,
                url=node_data.get("url"),
                data=node_data.get("data", {}),
                state=node_data.get("state", PipelineStateEnum.CREATED),
                override_id=node_data.get("id"),
            )
            # Use normalized url key to match add_existing_node/getters
            self.nodes[unquote(unescape(new_node.url))] = new_node
            node_map[new_node.id] = new_node

            temp_links.append(
                {
                    "node": new_node,
                    "out_ids": node_data.get("outgoing_ids", []),
                    "in_ids": node_data.get("incoming_ids", []),
                },
            )

        # Pass 2: Re-establish links (incoming/outgoing sets)
        for item in temp_links:
            current_node: Node = item["node"]
            for out_id in item["out_ids"]:
                if out_id in node_map:
                    current_node.add_outgoing(node_map[out_id])
            for in_id in item["in_ids"]:
                if in_id in node_map:
                    current_node.add_incoming(node_map[in_id])

        # Restore roots by id list if present
        root_ids = data.get("roots", [])
        for rid in root_ids:
            if rid in node_map:
                self.roots.add(node_map[rid])

        # Restore Node.id_counter to avoid ID collisions
        if self.nodes:
            Node.id_counter = max(node.id for node in self.nodes.values()) + 1

    def save_completed_root_url(self, nodeurl: str, filepath: Path) -> None:
        """Append deleted root url to a file."""
        filepath.parent.mkdir(parents=True, exist_ok=True)
        try:
            # open through the Path instance
            with filepath.open("a", encoding="utf-8") as f:
                f.write(nodeurl + "\n")
            logger.info(f"Deleted root url saved to {filepath}")
        except OSError as e:
            logger.error(f"Error saving file: {e}")

    def save_file(self, filepath: Path) -> None:
        """Save tree to a file."""
        with self.lock:
            filepath.parent.mkdir(parents=True, exist_ok=True)
            try:
                with filepath.open("w", encoding="utf-8") as f:
                    f.write(self.to_JSON())
                logger.info(f"Tree saved to {filepath}")
            except OSError as e:
                logger.error(f"Error saving file: {e}")

    def load_from_file(self, filepath: Path) -> None | int:
        """Load tree from a file. Returns 1 on success, None on failure."""
        with self.lock:
            if not filepath.exists():
                msg = f"{filepath} does not exist."
                logger.warning(msg)
                return None

            try:
                with filepath.open("r", encoding="utf-8") as f:
                    json_str = f.read()
            except OSError as e:
                logger.error(f"Error reading file {filepath}: {e}")
                return None

            self.from_JSON(json_str)
            if self.nodes:
                logger.info(
                    f"Tree loaded from {filepath} with {len(self.nodes)} nodes,"
                    f" {len(self.roots)} roots FROM JSON"
                    f"\nroots: {self.roots}",
                )
                return 1
            return None

    def __repr__(self) -> str:
        """Represent IndexedTree as a string."""
        return f"IndexedTree(Nodes: {len(self.nodes)}, Name: {self.name if self.name else 'None'})"
