import json
import threading
from collections import OrderedDict
from pathlib import Path
from typing import Any
from urllib.parse import unquote

from urllib3.util import parse_url

from src.config.pipeline_enums import PipelineRegistryKeys
from src.structures.indexed_tree import PipelineStateEnum
from src.utils.logger import logger
from src.utils.strings.normalize_url import normalize_url


class Node:
    """Node for IndexedTree."""

    id_counter = 1

    def __init__(  # noqa: PLR0913
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

        self.lock = threading.Lock()

        # Handle state input as Int (from JSON) or Enum
        if isinstance(state, int):
            self.state = PipelineStateEnum(state)
        else:
            self.state = state

    def set_state(self, state: PipelineStateEnum) -> None:
        with self.lock:
            self.state = state
    def add_container(self, container_ref: Any) -> None:
        with self.lock:
            self.container = container_ref

    def add_incoming(self, incoming_ref: Any) -> None:
        with self.lock:
            self.incoming.add(incoming_ref)

    def add_outgoing(self, outgoing_ref: Any) -> None:
        with self.lock:
            self.outgoing.add(outgoing_ref)

    def remove_container(self, container_ref: Any) -> None:
        if self.container == container_ref:
            with self.lock:
                self.container = None

    def remove_incoming(self, incoming_ref: Any) -> None:
        with self.lock:
            self.incoming.remove(incoming_ref)

    def remove_outgoing(self, outgoing_ref: Any) -> None:
        with self.lock:
            self.outgoing.remove(outgoing_ref)

    def _compare(self, val1, val2):
        """Compare two values safely, with partial URL match."""
        if isinstance(val1, str) and isinstance(val2, str):
            # Treat strings starting with "/" or "http" as URLs
            if val1.startswith("http") or val2.startswith("/"):
                return normalize_url(val2) in normalize_url(val1)
            return val1 == val2
        if isinstance(val1, (int, float, bool)):
            return val1 == val2
        # Fallback for iterables
        try:
            return val2 in val1
        except TypeError:
            return val1 == val2


    def _isMatch(
        self,
        *,
        data_attrs: dict | None = None,
        node_attrs: dict | None = None,
    ) -> bool:
        should_visit = True
        if data_attrs:
            for key, value in data_attrs.items():
                if key not in self.data:
                    should_visit = False
                    break
                node_value = self.data[key]
                if value is not None and node_value != value:
                    should_visit = False
                    break
        if node_attrs:
            for key, value in node_attrs.items():
                node_value = getattr(self, key, None)
                print(f"testing match between {value} and {node_value}")

                if value is not None and not self._compare(node_value, value):
                    return False
        print("should visit complete#$########")
        print(should_visit)
        return should_visit


    def to_dict(self) -> dict[str, Any]:
        """Serialize node to a dictionary."""
        return {
            "id": self.id,
            "outgoing_ids": [node.id for node in self.outgoing],
            "incoming_ids": [node.id for node in self.incoming],
            "type": self.type.value, # Store Enum as int
            "url": self.url,
            "data": self.data,
            "state": self.state.value,  # Store Enum as int
            "container": [type(self.container).__name__],
        }

    def __repr__(self) -> str:
        """Represent Node as string."""

        def _shorten(val, max_len=50):
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
    def __init__(self, nodes: list[Node] | None = None, name: str = "Directional Graph") \
            -> None:
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

    def load_node_list(self, nodes: list[Node]):
        for node in nodes:
            self.nodes[node.url] = node


    def reset(self):
        """Reset graph."""
        self.nodes: OrderedDict[str, Node] = OrderedDict()
        self.roots: set[Node] = set()

    def add_new_node(self, url:str, node_type: PipelineRegistryKeys,
                     incoming: list[Node] | None, *, outgoing: list[Node] | None = None,
                     data: dict | None = None,
                     state: PipelineStateEnum = PipelineStateEnum.CREATED,
                     override_id: int | None = None,
                     isRoot: bool = False,
                     ):
        """Create new node and add to graph."""
        new_node = Node(
        node_type,
        url,
        incoming = incoming,
        outgoing = outgoing,
        data = data,
        state = state,
        override_id = override_id,
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

    def add_existing_node(self, node: Node, *, isRoot: bool = False) -> Node:  # noqa: N803
        """Add node to graph."""
        node.add_container(self)
        self.nodes[unquote(node.url)] = node
        if isRoot:
            if self.roots:
                self.roots.add(node)
            else:
                self.roots = {node}
        if len(node.incoming) > 0:
            self.roots.add(node)
        return node

    def find_node_by_url(self, url: str) -> Node | None:
        """Find node by url."""
        return self.nodes.get(unquote(url), None)

    def delete_node(self, node: Node) -> None:
        """Remove node from graph."""
        delnode = self.nodes.pop(unquote(node.url), None)
        if delnode is None: return
        for outnode in list(delnode.outgoing):
            outnode.remove_incoming(delnode)
        for innode in list(delnode.incoming):
            innode.remove_outgoing(delnode)
        if delnode in self.roots:
            self.roots.remove(delnode)
        del delnode

    def cleanup(self) -> None:
        """Clean up graph. Remove orphaned nodes."""
        copied_nodes = self.nodes.copy().values()
        for node in copied_nodes:
            if len(node.outgoing) == 0 and len(node.incoming) == 0:
                self.delete_node(node)

    def _propagate_completion(self, node: Node) -> bool:
        """
        Recursively checks child states, propagates completion to the parent node,
        and removes the node if it's fully completed and has no remaining children.

        Returns True if the node was safely removed, False otherwise.
        """
        # Check if the node is awaiting children (meaning its local work is done)
        if (
            node.state == PipelineStateEnum.AWAITING_CHILDREN
            or node.set_state(PipelineStateEnum.COMPLETED)
        ):
            # Iterate over a copy because we might modify node.outgoing
            children_to_remove = []
            is_subtree_clean = True

            for child in list(node.outgoing):
                # Recursively check the child node
                if self._propagate_completion(child):
                    # If the child and its entire subtree were removed, flag it for parent removal
                    children_to_remove.append(child)
                else:
                    # If the child is not complete OR its subtree is still busy, stop propagation
                    is_subtree_clean = False

            # 2. Cleanup Outgoing Links
            for child in children_to_remove:
                with node.lock:
                    node.remove_outgoing(child)
                # Remove the incoming link on the child from the parent (if your graph requires it)
                # child.incoming.remove(node) # Assuming this is required in your Node class

            # 3. State Propagation (Self-Completion)
            if is_subtree_clean:
                # All children are gone (or were already gone)
                if node.state == PipelineStateEnum.AWAITING_CHILDREN:
                    node.set_state(PipelineStateEnum.COMPLETED)  # Mark parent as completed

                # 4. Final Removal Check (Only happens if self is COMPLETED and has no more outgoing links)
                if node.state == PipelineStateEnum.COMPLETED and not node.outgoing:
                    # Actual node deletion from the graph's main 'nodes' structure
                    self.state.remove_node(node)  # Assumes your DirectionalGraph has this method
                    return True  # Node successfully removed

        # If state isn't AWAITING_CHILDREN/COMPLETED or subtree is not clean
        return False

    def safe_delete_root(self, root_url: str) -> bool:
        """
        Finds the root node associated with the URL and triggers recursive cleanup.

        If the entire subtree is complete and removed, the root key is removed from
        the Orchestrator's internal roots tracking.
        """
        parsed_netloc = parse_url(root_url).netloc
        domain_key = unquote(parsed_netloc)

        root_node = domain_key if domain_key in self.roots else None

        if root_node is None:
            return False

        if self._propagate_completion(root_node):

            node = self.roots.pop(domain_key)
            del node
            print(f"Successfully deleted entire graph subtree for domain: {domain_key}")
            return True

        return False


    def find_in_graph(self, data_attrs: dict | None = None,
                      node_attrs: dict | None = None) -> Node | None:
        """Find node by node attrs or node data attrs."""
        result = [node for node in self.nodes.values()
                  if node._isMatch(data_attrs=data_attrs, node_attrs=node_attrs)]  # noqa: SLF001
        return result[0] if result else None

    def search_ancestors(self, node: Node, data_attrs: dict | None = None,
                         node_attrs: dict | None = None):
        return self._directional_search(node, data_attrs, node_attrs, searchUp=True)

    def search_descendants(self, node: Node, data_attrs: dict | None = None,
                           node_attrs: dict | None = None):
        return self._directional_search(node, data_attrs, node_attrs, searchUp=False)

    def _directional_search(self, node: Node, data_attrs: dict | None = None,
                           node_attrs: dict | None = None, *,
                            searchUp: bool = True,  # noqa: N803
                           searched_nodes: set[Node] | None = None):

        searched_nodes = searched_nodes or set()
        if node in searched_nodes: return None

        if node._isMatch(data_attrs=data_attrs, node_attrs=node_attrs):  # noqa: SLF001
            return node

        searched_nodes.add(node)
        next_search = node.incoming if searchUp else node.outgoing
        for next_node in next_search:
            result = self._directional_search(next_node, data_attrs, node_attrs,
                                           searched_nodes=searched_nodes, searchUp=searchUp)
            if result:
                return result
        return None

    def to_JSON(self) -> str:  # noqa: N802
        """Serialize IndexedTree to a JSON string."""
        data = {
            "name": self.name,
            "nodes": [node.to_dict() for node in self.nodes.values()],
        }
        return json.dumps(data, indent=4)


    def from_JSON(self, json_str: str) -> None:  # noqa: N802
        """Load IndexedTree from a JSON string."""
        data = json.loads(json_str)
        self.nodes.clear()
        self.name = data.get("name", "Directional Graph")

        node_map = {}
        temp_links = []

        # Pass 1: Create all Nodes (without links)
        for node_data in data["nodes"]:
            new_node = Node(
                node_type=node_data["node_type"],
                url=node_data["url"],
                data=node_data["data"],
                state=node_data["state"],
                override_id=node_data["id"],
                # Don't pass incoming/outgoing here
            )
            self.nodes[new_node.url] = new_node
            node_map[new_node.id] = new_node

            # Store links to process in Pass 2
            temp_links.append(
                {
                    "node": new_node,
                    "out_ids": node_data["outgoing_ids"],
                    "in_ids": node_data["incoming_ids"],
                },
            )

        # Pass 2: Re-establish links using ID lookup
        for item in temp_links:
            current_node = item["node"]

            for out_id in item["out_ids"]:
                if out_id in node_map:
                    current_node.add_outgoing(node_map[out_id])

            for in_id in item["in_ids"]:
                if in_id in node_map:
                    current_node.add_incoming(node_map[in_id])

        # Restore global counter
        if self.nodes:
            max_id = max(node.id for node in self.nodes.values())
            Node.id_counter = max_id + 1


    def save_file(self, filepath: Path) -> None:
        """Save tree to a file."""
        filepath.parent.mkdir(parents=True, exist_ok=True)
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
        if self.nodes:
            logger.info(f"Tree loaded from {filepath}")
            return 1
        return None

    def __repr__(self) -> str:
        """Represent IndexedTree as a string."""
        return (
            f"IndexedTree(Nodes: {len(self.nodes)}, "
            f"Name: {self.name if self.name else 'None'})"
        )


