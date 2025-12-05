import json
import threading
from collections import OrderedDict
from pathlib import Path
from typing import Any
from urllib.parse import unquote

from src.config.pipeline_enums import PipelineRegistryKeys
from src.structures.indexed_tree import PipelineStateEnum
from src.utils.logger import logger


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
        self.container = {container}

        self.lock = threading.Lock()

        # Handle state input as Int (from JSON) or Enum
        if isinstance(state, int):
            self.state = PipelineStateEnum(state)
        else:
            self.state = state

    def add_container(self, container_ref: Any) -> None:
        self.container.add(container_ref)

    def add_incoming(self, incoming_ref: Any) -> None:
        self.incoming.add(incoming_ref)

    def add_outgoing(self, outgoing_ref: Any) -> None:
        self.outgoing.add(outgoing_ref)

    def remove_container(self, container_ref: Any) -> None:
        self.container.remove(container_ref)

    def remove_incoming(self, incoming_ref: Any) -> None:
        self.incoming.remove(incoming_ref)

    def remove_outgoing(self, outgoing_ref: Any) -> None:
        self.outgoing.remove(outgoing_ref)

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
                if key not in self.__dict__:
                    should_visit = False
                    break
                node_value = getattr(self, key)
                if value is not None and node_value != value:
                    should_visit = False
                    break
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
            "container": [type(container).__name__ for container in self.container],
        }

    def __repr__(self) -> str:
        """Represent Node as string."""
        return (
            f"\tNode(id={self.id}, state={self.state.name}, "
            f"children={[child.id for child in self.outgoing] if self.outgoing else []}), "
            f"incoming={[incoming.id for incoming in self.incoming] if self.incoming else []}, "
            f"type={self.type}, url={self.url}), "
            f"container={[type(con) for con in self.container] if self.container else None}, )"
            f"data={self.data}, "
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
        if nodes is not None:
            self.load_node_list(nodes)

    def load_node_list(self, nodes: list[Node]):
        for node in nodes:
            self.nodes[node.url] = node


    def reset(self):
        """Reset graph."""
        self.nodes: OrderedDict[str, Node] = OrderedDict()

    def add_new_node(self, url:str, node_type: PipelineRegistryKeys,
                     incoming: list[Node] | None, *, outgoing: list[Node] | None = None,
                     data: dict | None = None,
                     state: PipelineStateEnum = PipelineStateEnum.CREATED,
                     override_id: int | None = None,
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
                inlink = self.find_node(link.url)
                if inlink is not None:
                    inlink.add_outgoing(new_node)
        if outgoing is not None:
            for link in outgoing:
                outlink = self.find_node(link.url)
                if outlink is not None:
                    outlink.add_incoming(new_node)

        self.add_existing_node(new_node)

    def add_existing_node(self, node: Node) -> None:
        """Add node to graph."""
        node.add_container(self)
        self.nodes[unquote(node.url)] = node

    def find_node(self, url: str) -> Node | None:
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
        del delnode

    def cleanup(self) -> None:
        """Clean up graph. Remove orphaned nodes."""
        copied_nodes = self.nodes.copy().values()
        for node in copied_nodes:
            if len(node.outgoing) == 0 and len(node.incoming) == 0:
                self.delete_node(node)


    def find_in_graph(self, url: str, data_attrs: dict | None = None,
                      node_attrs: dict | None = None) -> list[Node] | None:
        """Find node by node attrs or node data attrs."""
        result = [node for node in self.nodes.values()
                  if node._isMatch(data_attrs=data_attrs, node_attrs=node_attrs)]  # noqa: SLF001
        return result

    def search_ancestors(self, node: Node, data_attrs: dict | None = None,
                         node_attrs: dict | None = None, *,
                         searched_nodes: set[Node] | None = None):
        searched_nodes = searched_nodes or set()
        if node in searched_nodes: return None
        if node._isMatch(data_attrs=data_attrs, node_attrs=node_attrs):  # noqa: SLF001
            return node
        searched_nodes.add(node)
        for parent in node.incoming:
            result = self.search_ancestors(parent, data_attrs, node_attrs,
                                           searched_nodes=searched_nodes)
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


