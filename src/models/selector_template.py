"""SelectorTemplate class."""

from collections.abc import Callable
from dataclasses import dataclass

from bs4 import BeautifulSoup

from src.structures import directed_graph
from src.structures.registries import get_enum_by_url
from src.utils.strings.get_url_base_path import get_url_base_path

Selector = str | tuple[str, str] | Callable[[BeautifulSoup], list[str] | None]


@dataclass
class SelectorTemplate:
    """SelectorTemplate class for parsing beautiful soup objects."""

    selectors: dict[str, Selector]

    def __init___(self, selectors: dict[str, Selector]) -> None:
        """Initialize the selector template."""
        self.selectors = selectors

    def copy(self) -> dict:
        """Return a copy of the selector template."""
        return self.selectors.copy()

    def get_dynamic_state(
        self,
        state: directed_graph.DirectionalGraph,
        data_attrs: dict | None,
        node_attrs: dict | None,
    ) -> directed_graph.Node | None:
        """
        Retrieve state for the node from the state_tree dynamically.

        This function can be customized to fetch values based on node type or state_key.
        """
        return state.find_in_graph(data_attrs, node_attrs)

    def get_dynamic_state_from_parents(
        self,
        node: directed_graph.Node,
        state: directed_graph.DirectionalGraph,
        data_attrs: dict | None,
        node_attrs: dict | None,
    ) -> directed_graph.Node | None:
        """
        Retrieve state for the node from the state_tree dynamically.

        This function can be customized to fetch values based on node type or state_key.
        """
        node = state.search_ancestors(node, data_attrs, node_attrs)
        if not node and node_attrs and "url" in node_attrs:
            type_enum = get_enum_by_url(get_url_base_path(node_attrs["url"]))
            state.add_new_node(node_attrs["url"], type_enum, [node])
