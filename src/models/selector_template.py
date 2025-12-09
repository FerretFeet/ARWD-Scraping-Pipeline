"""SelectorTemplate class."""

from collections.abc import Callable
from dataclasses import dataclass

from bs4 import BeautifulSoup

from src.structures import directed_graph

Selector = str | tuple[str, str] | Callable[[BeautifulSoup], list[str] | None]


@dataclass
class SelectorTemplate:
    """SelectorTemplate class for parsing beautiful soup objects."""

    selectors: dict[str, Selector]
    def __init___(self, selectors: dict[str, Selector]) -> None:
        """Initialize the selector template."""
        self.selectors = selectors

    def copy(self):
        """Return a copy of the selector template."""
        return self.selectors.copy()

    def get_dynamic_state(self, node: directed_graph.Node,
                          state: directed_graph.DirectionalGraph,
                          data_attrs: dict | None, node_attrs: dict | None) -> directed_graph.Node | None:
        """
        Retrieve state for the node from the state_tree dynamically.

        This function can be customized to fetch values based on node type or state_key.
        """
        print(f"in get dynamic state {node}, \n\nnode_attrs {node_attrs}")
        print(f"\n\n\n State is {state}")
        print(f"STATE NODES IS {state.nodes}")
        return state.find_in_graph(data_attrs, node_attrs)

