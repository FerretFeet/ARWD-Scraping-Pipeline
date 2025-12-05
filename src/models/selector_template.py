"""SelectorTemplate class."""

from collections.abc import Callable
from dataclasses import dataclass

from bs4 import BeautifulSoup

from src.structures import indexed_tree

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

    def get_dynamic_state(self, node: indexed_tree.Node, state_tree: indexed_tree.IndexedTree,
                          data_attrs: dict | None, node_attrs: dict | None) -> dict | None:
        """
        Retrieve state for the node from the state_tree dynamically.

        This function can be customized to fetch values based on node type or state_key.
        """
        # Example logic: walk up the tree to find the relevant state
        return state_tree.find_val_in_ancestor(node, data_attrs, node_attrs)
