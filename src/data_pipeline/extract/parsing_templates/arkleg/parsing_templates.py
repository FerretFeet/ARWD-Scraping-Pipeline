"""Selector templates for Arkleg.state.ar.us."""
import re

from src.data_pipeline.transform.utils.empty_transform import empty_transform
from src.data_pipeline.transform.utils.normalize_str import normalize_str
from src.models.selector_template import SelectorTemplate
from src.structures.directed_graph import DirectionalGraph, Node


class CommitteeSelector(SelectorTemplate):
    """Selector for Arkleg Legislator List Page."""

    def __init__(self) -> None:
        """Initialize the selector template."""
        super().__init__(
            selectors={
                "name": ((
                    "h1",
                ), normalize_str),
                "state_committee_id": (self.strip_committee_code_from_link, empty_transform),
            },
        )
    def strip_committee_code_from_link(self, node: Node, state_tree: DirectionalGraph, parsed_data: dict):
        """
        Strips and returns the committee code from a URL, based on the parameter 'code'.

        Args:
            url_string (str): The input URL.

        Returns:
            str or None: The extracted code (e.g., '038'), or None if not found.

        """
        url = parsed_data.get("url")
        if not url: return None

        pattern = r"code=([0-9A-Za-z]+)"
        match = re.search(pattern, url)

        if match:
            # Group 1 (index 1) contains the value captured inside the parentheses
            return {"committee_id": match.group(1)}
        return None
