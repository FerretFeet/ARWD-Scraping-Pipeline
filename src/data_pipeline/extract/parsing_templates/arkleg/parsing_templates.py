"""Selector templates for Arkleg.state.ar.us."""
from src.data_pipeline.transform.utils.normalize_str import normalize_str
from src.models.selector_template import SelectorTemplate


class CommitteeSelector(SelectorTemplate):
    """Selector for Arkleg Legislator List Page."""

    def __init__(self) -> None:
        """Initialize the selector template."""
        super().__init__(
            selectors={
                "name": ((
                    "h1",
                ), normalize_str),
            },
        )
