"""Selector template for Arkleg.state.ar.us/Legislators/List."""
from src.data_pipeline.extract.fetching_templates.arkleg_fetchers import registry
from src.data_pipeline.transform.utils.empty_transform import empty_transform
from src.models.selector_template import SelectorTemplate
from src.structures.registries import PipelineRegistries, PipelineRegistryKeys


@registry.register(PipelineRegistryKeys.LEGISLATOR_LIST, PipelineRegistries.PROCESS)
class LegislatorListSelector(SelectorTemplate):
    """Selector for Arkleg Legislator List Page."""

    def __init__(self) -> None:
        """Initialize the selector template."""
        super().__init__(
            selectors={
                "legislator_link": ((
                    "div#tableDataWrapper div.row.tableRow a:first-child, div#tableDataWrapper "
                    "div.row.tableRowAlt a:first-child",
                    "href",
                ), empty_transform),
            },
        )
