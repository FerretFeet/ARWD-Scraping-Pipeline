"""Integration test start with html, into extract_parse_content, into transformer"""

import pytest

from src.data_pipeline.extract.webcrawler import Crawler
from src.data_pipeline.transform.pipeline_transformer import PipelineTransformer
from tests.configs.selector_config import SELECTOR_TESTS

param_list = [
    (_selector_info, param)
    for _selector_info in SELECTOR_TESTS
    for param in _selector_info["fixture_params"]
]


@pytest.mark.xfail(
    reason="Not all transformers yet implemented",
)  # Until all selectors have a transformer
@pytest.mark.parametrize(
    ("selector_info", "html_selector_fixture"),
    param_list,
    indirect=["html_selector_fixture"],
)
def test_extract_to_transform_expected_output(selector_info, html_selector_fixture):
    """Generalized tests for all selectors."""
    filename = html_selector_fixture["filename"]

    selector_class = selector_info["selector_class"]
    transformer_dict = selector_info["transformer_dict"]

    selector = selector_class("/stem/")
    crawler = Crawler("")

    extraction_result = crawler.get_content(selector, filename)
    assert extraction_result is not None

    transformer = PipelineTransformer()
    transformed_result = transformer.transform_content(transformer_dict, extraction_result)
    assert transformed_result is not None

    assert len(transformed_result) >= len(extraction_result)
    for val in transformed_result.values():
        assert isinstance(val, (str, int, float, list)) or val is None
