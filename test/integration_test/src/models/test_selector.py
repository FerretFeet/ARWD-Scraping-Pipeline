from src.data_pipeline.extract.WebCrawler import Crawler

import pytest

from test.selector_config import SELECTOR_TESTS

# Flatten selector + fixture param combinations
param_list = []
for selector_info in SELECTOR_TESTS:
    for param in selector_info["fixture_params"]:
        param_list.append((selector_info, param))  # param is (name, url, variant)

@pytest.mark.parametrize(
    "selector_info,html_selector_fixture",
    param_list,
    indirect=["html_selector_fixture"]  # <-- tell pytest this param is for a fixture
)
def test_selector_success_all(selector_info, html_selector_fixture):
    """
    Generalized test for all selectors.
    """
    soup = html_selector_fixture["soup"]
    filename = html_selector_fixture["filename"]

    selector_class = selector_info["selector_class"]
    required_keys = selector_info["required_keys"]

    selector = selector_class("/stem/")
    crawler = Crawler("")

    result = crawler.get_content(selector, filename)
    assert result is not None

    keys = ["rel_url"]
    keys.extend(str(key) for key in selector.selectors.keys())

    for key in keys:
        assert key in result.keys()
        if key in required_keys:
            assert result[key] is not None, f'required key {key} is missing'
            assert isinstance(result[key], list), f'expected list for key {key}, got {type(result[key])}'
        elif key == "rel_url":
            assert isinstance(result[key], str), f'expected str for key {key}, got {type(result[key])}'
        else:
            assert result[key] is None or isinstance(result[key], list), f'expected list or none for key {key}, got {type(result[key])}'

    if "committees" in result and "committee_links" in result:
        assert len(result["committees"]) == len(result["committee_links"]), f'Length mismatch between committees and committee_links'
