# import pytest
#
# from src.data_pipeline.extract.webcrawler import Crawler
# from tests.configs.selector_config import SELECTOR_TESTS
#
# # Flatten selector + fixture param combinations
#
# param_list = [
#     (_selector_info, param)
#     for _selector_info in SELECTOR_TESTS
#     for param in _selector_info["fixture_params"]
# ]
#
#
# @pytest.mark.parametrize(
#     ("selector_info", "html_selector_fixture"),
#     param_list,
#     indirect=["html_selector_fixture"],
# )
# def test_selector_expected_output(selector_info, html_selector_fixture):
#     """Generalized tests for all selectors."""
#     filename = html_selector_fixture["filename"]
#
#     selector_class = selector_info["selector_class"]
#     required_keys = selector_info["required_keys"]
#
#     selector = selector_class("/stem/")
#     crawler = Crawler("")
#
#     result = crawler.get_content(selector, filename)
#     assert result is not None
#     assert len(result.keys()) >= len(selector.selectors.keys())
#
#     keys = ["rel_url"]
#     keys.extend(str(key) for key in selector.selectors)
#     for key in keys:
#         assert key in result
#         if key in required_keys:
#             assert result[key] is not None, f"required key {key} is missing"
#             assert isinstance(
#                 result[key],
#                 list,
#             ), f"expected list for key {key}, got {type(result[key])}"
#             assert len(result[key]) > 0, f"length of list for key {key} is zero"
#         elif key == "rel_url":
#             assert isinstance(
#                 result[key],
#                 str,
#             ), f"expected str for key {key}, got {type(result[key])}"
#         else:
#             assert result[key] is None or isinstance(
#                 result[key],
#                 list,
#             ), f"expected list or none for key {key}, got {type(result[key])}"
