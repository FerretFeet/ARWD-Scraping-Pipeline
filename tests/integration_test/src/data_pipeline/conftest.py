# ruff: noqa: ERA001
# # conftest.py (or similar)
#
# from typing import Any
#
# import pytest
#
# from src.data_pipeline.extract.webcrawler import Crawler
# from tests.selector_config import SELECTOR_TESTS
#
# param_list = [
#     (_selector_info, param)
#     for _selector_info in SELECTOR_TESTS
#     for param in _selector_info["fixture_params"]
# ]
#
# @pytest.fixture(scope="module", params=param_list) # scope can be function, module, etc.
# def extracted_content(request) -> dict[str, Any]:
#     """
#     Performs the extraction logic and yields the result.
#     The request.param contains the (_selector_info, param) tuple.
#     """
#     selector_info, html_selector_fixture = request.param
#
#     filename = html_selector_fixture["filename"]
#     selector_class = selector_info["selector_class"]
#
#     selector = selector_class("/stem/")
#     crawler = Crawler("")
#
#     result = crawler.get_content(selector, filename)
#
#     # You can move all the assertions from the original test_selector_success_all here
#     # and use the `yield` statement to pass the result to the test function.
#     # We will keep the assertions separate for clarity below.
#
#     return result
