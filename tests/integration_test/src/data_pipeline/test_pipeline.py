# ruff: noqa: ERA001
# import pytest
#
# from src.data_pipeline.extract.webcrawler import Crawler
# from src.data_pipeline.transform.pipeline_transformer import PipelineTransformer
# from tests.selector_config import SELECTOR_TESTS
#
# param_list = [
#     (_selector_info, param)
#     for _selector_info in SELECTOR_TESTS
#     for param in _selector_info["fixture_params"]
# ]
#
# @pytest.mark.parametrize(
#     ("pipeline_info","html_selector_fixture"),
#     param_list,
#     indirect=["html_selector_fixture"],  # <-- tell pytest this param is for a fixture
# )
# def test_selector_success_all(pipeline_info, html_selector_fixture):
#     """Generalized tests for all selectors."""
#     filename = html_selector_fixture["filename"]
#
#     selector_class = pipeline_info["selector_class"]
#     required_keys = pipeline_info["required_keys"]
#
#     selector = selector_class("/stem/")
#     crawler = Crawler("")
#
#     result = crawler.get_content(selector, filename)
#     assert result is not None
#
#     keys = ["rel_url"]
#     keys.extend(str(key) for key in selector.selectors.keys())
#     for key in required_keys:
#         assert result[key] is not None
#
#     try:
#         transformer_class = pipeline_info["transformer_class"]
#     except KeyError:
#         transformer_class = None
#     if transformer_class is not None:
#         transformer = PipelineTransformer()
#         transformed_result = transformer.transform_content(transformer_class, result)
#         print(f"\nPRE TRANSFORM \n {result}")
#         print(f"\nTRANSFORMED \n {transformed_result}")
