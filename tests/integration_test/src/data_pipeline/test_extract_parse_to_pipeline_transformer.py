# """Integration test start with html, into extract_parse_content, into transformer"""
#
# from datetime import datetime
# from urllib.parse import urlparse
#
# import pytest
#
# from src.config.pipeline_enums import PipelineRegistries
# from src.config.settings import PIPELINE_REGISTRY
# from src.data_pipeline.extract.html_parser import HTMLParser
# from src.data_pipeline.transform.pipeline_transformer import PipelineTransformer
# from src.structures.registries import get_enum_by_url
# from tests.configs.selector_config import SELECTOR_TESTS
#
# param_list = [
#     (_selector_info, param)
#     for _selector_info in SELECTOR_TESTS
#     for param in _selector_info["fixture_params"]
# ]
#
#
#
# def split_process_dict(original_templates: dict) -> tuple[dict, dict]:
#
#     parsing_templates = original_templates
#
#     state_key_pair = parsing_templates.pop("state", (None, None))
#
#     # Separate selectors and transformers from parsing templates
#     selector_template = {key: val[0] for key, val in parsing_templates.items()}
#     transformer_template = {key: val[1] for key, val in parsing_templates.items()}
#
#     return selector_template, transformer_template
#
#
# @pytest.mark.parametrize(
#     ("selector_info", "html_selector_fixture"),
#     param_list,
#     indirect=["html_selector_fixture"],
# )
# def test_extract_parse_to_transform_expected_output(selector_info, html_selector_fixture):
#     """Generalized tests for all selectors."""
#     registry = PIPELINE_REGISTRY
#
#     filename = html_selector_fixture["filename"]
#     url = html_selector_fixture["url"]
#     html = html_selector_fixture["html"]
#     variant = html_selector_fixture["variant"]
#     isFetch = selector_info["fetch"]
#     isProcess = selector_info["process"]
#     html_parser = HTMLParser()
#     transformer = PipelineTransformer(strict=True)
#
#     parsed_url = urlparse(url)
#     parsed_url = parsed_url.netloc + parsed_url.path
#     url_enum = get_enum_by_url(parsed_url)
#     try:
#         parse_template = registry.get_processor(url_enum, PipelineRegistries.FETCH)
#         content = html_parser.get_content(parse_template.copy(), html)
#         assert content is not None
#
#         for key, value in content.items():
#             assert isinstance(value, (str, int, float, list, datetime, tuple)) or value is None
#             if variant.lower() == "known":
#                 assert value is not None
#     except KeyError as e:
#         if "No processor found for" in str(e):
#             if isFetch: raise
#         else: raise
#
#     try:
#         parse_template = registry.get_processor(url_enum, PipelineRegistries.PROCESS)
#         parse_template, transform_template = split_process_dict(parse_template.copy())
#         content = html_parser.get_content(parse_template, html)
#         assert content is not None
#         for key, value in content.items():
#             assert isinstance(value, (str, int, float, list, datetime, tuple)) or value is None
#             if variant.lower() == "known":
#                 assert value is not None
#
#         transformed_content = transformer.transform_content(transform_template, content)
#         assert transformed_content is not None
#         for key, value in transformed_content.items():
#             assert isinstance(value, (str, int, float, list, datetime, tuple)) or value is None
#             if variant.lower() == "known":
#                 assert value is not None
#     except KeyError as e:
#         if "No processor found for" in str(e):
#             if isProcess: raise
#         else: raise
#
#
#
#
#
#     #
#     # selector_class = selector_info["selector_class"]
#     # transformer_dict = selector_info["transformer_dict"]
#     #
#     #
#     # selector = selector_class("/stem/")
#     # crawler = Crawler("")
#     # parser = HTMLParser
#     # ######
#     # # CHANGE THIS, SELECTOR CLASS NOW HOLDS TRANSFORMER DICT
#     # # THIS FUNCTION WILL SPLIT IT INTO 3 DICTS, PARSER, TRANSFORMER, STATE LOOKUP
#     # # IGNORE STATE LOOKUP FOR THESE TESTS
#     # # TEST WITH FETCH TEMPLATES AND PROCESS TEMPLATES
#     #
#     # #patch crawler.getpage return html
#     # extraction_result = crawler.get_content(selector, filename)
#     #
#     # assert extraction_result is not None
#     #
#     # transformer = PipelineTransformer(strict=True)
#     # transformed_result = transformer.transform_content(transformer_dict, extraction_result)
#     # assert transformed_result is not None
#     #
#     # assert len(transformed_result) >= len(extraction_result)
#     # for val in transformed_result.values():
#     #     assert isinstance(val, (str, int, float, list, datetime, tuple)) or val is None
