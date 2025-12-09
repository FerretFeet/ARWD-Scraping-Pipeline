# import re
# from queue import Queue
# from unittest.mock import patch
#
# import pytest
# from bs4 import BeautifulSoup
#
# from src.config.pipeline_enums import PipelineRegistryKeys
# from src.config.settings import PIPELINE_REGISTRY
# from src.data_pipeline.extract import webcrawler
# from src.data_pipeline.extract.html_parser import HTMLParser
# from src.data_pipeline.extract.parsing_templates.arkleg.legislator_selector import (
#     LegislatorSelector,
# )
# from src.data_pipeline.transform.pipeline_transformer import PipelineTransformer
# from src.structures.directed_graph import DirectionalGraph
# from src.utils.paths import project_root
# from src.workers.pipeline_workers import ProcessorWorker
#
#
#
# @pytest.fixture(scope="session")
# def known_legislator_soup_fixture() -> BeautifulSoup:
#     """Load saved HTML fixture for legislators page."""
#     fixture_path = (
#         project_root / "tests" / "fixtures" / "html" / "legislator" / "legislator.known.html"
#     )
#     with fixture_path.open(encoding="utf-8") as f:
#         html = f.read()
#         return BeautifulSoup(html, "html.parser")
#
#
# class TestArStateLegislatorSelector:
#     @patch.object(webcrawler.Crawler, "get_page")
#     def test_known_legislator_known_return(self, mock_get_page, known_legislator_soup_fixture):
#         selector = LegislatorSelector("/stem/")
#         process_worker =
#         crawler = webcrawler.Crawler("")
#         rel_url = "/return/path"
#         mock_get_page.return_value = known_legislator_soup_fixture
#         result = crawler.get_content(selector, rel_url)
#
#         # Address scrapes ugly with lots of newlines and internal spaces
#         result["address"] = [re.sub(r"\s+", " ", x).strip() for x in result["address"]]
#
#         assert result is not None
#         assert result["rel_url"] == rel_url
#         assert result["title"] == ["Senator Justin Boyd (R)"]
#         assert result["phone"] == ["(479) 262-2156"]
#         assert result["address"] == ["P.O. Box 2625, Fort Smith, 72902"]
#         assert result["email"] == ["justin.boyd@senate.ar.gov"]
#         assert result["district"] == ["27"]
#         assert result["seniority"] == ["25"]
#         assert result["public_service"] == [
#             "Representative 2015,  2017,  2019,  2021,  Senate 2023,  2025",
#         ]
#
#         # committees too big to check all, just check first and last
#         assert result["committees"][0] == (
#             "TASK FORCE ON AUTISM",
#             "/Committees/Detail?code=985&ddBienniumSession=2025%2F2025R",
#         )
#
#         assert result["committees"][29] == (
#             "JBC-ADMINISTRATIVE RULE REVIEW SUBCOMMITTEE",
#             "/Committees/Detail?code=015&ddBienniumSession=2025%2F2025R",
#         )
