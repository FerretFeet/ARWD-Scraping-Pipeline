# import pytest
#
# from test.conftest import BILL_FIXTURE_PARAMS, BILL_LIST_FIXTURE_PARAMS, VOTE_PAGE_PARAMS, LEGISLATOR_FIXTURE_PARAMS, \
#     LEGISLATOR_LIST_PARAMS
#
# combined = [
#     *BILL_FIXTURE_PARAMS,
#     *BILL_LIST_FIXTURE_PARAMS,
#     *VOTE_PAGE_PARAMS,
#     *LEGISLATOR_FIXTURE_PARAMS,
#     *LEGISLATOR_LIST_PARAMS,
# ]
#
# @pytest.mark.parametrize(("name", "url", "variant"), combined)
# def test_variants(html_fixture, name, url, variant):
#     fp = html_selectorfixture(
#         url=url,
#         name=name,
#         variant=variant,
#     )
#
#     html = fp.read_text()
#     assert "Example" in html
