# ruff: noqa: E501
from src.data_pipeline.transform.pipeline_transformer import PipelineTransformer
from src.data_pipeline.transform.templates.arkleg.legislator_transformer import (
    LegislatorTransformer,
)

input_val = {
    "title": ["Senator Cecile Bledsoe (R)"],
    "phone": [""],
    "email": None,
    "address": [
        "\n\n709 Sky Mountain Dr.,                                Rogers,\n                                72757\n                            \n",
    ],
    "district": ["3"],
    "seniority": ["1"],
    "public_service": ["House 1999,  2001,  2003,  Senate 2009,  2011,  2013,  2015,  2017"],
    "committees": [
        (
            "\n                                        PUBLIC HEALTH, WELFARE AND LABOR COMMITTEE - SENATE\n                                    ",
            "/Committees/Detail?code=430&ddBienniumSession=2017%2F2017R",
        ),
        (
            "\n                                        ALC-CLAIMS REVIEW\n                                    ",
            "/Committees/Detail?code=004&ddBienniumSession=2017%2F2017R",
        ),
        (
            "\n                                         JBC-CLAIMS\n                                    ",
            "/Committees/Detail?code=009&ddBienniumSession=2017%2F2017R",
        ),
        (
            "\n                                         JBC-SPECIAL LANGUAGE\n                                    ",
            "/Committees/Detail?code=028&ddBienniumSession=2017%2F2017R",
        ),
        (
            "\n                                        ALC-ARKANSAS HEALTH INSURANCE MARKETPLACE OVERSIGHT SUBCOMMITTEE\n                                    ",
            "/Committees/Detail?code=032&ddBienniumSession=2017%2F2017R",
        ),
        (
            "\n                                        ALC-PEER\n                                    ",
            "/Committees/Detail?code=020&ddBienniumSession=2017%2F2017R",
        ),
        (
            "\n                                        ALC-POLICY MAKING\n                                    ",
            "/Committees/Detail?code=050&ddBienniumSession=2017%2F2017R",
        ),
        (
            "\n                                        ALC-REVIEW\n                                    ",
            "/Committees/Detail?code=010&ddBienniumSession=2017%2F2017R",
        ),
        (
            "\n                                        ARKANSAS HEALTH INSURANCE MARKETPLACE LEGISLATIVE OVERSIGHT COMMITTEE\n                                    ",
            "/Committees/Detail?code=131&ddBienniumSession=2017%2F2017R",
        ),
        (
            "\n                                        ARKANSAS LEGISLATIVE COUNCIL (ALC)\n                                    ",
            "/Committees/Detail?code=000&ddBienniumSession=2017%2F2017R",
        ),
        (
            "\n                                        ENERGY - JOINT\n                                    ",
            "/Committees/Detail?code=510&ddBienniumSession=2017%2F2017R",
        ),
        (
            "\n                                        INSURANCE & COMMERCE - SENATE\n                                    ",
            "/Committees/Detail?code=490&ddBienniumSession=2017%2F2017R",
        ),
        (
            "\n                                        JOINT BUDGET COMMITTEE\n                                    ",
            "/Committees/Detail?code=005&ddBienniumSession=2017%2F2017R",
        ),
        (
            "\n                                        JOINT BUDGET COMMITTEE - PRE-FISCAL SESSION BUDGET HEARINGS\n                                    ",
            "/Committees/Detail?code=007&ddBienniumSession=2017%2F2017R",
        ),
        (
            "\n                                        LEGISLATIVE JOINT AUDITING\n                                    ",
            "/Committees/Detail?code=905&ddBienniumSession=2017%2F2017R",
        ),
        (
            "\n                                        LEGISLATIVE JOINT AUDITING-MEDICAID SUBCOMMITTEE\n                                    ",
            "/Committees/Detail?code=931&ddBienniumSession=2017%2F2017R",
        ),
        (
            "\n                                        LEGISLATIVE JOINT AUDITING-STATE AGENCIES\n                                    ",
            "/Committees/Detail?code=920&ddBienniumSession=2017%2F2017R",
        ),
        (
            "\n                                        SENATE RULES, RESOLUTIONS & MEMORIALS\n                                    ",
            "/Committees/Detail?code=952&ddBienniumSession=2017%2F2017R",
        ),
        (
            "\n                                        STATE & PUBLIC SCHOOL LIFE & HEALTH INSURANCE TASK FORCE\n                                    ",
            "/Committees/Detail?code=461&ddBienniumSession=2017%2F2017R",
        ),
        (
            "\n                                        PUBLIC HEALTH - SENATE HEALTH SERVICES\n                                    ",
            "/Committees/Detail?code=428&ddBienniumSession=2017%2F2017R",
        ),
        (
            "\n                                        PUBLIC HEALTH - SENATE HUMAN SERVICES SUBCOMMITTEE\n                                    ",
            "/Committees/Detail?code=427&ddBienniumSession=2017%2F2017R",
        ),
        (
            "\n                                        PUBLIC HEALTH - SENATE LABOR & ENVIRONMENT SUBCOMMITTEE\n                                    ",
            "/Committees/Detail?code=438&ddBienniumSession=2017%2F2017R",
        ),
    ],
    "rel_url": "legislator/legislator.v3.html",
    "base_url": "/stem/",
}

output_val = {
    "party": "r",
    "first_name": "cecile",
    "last_name": "bledsoe",
    "chamber": "senate",
    "phone": None,
    "email": None,
    "address": "709 sky mountain dr., rogers, 72757",
    "district": 3,
    "seniority": 1,
    "public_service": "house 1999, 2001, 2003, senate 2009, 2011, 2013, 2015, 2017",
    "committees": [
        (
            "public health, welfare and labor committee - senate",
            "/Committees/Detail?code=430&ddBienniumSession=2017%2F2017R",
        ),
        ("alc-claims review", "/Committees/Detail?code=004&ddBienniumSession=2017%2F2017R"),
        ("jbc-claims", "/Committees/Detail?code=009&ddBienniumSession=2017%2F2017R"),
        ("jbc-special language", "/Committees/Detail?code=028&ddBienniumSession=2017%2F2017R"),
        (
            "alc-arkansas health insurance marketplace oversight subcommittee",
            "/Committees/Detail?code=032&ddBienniumSession=2017%2F2017R",
        ),
        ("alc-peer", "/Committees/Detail?code=020&ddBienniumSession=2017%2F2017R"),
        ("alc-policy making", "/Committees/Detail?code=050&ddBienniumSession=2017%2F2017R"),
        ("alc-review", "/Committees/Detail?code=010&ddBienniumSession=2017%2F2017R"),
        (
            "arkansas health insurance marketplace legislative oversight committee",
            "/Committees/Detail?code=131&ddBienniumSession=2017%2F2017R",
        ),
        (
            "arkansas legislative council (alc)",
            "/Committees/Detail?code=000&ddBienniumSession=2017%2F2017R",
        ),
        ("energy - joint", "/Committees/Detail?code=510&ddBienniumSession=2017%2F2017R"),
        (
            "insurance & commerce - senate",
            "/Committees/Detail?code=490&ddBienniumSession=2017%2F2017R",
        ),
        ("joint budget committee", "/Committees/Detail?code=005&ddBienniumSession=2017%2F2017R"),
        (
            "joint budget committee - pre-fiscal session budget hearings",
            "/Committees/Detail?code=007&ddBienniumSession=2017%2F2017R",
        ),
        (
            "legislative joint auditing",
            "/Committees/Detail?code=905&ddBienniumSession=2017%2F2017R",
        ),
        (
            "legislative joint auditing-medicaid subcommittee",
            "/Committees/Detail?code=931&ddBienniumSession=2017%2F2017R",
        ),
        (
            "legislative joint auditing-state agencies",
            "/Committees/Detail?code=920&ddBienniumSession=2017%2F2017R",
        ),
        (
            "senate rules, resolutions & memorials",
            "/Committees/Detail?code=952&ddBienniumSession=2017%2F2017R",
        ),
        (
            "state & public school life & health insurance task force",
            "/Committees/Detail?code=461&ddBienniumSession=2017%2F2017R",
        ),
        (
            "public health - senate health services",
            "/Committees/Detail?code=428&ddBienniumSession=2017%2F2017R",
        ),
        (
            "public health - senate human services subcommittee",
            "/Committees/Detail?code=427&ddBienniumSession=2017%2F2017R",
        ),
        (
            "public health - senate labor & environment subcommittee",
            "/Committees/Detail?code=438&ddBienniumSession=2017%2F2017R",
        ),
    ],
    "rel_url": "legislator/legislator.v3.html",
    "base_url": "/stem/",
}


class TestLegislatorTransformer:
    def test_legislator_transformer_success(self):

        transformer = PipelineTransformer()
        result = transformer.transform_content(LegislatorTransformer, input_val)

        assert result == output_val
