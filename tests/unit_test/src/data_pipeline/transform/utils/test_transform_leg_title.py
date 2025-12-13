import pytest

from src.data_pipeline.transform.utils.transform_leg_title import (
    _parse_name,
    transform_leg_title,
)


class TestTransformTitles:
    @pytest.mark.parametrize(
        ("input_string", "expected_output"),
        [
            # 1. Remove non numbers
            (
                "representative aaron pilkington (r)",
                {
                    "first_name": "aaron",
                    "last_name": "pilkington",
                    "chamber": "house",
                    "party": "r",
                },
            ),
            # 2. handles missing data
            (
                "senator john s. doe",
                {
                    "first_name": "john",
                    "last_name": "s. doe",
                    "chamber": "senate",
                    "party": None,
                },
            ),
            (
                "john doe (r)",
                {
                    "first_name": "john",
                    "last_name": "doe",
                    "chamber": None,
                    "party": "r",
                },
            ),
            (
                "john (r)",
                {
                    "first_name": None,
                    "last_name": "john",
                    "chamber": None,
                    "party": "r",
                },
            ),
        ],
    )
    def test_transform_leg_title(self, input_string: str, expected_output: str):
        """Tests the logic of normalize_phone for removing non-numbers.
        The expected results MUST be all numbers or empty to pass.
        """
        # ACT
        result = transform_leg_title(input_string)

        # ASSERT
        assert result == expected_output


class TestParseName:
    @pytest.mark.parametrize(
        ("input_name", "expected_output"),
        [
            # 1. Standard Case (First and Last Name)
            ("Representative Aaron Pilkington (R)", ("Aaron", "Pilkington")),
            ("Jane Doe", ("Jane", "Doe")),
            ("John Smith", ("John", "Smith")),
            # 2. Multi-word Name (Middle names become part of the Last Name)
            ("Michael B. Jordan", ("Michael", "B. Jordan")),
            ("John Fitzgerald Kennedy", ("John", "Fitzgerald Kennedy")),
            ("Alice Cooper Smith", ("Alice", "Cooper Smith")),
            # 3. Single Word Name
            ("Cher", (None, "Cher")),
            ("Madonna", (None, "Madonna")),
            ("john (r)", (None, "john")),
            # 4. Leading/Trailing Whitespace
            (" Leading Space", ("Leading", "Space")),
            ("Trailing Space  ", ("Trailing", "Space")),
            (" Internal      Space ", ("Internal", "Space")),
            (" Internal   Middle   Space ", ("Internal", "Middle Space")),
        ],
    )
    def test_parse_name_logic(self, input_name, expected_output):
        """Tests the core logic of split_name across various input formats."""
        # ACT
        result = _parse_name(input_name)

        # ASSERT
        assert result == expected_output
