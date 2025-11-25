import pytest

from src.data_pipeline.transform.utils.normalize_str import normalize_str


class TestNormalizeStr:
    # --- Tests for normalize_str function ---
    @pytest.mark.parametrize(
        ("input_string", "expected_output"),
        [
            # 1. Standard Case (No change, lowercased)
            ("hello world", "hello world"),
            # 2. Leading and Trailing Whitespace
            ("  trim me  ", "trim me"),
            ("\t\nnewline and tab\t", "newline and tab"),
            # 3. Multiple Internal Spaces/Whitespace
            ("A  B\tC\n D", "a b c d"),
            ("multiple   spaces here", "multiple spaces here"),
            # 4. Uppercase/Mixed Case (This confirms the lowercasing behavior)
            ("HELLO WORLD", "hello world"),
            ("MiXeD cAsE Test", "mixed case test"),
            # 5. Empty/Whitespace only
            ("", ""),
            ("   ", ""),
            ("\t\n", ""),
        ],
    )
    def test_normalize_str_logic(self, input_string: str, expected_output: str):
        """Tests the logic of normalize_str for whitespace normalization and lowercasing.
        The expected results MUST be lowercase to pass the tests.
        """
        # ACT
        result = normalize_str(input_string)

        # ASSERT
        assert result == expected_output

    def test_normalize_str_empty_string(self):
        """Ensure an empty string remains empty."""
        assert normalize_str("") == ""

    def test_normalize_str_only_whitespace_yields_empty_string(self):
        """Ensure a string of only whitespace yields an empty string."""
        assert normalize_str(" \t\n ") == ""
