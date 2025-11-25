import pytest

from src.data_pipeline.transform.utils.transform_phone import transform_phone


class TestNormalizePhone:
    # --- Tests for normalize_phone function ---
    @pytest.mark.parametrize(
        "input_string, expected_output",
        [
            # 1. Remove non numbers
            ("+1(555)-555-5555", "15555555555"),
            ("1 (555)-555 5555", "15555555555"),
            # 2. Do nothing
            ("15555555555", "15555555555"),
            ("", None),
            # 3. Add default country code if 10s-digit number
            ("5555555555", "15555555555"),
            # 4. Keeps country code if over 10-digit
            ("455555555555", "455555555555"),

            # 5. Returns cleaned number even with insufficient digits
            ("1(555)-555-55", "155555555"),

        ]
    )
    def test_normalize_phone_logic(self, input_string: str, expected_output: str):
        """
        Tests the logic of normalize_phone for removing non-numbers.
        The expected results MUST be all numbers or empty to pass.
        """
        # ACT
        result = transform_phone(input_string)

        # ASSERT
        assert result == expected_output
    @pytest.mark.parametrize(
        "input_string, expected_output",
        [
            # 1. Remove non numbers
            ("+1(555)-55555-5555", "15555555555"),
            ("1(555)-555-55", "155555555")
        ])
    def test_normalize_phone_strict_raises_error(self, input_string: str, expected_output: str):
        with pytest.raises(Exception):
            transform_phone(input_string, strict=True)

    @pytest.mark.parametrize(
        "input_string, expected_output",
        [
            # 1. Remove non numbers
            ("+1(555)-555-5555", "15555555555"),
            ("1 (555)-555 5555", "15555555555"),
            # 2. Do nothing
            ("15555555555", "15555555555"),
            ("", None),
            # 3. Add default country code if 10s-digit number
            ("5555555555", "15555555555"),
            # 4. Keeps country code if over 10-digit
            ("45555555555", "45555555555"),
    ])
    def test_normalize_phone_strict_success(self, input_string: str, expected_output: str):
        result = transform_phone(input_string, strict=True)
        assert result == expected_output
