from datetime import datetime
from zoneinfo import ZoneInfo

import pytest

from src.data_pipeline.transform.utils.transform_str_to_date import transform_str_to_date

CST = ZoneInfo("America/Chicago")


class TestParseDate:
    @pytest.mark.parametrize(
        ("input_str", "expected_dt"),
        [
            # ---------- Valid full datetime strings ----------
            ("1/13/2025 2:39:05 PM", datetime(2025, 1, 13, 14, 39, 5, tzinfo=CST)),
            ("12/31/2024 11:59:59 AM", datetime(2024, 12, 31, 11, 59, 59, tzinfo=CST)),
            # ---------- Valid date-only strings (default midnight) ----------
            ("1/27/2025", datetime(2025, 1, 27, 0, 0, 0, tzinfo=CST)),
            ("12/1/2024", datetime(2024, 12, 1, 0, 0, 0, tzinfo=CST)),
            # ---------- Leading zeros ----------
            ("01/02/2025 01:02:03 PM", datetime(2025, 1, 2, 13, 2, 3, tzinfo=CST)),
            ("01/02/2025", datetime(2025, 1, 2, 0, 0, 0, tzinfo=CST)),
            # ---------- Leap year ----------
            ("2/29/2024", datetime(2024, 2, 29, 0, 0, 0, tzinfo=CST)),
            # ---------- DST edge cases ----------
            # First second *after* DST start (2 AM → 3 AM)
            ("3/10/2024 3:00:00 AM", datetime(2024, 3, 10, 3, 0, 0, tzinfo=CST)),
            # Last second before DST end
            ("11/3/2024 1:59:59 AM", datetime(2024, 11, 3, 1, 59, 59, tzinfo=CST)),
        ],
    )
    def test_parse_date_valid_formats(self, input_str, expected_dt):
        """Test parsing of valid date strings in supported formats."""
        result = transform_str_to_date(input_str)

        # Ensure returned type is timezone-aware datetime
        assert isinstance(result, datetime)
        assert result.tzinfo is not None
        assert result.tzinfo == CST

        assert result == expected_dt

    # -------------------------------------------------------------------------

    @pytest.mark.parametrize(
        "invalid_input",
        [
            "   ",  # whitespace only
            "2025-01-27",  # unsupported format
            "13/13/2025",  # invalid month/day combination
            "random text",  # garbage string
            "1/13/25",  # unsupported 2-digit year
            "02/29/2023",  # invalid non-leap-year date
            "1/1/2025 25:00:00 PM",  # invalid hour
            "1/1/2025 10:61:00 AM",  # invalid minute
        ],
    )
    def test_parse_date_invalid_formats(self, invalid_input):
        """Test that invalid or unsupported date strings raise ValueError."""
        with pytest.raises(ValueError, match="Invalid input to parse_date"):
            transform_str_to_date(invalid_input, strict=True)
        result = transform_str_to_date(invalid_input, strict=False)
        assert result is None

    def test_parse_date_too_many_inputs(self):
        with pytest.raises(ValueError, match="Date_str passed with too many"):
            transform_str_to_date(["", "", ""], strict=True)
        result = transform_str_to_date(["", "", ""], strict=False)
        assert result is None
