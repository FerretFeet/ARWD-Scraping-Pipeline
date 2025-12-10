import pytest

from src.data_pipeline.transform.utils.strip_session_from_string import strip_session_from_link


def test_strip_session_from_link():
    # Test cases
    test_data = [
        ("/Legislators/Detail?member=S.+Flowers&ddBienniumSession=2025%2F2025R",
         "2025%2F2025R"),
        ("/Legislators/Detail?member=Furman&ddBienniumSession=2025%2F2025R",
         "2025%2F2025R"),
    ]

    for input_link, expected in test_data:
        assert strip_session_from_link(input_link) == expected


def test_strip_session_from_link_bad_input():
    # Test cases
    test_data = [
        ("/Legislators/Detail?member=Gazaway",  # no '&' present
         ["/Legislators/Detail?member=Gazaway"]),
        ("", [""]),  # empty string
        ("abc&def=ghi", ["abc&def=ghi"]),  # multiple '&'
    ]

    for input_link, expected in test_data:
        with pytest.raises(ValueError):
            assert strip_session_from_link(input_link) == expected
