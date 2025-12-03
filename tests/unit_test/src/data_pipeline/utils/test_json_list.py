import json
import tempfile
from pathlib import Path

from src.utils.json_list import append_to_json_list, load_json_list


def test_load_nonexistent_file_returns_empty_list():
    with tempfile.NamedTemporaryFile(delete=True) as tmp:
        # Remove the file to simulate non-existent
        tmp_path = tmp.name
    # Now tmp_path does not exist
    assert load_json_list(tmp_path) == []

def test_append_and_load():
    with tempfile.NamedTemporaryFile(delete=False) as tmp:
        tmp_path = Path(tmp.name)
    try:
        append_to_json_list(tmp_path, "Alice")
        append_to_json_list(tmp_path, "Bob")
        data = load_json_list(tmp_path)
        assert len(data) == 2  # noqa: PLR2004
        assert data[0] == "Alice"
        assert data[1] == "Bob"
    finally:
        Path.unlink(tmp_path)

def test_invalid_json_returns_empty_list():
    with tempfile.NamedTemporaryFile(mode="w+", delete=False) as tmp:
        tmp.write("not valid json")
        tmp_path = Path(tmp.name)
    try:
        assert load_json_list(tmp_path) == []
    finally:
        Path.unlink(tmp_path)

def test_file_contains_non_list_raises_error():
    with tempfile.NamedTemporaryFile(mode="w+", delete=False) as tmp:
        json.dump({"not": "a list"}, tmp)
        tmp_path = Path(tmp.name)
    try:
        import pytest  # noqa: PLC0415
        with pytest.raises(ValueError):  # noqa: PT011
            load_json_list(tmp_path)
    finally:
        Path.unlink(tmp_path)
