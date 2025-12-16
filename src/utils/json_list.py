"""Utility functions for loading Json files."""

import json
from pathlib import Path


def load_json_list(file_path: str | Path) -> list:
    """
    Load a JSON file as a list.

    Returns an empty list if the file does not exist or is invalid.
    """
    file_path = Path(file_path)

    if not file_path.exists():
        return []

    try:
        with Path.open(file_path, "r", encoding="utf-8") as f:
            data = json.load(f)
            if isinstance(data, list):
                return data
            msg = "JSON file does not contain a list"
            raise ValueError(msg)
    except json.JSONDecodeError:
        return []


def append_to_json_list(file_path: str | Path, new_item: str) -> None:
    """
    Append a dictionary to a JSON list file.

    Creates the file if it does not exist.
    """
    file_path = Path(file_path)
    data = load_json_list(file_path)

    data.append(new_item)

    with Path.open(file_path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=4)
