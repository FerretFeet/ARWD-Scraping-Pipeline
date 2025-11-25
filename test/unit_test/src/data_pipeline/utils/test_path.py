from pathlib import Path

from src.utils.paths import find_project_root, project_root


def test_project_root():
    result = project_root
    expected = find_project_root(Path(__file__))
    assert result == expected