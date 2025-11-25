"""Utility function to find project root."""

from pathlib import Path


def find_project_root(start: Path) -> Path:
    """Return project root path."""
    root_marker = "scraper.log"
    for parent in [start, *start.parents]:
        if (parent / root_marker).exists():
            return parent
    msg = f"Project root not found, was searching for parent of {root_marker}"
    raise RuntimeError(msg)


project_root = find_project_root(Path(__file__).parent)
