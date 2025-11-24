from pathlib import Path


def find_project_root(start: Path) -> Path:
    root_marker = 'scraper.log'
    for parent in [start, *start.parents]:
        if (parent / root_marker).exists():
            return parent
    raise RuntimeError(f"Project root not found, was searching for parent of {root_marker}")
project_root = find_project_root(Path(__file__).parent)