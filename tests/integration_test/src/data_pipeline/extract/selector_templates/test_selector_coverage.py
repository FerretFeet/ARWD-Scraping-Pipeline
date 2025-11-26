"""Enforce code coverage for selectors
SELECTORS_TESTS is a config object for
"""

from pathlib import Path

from src.utils.paths import project_root
from tests.configs.selector_config import SELECTOR_TESTS

SELECTORS_DIR = (
    project_root / "src" / "data_pipeline" / "extract" / "selector_templates"
)  # Replace with your directory path


def test_number_of_configurations_matches_files():
    """Enforce that a selector tests is created for each selector object"""
    num_files = len([f for f in Path(SELECTORS_DIR).rglob("*.py") if "__init__.py" not in str(f)])

    # Load your configuration objects
    configurations = SELECTOR_TESTS

    # Assert the counts match
    assert (
        len(configurations) == num_files
    ), f"Expected {num_files} configurations, got {len(configurations)}"
