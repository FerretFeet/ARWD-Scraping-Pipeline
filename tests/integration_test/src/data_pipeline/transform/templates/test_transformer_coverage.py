"""Enforce code coverage for selectors
SELECTORS_TESTS is a config object for
"""

from pathlib import Path

from src.utils.paths import project_root
from tests.configs.transformer_config import TRANSFORMER_TESTS

TRANSFORMERS_DIR = (
    project_root / "src" / "data_pipeline" / "transform" / "templates"
)  # Replace with your directory path


def test_number_of_configurations_matches_files():
    """Enforce that a transformer tests is created for each transformer object"""
    num_files = len(
        [f for f in Path(TRANSFORMERS_DIR).rglob("*.py") if "__init__.py" not in str(f)],
    )

    # Load your configuration objects
    configurations = TRANSFORMER_TESTS

    # Assert the counts match
    assert (
        len(configurations) == num_files
    ), f"Expected {num_files} configurations, got {len(configurations)}"
