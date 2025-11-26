"""Enforce code coverage for selectors
SELECTORS_TESTS is a config object for
"""

from pathlib import Path

import pytest

from src.utils.paths import project_root
from tests.configs.validator_config import VALIDATOR_TESTS

VALIDATORS_DIR = project_root / "src" / "data_pipeline" / "validate" / "models"


@pytest.mark.xfail
def test_number_of_configurations_matches_files():
    """Enforce that a selector tests is created for each selector object"""
    num_files = len([f for f in Path(VALIDATORS_DIR).rglob("*.py") if "__init__.py" not in str(f)])

    # Load your configuration objects
    configurations = VALIDATOR_TESTS

    # Assert the counts match
    assert (
        len(configurations) == num_files
    ), f"Expected {num_files} configurations, got {len(configurations)}"
