from pathlib import Path
from typing import Any


class PipelineLoader:
    """
    Configuration object for a specific database loading operation.
    """

    def __init__(self, sql_file_path: Path, upsert_function_name: str, required_params: dict[str, type]):
        self.sql_file_path: Path = sql_file_path
        self.upsert_function_name: str = upsert_function_name
        self.required_params: dict[str, type] = required_params

        self._template: str | None = None

    def validate_input(self, input_params: dict[str, Any]) -> None:
        """Ensures all required keys are present in the input dictionary."""
        missing_keys = [
            key for key in self.required_params
            if key not in input_params
        ]

        if missing_keys:
            raise ValueError(
                f"Loader for {self.upsert_function_name} is missing required parameters: "
                f"{', '.join(missing_keys)}",
            )

    @property
    def sql_template(self) -> str:
        if not self._template:
            self._template = self.sql_file_path.read_text()
        return self._template
