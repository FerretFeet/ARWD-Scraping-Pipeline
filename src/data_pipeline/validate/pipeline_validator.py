"""Pipeline class for validating content."""

from pydantic import BaseModel

from src.config.settings import config


class PipelineValidator:
    """Pipeline class for validating content."""

    def __init__(self, *, strict: bool | None = None) -> None:
        """Initialize the transformer with strict flag."""
        self.strict: bool = config["strict"] if strict is None else strict

    def validate(self, val_model: type[BaseModel], unval_content: dict) -> dict[str, ...]:
        """
        Pipeline function for validating content.

        Args:
            val_model (BaseModel): Validation model.
            unval_content (dict): Unvalidation content.

        Each model should have a key to match each content key

        """
        validated = val_model.model_validate(unval_content)
        return validated.model_dump()
