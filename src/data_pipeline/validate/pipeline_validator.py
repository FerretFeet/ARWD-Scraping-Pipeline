"""Pipeline class for validating content."""

from dataclasses import asdict

from pydantic import BaseModel, ValidationError

from src.config.settings import config
from src.utils.logger import logger


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
        try:
            validated = val_model(**unval_content)
            return asdict(validated)
        except ValidationError as err:
            msg = f"Validation error for {val_model.__name__}: {err}"
            logger.error(msg)
            raise ValueError(msg) from err
