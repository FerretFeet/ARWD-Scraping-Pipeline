"""pipeline class for transforming content."""

import html
from collections.abc import Callable
from typing import Any, TypeVar

from src.config.settings import project_config
from src.utils.logger import logger


class PipelineTransformer:
    """Pipeline transformer for transforming content."""

    def __init__(self, *, strict: bool | None = None) -> None:
        """Initialize the transformer with strict flag."""
        self.strict: bool = project_config["strict"] if strict is None else strict

    def transform_content(self, template: dict, content: dict, *, strict: bool = False) -> dict:
        """
        Push values through given template.

        Args:
            template: dict with form (str_key, callable(_T, strict: bool))
            content: dict with content to transform (matching_key, _T))
            strict: a bool which flags whether to raise errors or handle gracefully when possible.

        """
        self.strict = strict
        transformed_content = {}
        failed_keys = []

        for key, value in content.items():

            if key not in template:
                msg = f"Key {key} not in template"
                logger.error(msg)
                failed_keys.append(key)
                if self.strict:
                    raise KeyError(msg)
                continue

            if value is None:
                transformed_content[key] = value
                continue

            new_value = self._normalize_input(value)

            transform_func = template[key]
            self._validate_transform_func(key, transform_func, failed_keys)
            transformed_val = transform_func(new_value, strict=self.strict)

            self._update_transformed_content(key, transformed_val, transformed_content, failed_keys)

        self._flag_failed_keys(failed_keys, content, template)

        return transformed_content

    _T = TypeVar("_T")

    def _normalize_input(self, value: _T) -> _T:
        """Normalize input for transformation."""
        if isinstance(value, (list, set)) and len(value) > 0:
            # If it's a list or set, iterate and unescape each item that is string-like
            new_value = type(value)(
                html.unescape(str(v)) if isinstance(v, (str, bytes)) else v for v in value
            )
        elif isinstance(value, str):
            # If it's a scalar value, convert to string and unescape
            new_value = html.unescape(value)
        else:
            new_value = value
        return new_value

    def _validate_transform_func(
        self,
        key: str,
        transform_func: Callable,
        failed_keys_tracker: list,
    ) -> None:
        """Validate transform function is of expected form."""
        if not callable(transform_func):
            msg = f"Value {transform_func} is not callable for key {key}"
            logger.warning(msg)
            failed_keys_tracker.append(key)
            raise TypeError(msg)

    def _update_transformed_content(
        self,
        key: str,
        transformed_val: Any,
        transformed_content: dict,
        failed_keys: list,
    ) -> None:
        """Update transformed content for given key."""
        try:

            if isinstance(transformed_val, dict):
                transformed_content.update(transformed_val)
            else:
                transformed_content[key] = transformed_val
        except (TypeError, KeyError) as e:
            logger.warning(f"Error occurred while transforming {key} : {e}")
            failed_keys.append(key)
            if self.strict:
                raise
            return
        except Exception as e:  # noqa: BLE001
            logger.error(f"Error occurred while transforming {key}: {e}")
            failed_keys.append(key)
            return

    def _flag_failed_keys(self, failed_keys: list, content: dict, template: dict) -> None:
        """Log a message for all failed keys."""
        if failed_keys:
            msg = (
                f"Keys {failed_keys} encountered error while transforming {content} "
                f"with {type(template)}"
            )
            logger.error(msg)
