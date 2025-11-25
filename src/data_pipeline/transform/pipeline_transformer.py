"""pipeline function for transforming content."""

import html

from src.config.settings import config
from src.utils.logger import logger


class PipelineTransformer:
    """Pipeline transformer for transforming content."""

    def __init__(self, *, strict: bool | None = None) -> None:
        """Initialize the transformer with strict flag."""
        self.strict: bool = config["strict"] if strict is None else strict

    def transform_content(self, template: dict, content: dict) -> dict:  # noqa: C901, PLR0912
        """
        Pipeline function for transforming content.

        Args:
            template (dict): template holding transform functions for each possible expected
                content.key
            content (dict): content to be transformed

        Returns:
            dict: transformed content

        """
        transformed_content = {}
        failed_keys = []
        for key, value in content.items():
            if value is None:
                continue
            if key not in template:
                msg = f"Key {key} not in template"
                logger.error(msg)
                failed_keys.append(key)
                if self.strict:
                    raise KeyError(msg)
                continue

            if isinstance(value, list):
                new_value = [html.unescape(v) for v in value]
            else:
                new_value = html.unescape(value)

            transform_func = template[key]

            if not callable(transform_func):
                msg = f"Value {transform_func} is not callable for key {key}"

                logger.warning(msg)
                failed_keys.append(key)
                raise TypeError(msg)

            try:
                transformed_val = transform_func(new_value)

                if isinstance(transformed_val, dict):
                    transformed_content.update(transformed_val)
                else:
                    transformed_content[key] = transformed_val
            except (TypeError, KeyError) as e:
                logger.warning(f"Error occurred while transforming {key} : {e}")
                failed_keys.append(key)
                if self.strict:
                    raise
                continue

            except Exception as e:
                logger.error(f"Error occurred while transforming {key}: {e}")
                failed_keys.append(key)
                raise

        if failed_keys:
            logger.error(
                f"Keys {failed_keys} encountered error while transforming {content} "
                f"with {type(template)}",
            )
        return transformed_content
