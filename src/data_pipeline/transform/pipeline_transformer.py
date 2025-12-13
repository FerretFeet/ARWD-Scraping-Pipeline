"""pipeline class for transforming content."""

import html

from src.config.settings import project_config
from src.utils.logger import logger


class PipelineTransformer:
    """Pipeline transformer for transforming content."""

    def __init__(self, *, strict: bool | None = None) -> None:
        """Initialize the transformer with strict flag."""
        self.strict: bool = project_config["strict"] if strict is None else strict

    def transform_content(self, template: dict, content: dict, *, strict: bool = False) -> dict:
        self.strict = strict # Assuming 'self.strict' is used elsewhere
        transformed_content = {}
        failed_keys = []

        # ... (other setup code) ...

        for key, value in content.items():
            # ... (key and None checks) ...

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

            # 💡 FIX START: Correctly handle unescaping for collections and scalars

            if isinstance(value, (list, set)) and len(value) > 0:
                # If it's a list or set, iterate and unescape each item that is string-like
                new_value = type(value)(
                    html.unescape(str(v)) if isinstance(v, (str, bytes)) else v
                    for v in value
                )
            elif isinstance(value, str):
                # If it's a scalar value, convert to string and unescape
                new_value = html.unescape(value)
            else:
                new_value = value

            transform_func = template[key]

            if not callable(transform_func):
                msg = f"Value {transform_func} is not callable for key {key}"
                logger.warning(msg)
                failed_keys.append(key)
                raise TypeError(msg)

            try:

                transformed_val = transform_func(new_value, strict=strict)
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

        # ... (final logging and return) ...
        if failed_keys:
            logger.error(
                f"Keys {failed_keys} encountered error while transforming {content} "
                f"with {type(template)}",
            )
        return transformed_content
