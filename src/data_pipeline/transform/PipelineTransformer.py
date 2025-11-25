"""pipeline function for transforming content"""

from src.config.settings import config
from src.utils.logger import logger


class PipelineTransformer:
    strict: bool = False

    def __init__(self, strict: bool | None = None):
        self.strict: bool = config["strict"] if strict is None else strict

    def transform_content(self, template: dict, content: dict) -> dict:
        """
        pipeline function for transforming content
        each template is coupled with an extract/selector

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
                logger.error(f"Key {key} not in template")
                failed_keys.append(key)
                if self.strict:
                    raise KeyError(f"Key {key} not in template")
                else:
                    continue
            transform_func = template[key]

            if not callable(transform_func):
                logger.warning(f"Value {transform_func} is not callable for key {key}")
                failed_keys.append(key)
                if self.strict:
                    raise TypeError(f"Value {transform_func} is not callable for key {key}")
                else:
                    continue

            try:
                transformed_val = transform_func(value)

                if isinstance(transformed_val, dict):
                    transformed_content.update(transformed_val)
                else:
                    transformed_content[key] = transformed_val
            except (TypeError, KeyError) as e:
                logger.warning(f"Error occurred while transforming {key} : {e}")
                failed_keys.append(key)
                if self.strict:
                    raise
                else:
                    continue

            except Exception as e:
                logger.error(f"Error occurred while transforming {key}: {e}")
                failed_keys.append(key)
                raise

        if failed_keys:
            logger.error(
                f"Keys {failed_keys} encountered error while transforming {content} "
                f"with {type(template)}"
            )
        return transformed_content
