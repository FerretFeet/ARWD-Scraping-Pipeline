"""Transform function for items like [('string_to_normalize', 'link or empty transform item')]."""

from src.data_pipeline.transform.utils.empty_transform import empty_transform
from src.data_pipeline.transform.utils.normalize_str import normalize_str


def normalize_list_of_str_link(
    input_list: list[tuple[str, str]],
    *,
    strict: bool = False,
) -> list[tuple[str, str]] | None:
    """Transform a list of tuples like [('string_to_normalize', 'link or empty transform item')]."""
    if not input_list:
        return None
    return [
        (normalize_str(x, strict=strict), empty_transform(y, strict=strict))
        for x, y in input_list
        if x and y
    ]
