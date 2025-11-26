"""Validator for bill category transformer."""

from pydantic import BaseModel

from src.data_pipeline.validate.utils.validate_path_str import PathString
from src.data_pipeline.validate.utils.validate_url_str import BaseHttpUrlString


class BillCategoryValidator(BaseModel):
    """Bill category validator."""

    base_url: BaseHttpUrlString
    rel_url: PathString
    bill_cat_link: list[PathString]
