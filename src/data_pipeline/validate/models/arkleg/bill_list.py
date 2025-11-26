"""Validator for bill list transformer."""

from pydantic import BaseModel, HttpUrl

from src.data_pipeline.validate.models.chamber_enum import ChamberEnum
from src.data_pipeline.validate.utils.validate_path_str import PathString
from src.data_pipeline.validate.utils.validate_url_str import BaseHttpUrlString


class BillListValidator(BaseModel):
    """Bill List validator."""

    base_url: BaseHttpUrlString
    rel_url: PathString
    chamber: ChamberEnum
    session: str
    bill_url: list[HttpUrl]
    next_page: PathString
