"""Arkleg bill validator."""

from pydantic import BaseModel, EmailStr, model_validator
from pydantic_extra_types.phone_numbers import PhoneNumber

from src.data_pipeline.validate.models.chamber_enum import ChamberEnum
from src.data_pipeline.validate.utils.validate_path_str import PathString
from src.data_pipeline.validate.utils.validate_url_str import BaseHttpUrlString


class BillValidator(BaseModel):
    """arkleg Bill Validator."""

    base_url: BaseHttpUrlString
    rel_url: PathString
    f_name: str
    l_name: str
    chamber: ChamberEnum
    party: str
    phone: PhoneNumber | None
    email: EmailStr
    address: str
    district: int
    seniority: int
    public_service: str
    committees: list[str]
    committee_links: list[str]

    @model_validator(mode="after")
    def validate_committee_names_and_links(self) -> BaseModel:
        """Ensure committee names and links are equal length."""
        if len(self.committee_links) != len(self.committees):
            msg = (
                f"Lengths of committee_links and committees do not match."
                f" {len(self.committee_links)} != {len(self.committees)}"
            )
            raise ValueError(msg)
        return self
