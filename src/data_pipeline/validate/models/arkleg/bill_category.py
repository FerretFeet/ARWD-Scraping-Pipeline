"""Validator for bill category transformer."""

from pydantic.dataclasses import dataclass


@dataclass
class BillCategoryValidator:
    """Bill category validator."""

    base_url: str
    rel_url: str
    bill_cat_link: list[str]

    def __post_init__(self) -> None:
        """Post initialization."""
        error_msg = None
        if not self.base_url.startswith("http"):
            error_msg = "base_url must start with http"
            raise ValueError(error_msg)
        if self.base_url.endswith("/"):
            error_msg = "base_url must not end with /"
            raise ValueError(error_msg)
        if not self.rel_url.startswith("/"):
            error_msg = "rel_url must start with http"
            raise ValueError(error_msg)
        for item in self.bill_cat_link:
            if not item.startswith("/"):
                error_msg = "bill_cat_link must start with /"
                raise ValueError(error_msg)
