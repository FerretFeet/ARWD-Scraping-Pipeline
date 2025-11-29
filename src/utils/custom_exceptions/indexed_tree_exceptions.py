"""Custom exceptions for indexed tree."""


class TreeUpdateError(Exception):
    """Exception raised when tree update fails."""

    def __init__(self, message: str | None = None) -> None:
        """Initialize the exception."""
        default_msg = "Error when attempting to update tree."
        super().__init__(message or default_msg)
