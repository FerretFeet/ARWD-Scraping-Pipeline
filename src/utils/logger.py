"""Logger."""

import logging
import sys
from logging.handlers import RotatingFileHandler

from src.utils.paths import project_root


def setup_logger(log_path: str = "app.log") -> logging.Logger:
    """Initialize logger."""
    logger = logging.getLogger(__name__)

    logger.setLevel(logging.DEBUG)

    # --- General log (INFO+) ---
    general_handler = RotatingFileHandler(
        project_root / log_path,
        maxBytes=5 * 1024 * 1024,  # 5 MB
        backupCount=3,
    )
    general_handler.setLevel(logging.INFO)  # INFO, WARNING, ERROR, CRITICAL
    general_handler.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s"))

    # --- Warnings/errors log (WARNING+) ---
    warning_handler = RotatingFileHandler(
        project_root / str("warnings_" + log_path),
        maxBytes=2 * 1024 * 1024,  # 2 MB
        backupCount=5,
    )
    warning_handler.setLevel(logging.WARNING)  # WARNING, ERROR, CRITICAL only
    warning_handler.setFormatter(
        logging.Formatter("%(asctime)s [%(levelname)s] %(filename)s:%(lineno)d - %(message)s"),
    )

    # --- Console logging (optional) ---
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(logging.INFO)
    console_handler.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s]"
                                                   " %(filename)s:%(lineno)d - %(message)s"))

    # Add handlers
    logger.addHandler(general_handler)
    logger.addHandler(warning_handler)
    logger.addHandler(console_handler)
    return logger


logger = setup_logger("scraper.log")
