import logging
import sys
from logging.handlers import RotatingFileHandler


def setup_logger(log_path="app.log"):
    logger = logging.getLogger(__name__)

    logger.setLevel(logging.DEBUG)  # capture everything; handlers filter

    # --- General log (INFO+) ---
    general_handler = RotatingFileHandler(
        log_path,
        maxBytes=5 * 1024 * 1024,  # 5 MB
        backupCount=3,
    )
    general_handler.setLevel(logging.INFO)  # INFO, WARNING, ERROR, CRITICAL
    general_handler.setFormatter(
        logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")
    )

    # --- Warnings/errors log (WARNING+) ---
    warning_handler = RotatingFileHandler(
        "warnings_" + log_path,
        maxBytes=2 * 1024 * 1024,  # 2 MB
        backupCount=5,
    )
    warning_handler.setLevel(logging.WARNING)  # WARNING, ERROR, CRITICAL only
    warning_handler.setFormatter(
        logging.Formatter(
            "%(asctime)s [%(levelname)s] %(filename)s:%(lineno)d - %(message)s"
        )
    )

    # --- Console logging (optional) ---
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(logging.INFO)
    console_handler.setFormatter(
        logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")
    )

    # Add handlers
    logger.addHandler(general_handler)
    logger.addHandler(warning_handler)
    logger.addHandler(console_handler)
    return logger


# ---- Usage ----
logger = setup_logger("scraper.log")
