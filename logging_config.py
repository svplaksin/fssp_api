"""Logging configuration setup for the debt checker application."""

import logging
import os


def setup_logging(log_file="logs/app.log", log_level=logging.INFO):
    """Configure application logging with file and console output.

    Args:
        log_file: Path to log file (default: 'logs/app.log')
        log_level: Minimum logging level (default: logging.INFO)

    Returns:
        logging.Logger: Configured logger instance

    Side Effects:
        - Creates log directory if it doesn't exist
        - Sets up basicConfig for entire application

    Example:
        >>> logger = setup_logging()
        >>> logger.info("Application started")

    """
    log_dir = os.path.dirname(log_file)
    if not os.path.exists(log_dir):
        os.makedirs(log_dir)

    logging.basicConfig(
        filename=log_file,
        level=log_level,
        format="%(asctime)s - %(name)s - %(module)s - %(message)s",
        datefmt="%d-%m-%Y %H:%M:%S",
    )
    logger = logging.getLogger(__name__)
    return logger
