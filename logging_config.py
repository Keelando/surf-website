#!/usr/bin/env python3
"""
Centralized logging configuration for envcan_wave project.

Provides consistent logging setup across all scripts with:
- Rotating file handlers (prevents unlimited log growth)
- Consistent formatting
- Configurable log levels
- Automatic logs/ directory creation

Usage:
    from logging_config import setup_logging

    logger = setup_logging('my_script')
    logger.info('Script started')
    logger.warning('Something to watch')
    logger.error('An error occurred')
"""

import logging
from logging.handlers import RotatingFileHandler
from pathlib import Path
import sys


# Log configuration
LOGS_DIR = Path(__file__).parent / "logs"
DEFAULT_LOG_LEVEL = logging.INFO
MAX_LOG_SIZE = 10 * 1024 * 1024  # 10 MB
BACKUP_COUNT = 5  # Keep 5 old log files

# Log format
LOG_FORMAT = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
DATE_FORMAT = '%Y-%m-%d %H:%M:%S'


def setup_logging(
    name: str,
    log_level: int = DEFAULT_LOG_LEVEL,
    console: bool = True,
    log_file: str = None
) -> logging.Logger:
    """
    Set up logging for a script with rotating file handler.

    Args:
        name: Logger name (typically script name without .py)
        log_level: Logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
        console: Whether to also log to console (default: True)
        log_file: Optional custom log filename (default: {name}.log)

    Returns:
        Configured logger instance

    Example:
        logger = setup_logging('tide_fetch', log_level=logging.DEBUG)
        logger.info('Starting tide data fetch')
    """
    # Create logs directory if it doesn't exist
    LOGS_DIR.mkdir(exist_ok=True)

    # Determine log file path
    if log_file is None:
        log_file = f"{name}.log"
    log_path = LOGS_DIR / log_file

    # Create logger
    logger = logging.getLogger(name)
    logger.setLevel(log_level)

    # Remove existing handlers to avoid duplicates
    logger.handlers.clear()

    # Create formatter
    formatter = logging.Formatter(LOG_FORMAT, datefmt=DATE_FORMAT)

    # Add rotating file handler
    file_handler = RotatingFileHandler(
        log_path,
        maxBytes=MAX_LOG_SIZE,
        backupCount=BACKUP_COUNT,
        encoding='utf-8'
    )
    file_handler.setLevel(log_level)
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

    # Add console handler if requested
    if console:
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setLevel(log_level)
        console_handler.setFormatter(formatter)
        logger.addHandler(console_handler)

    return logger


def get_logger(name: str) -> logging.Logger:
    """
    Get an existing logger by name.

    Useful for accessing the same logger from different modules.
    If the logger doesn't exist, it will create one with default settings.

    Args:
        name: Logger name

    Returns:
        Logger instance
    """
    logger = logging.getLogger(name)
    if not logger.handlers:
        # If logger doesn't exist or has no handlers, set it up
        return setup_logging(name)
    return logger


# Convenience function for quick one-off logging setup
def quick_setup(script_name: str, debug: bool = False) -> logging.Logger:
    """
    Quick logging setup for simple scripts.

    Args:
        script_name: Name of the script (without .py extension)
        debug: If True, sets log level to DEBUG (default: False = INFO)

    Returns:
        Configured logger

    Example:
        logger = quick_setup('my_script', debug=True)
    """
    level = logging.DEBUG if debug else logging.INFO
    return setup_logging(script_name, log_level=level)
