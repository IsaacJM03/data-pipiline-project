"""Logging utility for patent analytics pipeline."""

import logging
import sys
from pathlib import Path
from datetime import datetime

# Create logs directory if it doesn't exist
LOGS_DIR = Path("logs")
LOGS_DIR.mkdir(exist_ok=True)

# Configure logging
LOG_FILE = LOGS_DIR / f"pipeline_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"

# Create logger
logger = logging.getLogger("patent_pipeline")
logger.setLevel(logging.DEBUG)

# Console handler (INFO level and above)
console_handler = logging.StreamHandler(sys.stdout)
console_handler.setLevel(logging.INFO)
console_format = logging.Formatter(
    "%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
console_handler.setFormatter(console_format)

# File handler (DEBUG level and above)
file_handler = logging.FileHandler(LOG_FILE)
file_handler.setLevel(logging.DEBUG)
file_format = logging.Formatter(
    "%(asctime)s | %(levelname)-8s | %(name)s | %(funcName)s:%(lineno)d | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
file_handler.setFormatter(file_format)

# Add handlers to logger
logger.addHandler(console_handler)
logger.addHandler(file_handler)


def get_logger(module_name: str) -> logging.Logger:
    """Get a logger instance for a specific module."""
    return logging.getLogger(f"patent_pipeline.{module_name}")


def log_pipeline_start(stage_name: str):
    """Log the start of a pipeline stage."""
    main_logger = get_logger("main")
    main_logger.info("=" * 80)
    main_logger.info(f"STARTING: {stage_name}")
    main_logger.info("=" * 80)


def log_pipeline_end(stage_name: str, status: str = "SUCCESS"):
    """Log the end of a pipeline stage."""
    main_logger = get_logger("main")
    main_logger.info("=" * 80)
    main_logger.info(f"COMPLETED: {stage_name} - Status: {status}")
    main_logger.info("=" * 80)


def log_stats(topic: str, stats: dict):
    """Log statistics in a formatted way."""
    logger_instance = get_logger("stats")
    logger_instance.info(f"\n{topic}:")
    for key, value in stats.items():
        if isinstance(value, (int, float)):
            logger_instance.info(f"  - {key}: {value:,}")
        else:
            logger_instance.info(f"  - {key}: {value}")
