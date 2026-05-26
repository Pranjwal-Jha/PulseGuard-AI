"""Logging configuration for the application."""

import logging
import sys
from backend.config import get_settings

settings = get_settings()


def setup_logging():
    """Configure application logging."""
    
    # Create logger
    logger = logging.getLogger("rag_of_fire")
    
    if logger.handlers:
        return logger
    
    # Set log level
    log_level = logging.DEBUG if settings.debug else logging.INFO
    logger.setLevel(log_level)
    
    # Console handler
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(log_level)
    
    # Formatter
    formatter = logging.Formatter(
        "%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S"
    )
    console_handler.setFormatter(formatter)
    
    # Add handler
    logger.addHandler(console_handler)
    
    return logger


def get_logger(name: str) -> logging.Logger:
    """Get a logger instance."""
    setup_logging()
    return logging.getLogger(f"rag_of_fire.{name}")
