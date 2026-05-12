"""
Logging configuration for SRF Event Monitoring System.

Provides structured logging with configurable output (console/file),
log rotation, and JSON formatting.
"""

import json
import logging
import logging.handlers
import sys
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Optional, Union

from .config import get_config


class JSONFormatter(logging.Formatter):
    """JSON formatter for structured logging."""
    
    def format(self, record: logging.LogRecord) -> str:
        """Format log record as JSON."""
        log_entry: Dict[str, Any] = {
            "timestamp": datetime.fromtimestamp(record.created).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "module": record.module,
            "function": record.funcName,
            "line": record.lineno,
            "message": record.getMessage(),
        }
        
        # Add extra attributes from record
        if hasattr(record, "props") and isinstance(record.props, dict):
            log_entry.update(record.props)
        
        # Include exception info if present
        if record.exc_info:
            log_entry["exception"] = self.formatException(record.exc_info)
        
        return json.dumps(log_entry)


class ColoredConsoleFormatter(logging.Formatter):
    """Colored console formatter for better readability."""
    
    COLORS = {
        "DEBUG": "\033[36m",      # Cyan
        "INFO": "\033[32m",       # Green
        "WARNING": "\033[33m",    # Yellow
        "ERROR": "\033[31m",      # Red
        "CRITICAL": "\033[41m",   # Red background
    }
    RESET = "\033[0m"
    
    def format(self, record: logging.LogRecord) -> str:
        """Format log record with colors."""
        color = self.COLORS.get(record.levelname, "")
        levelname = f"{color}{record.levelname:8s}{self.RESET}"
        
        timestamp = datetime.fromtimestamp(record.created).strftime("%Y-%m-%d %H:%M:%S")
        logger_name = record.name
        
        message = record.getMessage()
        
        # Add extra context if available
        extra = ""
        if hasattr(record, "props") and isinstance(record.props, dict):
            extra = " " + " ".join(f"{k}={v}" for k, v in record.props.items())
        
        # Include exception info if present
        if record.exc_info:
            message += "\n" + self.formatException(record.exc_info)
        
        return f"{timestamp} {levelname} [{logger_name}] {message}{extra}"


def setup_logging(
    level: Optional[str] = None,
    log_file: Optional[Path] = None,
    console: bool = True,
    max_file_size_mb: int = 10,
    retention_days: int = 30,
) -> None:
    """
    Configure logging based on configuration or parameters.
    
    This function is compatible with existing modules that call it with
    level, log_file, console, max_file_size_mb, retention_days parameters.
    
    Args:
        level: Logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL).
               If None, uses level from configuration.
        log_file: Path to log file. If None, uses path from configuration.
        console: Enable console logging. If True, uses console_enabled from config.
        max_file_size_mb: Maximum log file size in MB (overrides config).
        retention_days: Log retention days (overrides config).
    """
    config = get_config()
    logging_config = config.logging
    
    # Override with parameters if provided
    if level is None:
        level = logging_config.level
    if log_file is None:
        log_dir = config.paths.log_dir
        log_dir.mkdir(parents=True, exist_ok=True)
        log_file = log_dir / config.paths.log_file
    
    # Get root logger
    logger = logging.getLogger()
    logger.setLevel(getattr(logging, level.upper()))
    
    # Clear existing handlers
    logger.handlers.clear()
    
    # Console handler
    if console and logging_config.console_enabled:
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setLevel(getattr(logging, level.upper()))
        
        if logging_config.console_format == "colored":
            console_formatter = ColoredConsoleFormatter()
        else:
            console_formatter = logging.Formatter(
                fmt="%(asctime)s %(levelname)-8s [%(name)s] %(message)s",
                datefmt="%Y-%m-%d %H:%M:%S"
            )
        
        console_handler.setFormatter(console_formatter)
        logger.addHandler(console_handler)
    
    # File handler
    if logging_config.file_enabled:
        # Ensure log directory exists
        log_file.parent.mkdir(parents=True, exist_ok=True)
        
        if logging_config.rotation == "daily":
            file_handler = logging.handlers.TimedRotatingFileHandler(
                filename=log_file,
                when="midnight",
                interval=1,
                backupCount=retention_days,
                encoding="utf-8",
            )
        elif logging_config.rotation == "weekly":
            file_handler = logging.handlers.TimedRotatingFileHandler(
                filename=log_file,
                when="W0",
                interval=1,
                backupCount=retention_days,
                encoding="utf-8",
            )
        elif logging_config.rotation.endswith("MB"):
            # Size-based rotation (e.g., "10MB")
            max_bytes = int(logging_config.rotation.replace("MB", "")) * 1024 * 1024
            file_handler = logging.handlers.RotatingFileHandler(
                filename=log_file,
                maxBytes=max_bytes,
                backupCount=retention_days,
                encoding="utf-8",
            )
        else:
            # Default to no rotation
            file_handler = logging.FileHandler(
                filename=log_file,
                encoding="utf-8",
            )
        
        file_handler.setLevel(getattr(logging, level.upper()))
        
        if logging_config.format == "json":
            file_formatter = JSONFormatter()
        else:
            file_formatter = logging.Formatter(
                fmt="%(asctime)s %(levelname)-8s [%(name)s] %(message)s",
                datefmt="%Y-%m-%d %H:%M:%S"
            )
        
        file_handler.setFormatter(file_formatter)
        logger.addHandler(file_handler)
    
    # Add custom attributes to log records
    old_factory = logging.getLogRecordFactory()
    
    def record_factory(*args, **kwargs):
        record = old_factory(*args, **kwargs)
        # record.props will be set via extra parameter
        return record
    
    logging.setLogRecordFactory(record_factory)
    
    logger.info(
        "Logging configured",
        extra={"props": {
            "level": level,
            "console": console and logging_config.console_enabled,
            "file": logging_config.file_enabled,
            "format": logging_config.format,
            "rotation": logging_config.rotation,
        }}
    )


def get_logger(name: str) -> logging.Logger:
    """
    Get a logger with the given name.
    
    Args:
        name: Logger name (usually __name__)
        
    Returns:
        Configured logger instance.
    """
    # Ensure logging is configured
    if not logging.getLogger().handlers:
        setup_logging()
    
    return logging.getLogger(name)


# Additional utilities for contextual logging (optional)

class ContextLogger:
    """
    Contextual logger that adds metadata to log messages.
    
    Example:
        logger = ContextLogger("preprocessor", file="test.csv")
        logger.info("Processing file", extra={"size": 1024})
    """
    
    def __init__(self, module: str, **context):
        """
        Initialize contextual logger.
        
        Args:
            module: Module name (e.g., "preprocessor", "classifier")
            **context: Additional context to include in all log messages
        """
        self.logger = get_logger(module)
        self.context = context
    
    def _add_context(self, extra: Dict[str, Any] = None) -> Dict[str, Any]:
        """Merge instance context with message-specific extra data."""
        props = self.context.copy()
        if extra:
            props.update(extra)
        return {"props": props}
    
    def debug(self, msg: str, *args, extra: Dict[str, Any] = None, **kwargs):
        self.logger.debug(msg, *args, extra=self._add_context(extra), **kwargs)
    
    def info(self, msg: str, *args, extra: Dict[str, Any] = None, **kwargs):
        self.logger.info(msg, *args, extra=self._add_context(extra), **kwargs)
    
    def warning(self, msg: str, *args, extra: Dict[str, Any] = None, **kwargs):
        self.logger.warning(msg, *args, extra=self._add_context(extra), **kwargs)
    
    def error(self, msg: str, *args, extra: Dict[str, Any] = None, **kwargs):
        self.logger.error(msg, *args, extra=self._add_context(extra), **kwargs)
    
    def critical(self, msg: str, *args, extra: Dict[str, Any] = None, **kwargs):
        self.logger.critical(msg, *args, extra=self._add_context(extra), **kwargs)
    
    def exception(self, msg: str, *args, extra: Dict[str, Any] = None, **kwargs):
        self.logger.exception(msg, *args, extra=self._add_context(extra), **kwargs)