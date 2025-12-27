"""
Structured logging utilities for ingestion-api.

Provides context-aware logging with batch_id and request_id correlation.
"""

import logging
from contextvars import ContextVar
from typing import Optional
from uuid import UUID

# Context variables for request/batch tracking
request_id_ctx: ContextVar[Optional[str]] = ContextVar('request_id', default=None)
batch_id_ctx: ContextVar[Optional[UUID]] = ContextVar('batch_id', default=None)


class StructuredLoggerAdapter(logging.LoggerAdapter):
    """
    Logger adapter that adds structured context to log records.

    Automatically includes request_id and batch_id from context vars.
    """

    def process(self, msg, kwargs):
        """Add context variables to log record extra dict."""
        extra = kwargs.get('extra', {})

        # Add request_id if available
        request_id = request_id_ctx.get()
        if request_id:
            extra['request_id'] = request_id

        # Add batch_id if available
        batch_id = batch_id_ctx.get()
        if batch_id:
            extra['batch_id'] = str(batch_id)

        kwargs['extra'] = extra
        return msg, kwargs


def get_logger(name: str) -> StructuredLoggerAdapter:
    """
    Get a structured logger for the given module.

    Args:
        name: Logger name (usually __name__)

    Returns:
        StructuredLoggerAdapter with context-aware logging
    """
    base_logger = logging.getLogger(name)
    return StructuredLoggerAdapter(base_logger, {})


def set_request_id(request_id: str) -> None:
    """
    Set the request ID for the current context.

    Args:
        request_id: Request ID to set (from X-Request-ID header)
    """
    request_id_ctx.set(request_id)


def set_batch_id(batch_id: UUID) -> None:
    """
    Set the batch ID for the current context.

    Args:
        batch_id: Batch ID to set
    """
    batch_id_ctx.set(batch_id)


def clear_context() -> None:
    """Clear request and batch IDs from context."""
    request_id_ctx.set(None)
    batch_id_ctx.set(None)


class StructuredFormatter(logging.Formatter):
    """
    Custom formatter that outputs structured log records.

    Includes timestamp, level, logger name, message, and context fields.
    """

    def format(self, record):
        """Format log record with structured context."""
        # Base format
        log_parts = [
            f"{self.formatTime(record, self.datefmt)}",
            f"[{record.levelname:8s}]",
            f"{record.name}:",
        ]

        # Add request_id if present
        if hasattr(record, 'request_id'):
            log_parts.append(f"[req:{record.request_id[:8]}]")

        # Add batch_id if present
        if hasattr(record, 'batch_id'):
            batch_short = record.batch_id[:8] if len(record.batch_id) > 8 else record.batch_id
            log_parts.append(f"[batch:{batch_short}]")

        # Add message
        log_parts.append(record.getMessage())

        # Add exception info if present
        if record.exc_info:
            log_parts.append("\n" + self.formatException(record.exc_info))

        return " ".join(log_parts)


__all__ = [
    "get_logger",
    "set_request_id",
    "set_batch_id",
    "clear_context",
    "StructuredFormatter",
    "StructuredLoggerAdapter",
]
