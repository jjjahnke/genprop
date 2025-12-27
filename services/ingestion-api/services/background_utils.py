"""
Utilities for safe background task execution.

Provides error handling wrappers for FastAPI background tasks.
"""

import logging
import functools
from typing import Callable, Any
from uuid import UUID

from .logging_utils import get_logger, set_batch_id

logger = get_logger(__name__)


def safe_background_task(func: Callable) -> Callable:
    """
    Decorator to wrap background tasks in comprehensive error handling.

    Ensures exceptions in background tasks are logged and don't crash the service.
    Also sets up batch_id logging context if batch_id is passed as a parameter.

    Args:
        func: The async function to wrap

    Returns:
        Wrapped function with error handling

    Example:
        ```python
        @safe_background_task
        async def process_csv_async(csv_path, batch_id, ...):
            # Your processing code
            pass
        ```
    """
    @functools.wraps(func)
    async def wrapper(*args, **kwargs) -> None:
        """Wrapper function with error handling."""
        # Extract batch_id if present (for logging context)
        batch_id = kwargs.get('batch_id')
        if batch_id and isinstance(batch_id, UUID):
            set_batch_id(batch_id)

        try:
            # Execute the background task
            await func(*args, **kwargs)

        except Exception as e:
            # Log the error with full traceback
            # The inner function should have already called fail_batch()
            task_name = func.__name__
            logger.error(
                f"Background task '{task_name}' failed: {e}",
                exc_info=True,
                extra={
                    'task_name': task_name,
                    'batch_id': str(batch_id) if batch_id else None,
                    'error_type': type(e).__name__
                }
            )
            # Suppress the exception to prevent service crash
            # The error has been logged and batch should be marked as failed by inner function

    return wrapper


__all__ = ["safe_background_task"]
