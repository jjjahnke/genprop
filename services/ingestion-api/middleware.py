"""
FastAPI middleware for request tracking and logging.
"""

import logging
import time
import uuid
from typing import Callable

from fastapi import Request, Response
from starlette.middleware.base import BaseHTTPMiddleware

from services.logging_utils import set_request_id, clear_context

logger = logging.getLogger(__name__)


class RequestIDMiddleware(BaseHTTPMiddleware):
    """
    Middleware to add unique request ID to each request.

    Generates a request ID (or uses X-Request-ID header if provided)
    and adds it to the response headers for tracing.
    """

    async def dispatch(
        self,
        request: Request,
        call_next: Callable
    ) -> Response:
        """Process request with request ID tracking."""
        # Get or generate request ID
        request_id = request.headers.get('X-Request-ID') or str(uuid.uuid4())

        # Set request ID in context for logging
        set_request_id(request_id)

        # Start timer
        start_time = time.time()

        try:
            # Process request
            response = await call_next(request)

            # Calculate duration
            duration_ms = (time.time() - start_time) * 1000

            # Add request ID to response headers
            response.headers['X-Request-ID'] = request_id

            # Log request completion
            logger.info(
                f"{request.method} {request.url.path} - "
                f"{response.status_code} - {duration_ms:.2f}ms",
                extra={
                    'request_id': request_id,
                    'method': request.method,
                    'path': request.url.path,
                    'status_code': response.status_code,
                    'duration_ms': duration_ms
                }
            )

            return response

        except Exception as e:
            # Log error with request ID
            logger.error(
                f"Request failed: {request.method} {request.url.path} - {str(e)}",
                exc_info=True,
                extra={
                    'request_id': request_id,
                    'method': request.method,
                    'path': request.url.path
                }
            )
            raise

        finally:
            # Clear context to avoid leaking into other requests
            clear_context()


__all__ = ["RequestIDMiddleware"]
