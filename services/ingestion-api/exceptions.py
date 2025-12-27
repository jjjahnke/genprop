"""
Global exception handlers for the ingestion API.

Provides standardized error responses for:
- HTTP exceptions (400, 404, 413, 422, 500)
- Validation errors (Pydantic)
- Request validation errors
- Unhandled exceptions
"""

import logging
from datetime import datetime, timezone
from typing import Union

from fastapi import FastAPI, Request, status
from fastapi.exceptions import RequestValidationError, HTTPException
from fastapi.responses import JSONResponse
from pydantic import ValidationError

from models.schemas import ErrorResponse

logger = logging.getLogger(__name__)


def register_exception_handlers(app: FastAPI) -> None:
    """
    Register all exception handlers with the FastAPI application.

    Args:
        app: The FastAPI application instance
    """
    app.add_exception_handler(HTTPException, http_exception_handler)
    app.add_exception_handler(RequestValidationError, validation_exception_handler)
    app.add_exception_handler(ValidationError, pydantic_validation_handler)
    app.add_exception_handler(Exception, global_exception_handler)

    logger.info("Registered global exception handlers")


async def http_exception_handler(request: Request, exc: HTTPException) -> JSONResponse:
    """
    Handle FastAPI HTTPException errors.

    Converts HTTPException to standardized ErrorResponse format.

    Args:
        request: The incoming request
        exc: The HTTPException instance

    Returns:
        JSONResponse with error details
    """
    # Log the error (except for client errors like 404, 400)
    if exc.status_code >= 500:
        logger.error(
            f"HTTP {exc.status_code}: {exc.detail}",
            extra={
                "path": request.url.path,
                "method": request.method,
                "status_code": exc.status_code
            }
        )
    else:
        logger.info(
            f"HTTP {exc.status_code}: {exc.detail}",
            extra={
                "path": request.url.path,
                "method": request.method
            }
        )

    # If detail is already a dict (from our custom exceptions), use it
    if isinstance(exc.detail, dict):
        error_response = ErrorResponse(
            error=exc.detail.get("error", "HTTPException"),
            message=exc.detail.get("message", str(exc.detail)),
            detail=exc.detail.get("detail"),
            timestamp=datetime.now(timezone.utc)
        )
    else:
        # Simple string detail
        error_name = {
            400: "BadRequest",
            401: "Unauthorized",
            403: "Forbidden",
            404: "NotFound",
            413: "PayloadTooLarge",
            422: "UnprocessableEntity",
            500: "InternalServerError",
            503: "ServiceUnavailable",
        }.get(exc.status_code, "HTTPException")

        error_response = ErrorResponse(
            error=error_name,
            message=str(exc.detail),
            detail=None,
            timestamp=datetime.now(timezone.utc)
        )

    return JSONResponse(
        status_code=exc.status_code,
        content=error_response.model_dump(mode="json")
    )


async def validation_exception_handler(
    request: Request,
    exc: RequestValidationError
) -> JSONResponse:
    """
    Handle FastAPI request validation errors (422).

    Occurs when request body, query params, or path params fail Pydantic validation.

    Args:
        request: The incoming request
        exc: The RequestValidationError instance

    Returns:
        JSONResponse with validation error details
    """
    logger.warning(
        f"Validation error: {exc.errors()}",
        extra={
            "path": request.url.path,
            "method": request.method,
            "errors": exc.errors()
        }
    )

    # Format validation errors for user-friendly display
    error_details = []
    for error in exc.errors():
        field_path = " -> ".join(str(loc) for loc in error["loc"])
        error_details.append({
            "field": field_path,
            "message": error["msg"],
            "type": error["type"],
            "input": error.get("input")
        })

    error_response = ErrorResponse(
        error="ValidationError",
        message=f"Request validation failed: {len(error_details)} error(s)",
        detail={"errors": error_details},
        timestamp=datetime.now(timezone.utc)
    )

    return JSONResponse(
        status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
        content=error_response.model_dump(mode="json")
    )


async def pydantic_validation_handler(
    request: Request,
    exc: ValidationError
) -> JSONResponse:
    """
    Handle Pydantic validation errors.

    Occurs when Pydantic model validation fails in application code.

    Args:
        request: The incoming request
        exc: The ValidationError instance

    Returns:
        JSONResponse with validation error details
    """
    logger.warning(
        f"Pydantic validation error: {exc.errors()}",
        extra={
            "path": request.url.path,
            "method": request.method,
            "errors": exc.errors()
        }
    )

    # Format validation errors
    error_details = []
    for error in exc.errors():
        field_path = " -> ".join(str(loc) for loc in error["loc"])
        error_details.append({
            "field": field_path,
            "message": error["msg"],
            "type": error["type"]
        })

    error_response = ErrorResponse(
        error="DataValidationError",
        message=f"Data validation failed: {len(error_details)} error(s)",
        detail={"errors": error_details},
        timestamp=datetime.now(timezone.utc)
    )

    return JSONResponse(
        status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
        content=error_response.model_dump(mode="json")
    )


async def global_exception_handler(request: Request, exc: Exception) -> JSONResponse:
    """
    Handle all unhandled exceptions.

    Catches any exception not handled by other handlers. Logs the full
    traceback and returns a generic error to the client.

    Args:
        request: The incoming request
        exc: The exception instance

    Returns:
        JSONResponse with generic error message
    """
    # Log the full exception with traceback
    logger.error(
        f"Unhandled exception: {exc}",
        exc_info=True,
        extra={
            "path": request.url.path,
            "method": request.method,
            "client": request.client.host if request.client else "unknown",
            "exception_type": type(exc).__name__
        }
    )

    # In production, don't expose internal error details
    from config import settings

    if settings.DEBUG:
        detail = {
            "error": str(exc),
            "type": type(exc).__name__
        }
    else:
        detail = None

    error_response = ErrorResponse(
        error="InternalServerError",
        message="An unexpected error occurred. Please try again later.",
        detail=detail,
        timestamp=datetime.now(timezone.utc)
    )

    return JSONResponse(
        status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
        content=error_response.model_dump(mode="json")
    )


__all__ = [
    "register_exception_handlers",
    "http_exception_handler",
    "validation_exception_handler",
    "pydantic_validation_handler",
    "global_exception_handler",
]
