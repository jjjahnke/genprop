"""
Batch status tracking endpoints.

Provides REST API endpoints for checking batch processing status:
- GET /api/v1/ingest/status/{batch_id} - Get batch progress and statistics
"""

import logging
from uuid import UUID

from fastapi import APIRouter, HTTPException
from fastapi.responses import JSONResponse

from models.schemas import BatchStatusResponse, ErrorResponse
from services.batch_tracker import fetch_batch

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/v1/ingest", tags=["status"])


@router.get(
    "/status/{batch_id}",
    response_model=BatchStatusResponse,
    summary="Get batch processing status",
    description="""
    Retrieve detailed progress information for a specific import batch.

    Returns processing status including:
    - Progress percentage (if total_records known)
    - Counts: processed, new, duplicate, failed records
    - Timestamps: started_at, completed_at
    - Error information (if status is 'failed')

    Use this endpoint to poll for completion after uploading files via:
    - POST /api/v1/ingest/parcel/csv
    - POST /api/v1/ingest/parcel/gdb
    - POST /api/v1/ingest/retr

    **Status Values:**
    - `processing`: Batch is currently being processed
    - `completed`: All records processed successfully
    - `failed`: Batch processing encountered a fatal error

    **Response Codes:**
    - 200 OK: Batch found, status returned
    - 404 Not Found: No batch with the given ID exists
    - 500 Internal Server Error: Database error
    """
)
async def get_batch_status(batch_id: UUID) -> BatchStatusResponse:
    """
    Get processing status for a batch.

    Args:
        batch_id: UUID of the batch to query

    Returns:
        BatchStatusResponse with progress and statistics

    Raises:
        HTTPException: 404 if batch not found, 500 on database error
    """
    logger.info(f"Fetching status for batch {batch_id}")

    try:
        # Fetch batch from database
        batch = await fetch_batch(batch_id)

        if not batch:
            logger.warning(f"Batch {batch_id} not found")
            raise HTTPException(
                status_code=404,
                detail={
                    "error": "BatchNotFound",
                    "message": f"No batch found with ID {batch_id}",
                    "detail": {"batch_id": str(batch_id)}
                }
            )

        # Calculate progress percentage
        progress_percent = None
        if batch["total_records"] and batch["total_records"] > 0:
            progress_percent = round(
                (batch["processed_records"] / batch["total_records"]) * 100,
                2
            )

        # Build response
        response = BatchStatusResponse(
            batch_id=batch["batch_id"],
            source_name=batch["source_name"],
            source_type=batch["source_type"],
            file_format=batch["file_format"],
            status=batch["status"],
            total_records=batch["total_records"],
            processed_records=batch["processed_records"],
            new_records=batch["new_records"],
            duplicate_records=batch["duplicate_records"],
            failed_records=batch["failed_records"],
            started_at=batch["started_at"],
            completed_at=batch["completed_at"],
            error=batch["error"],
            progress_percent=progress_percent
        )

        logger.info(
            f"Batch {batch_id}: {response.status}, "
            f"{response.processed_records}/{response.total_records or '?'} records"
        )

        return response

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error fetching batch status for {batch_id}: {e}", exc_info=True)
        raise HTTPException(
            status_code=500,
            detail={
                "error": "InternalServerError",
                "message": "Failed to fetch batch status",
                "detail": {"error": str(e)}
            }
        )


__all__ = ["router"]
