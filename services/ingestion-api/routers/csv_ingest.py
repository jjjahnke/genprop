"""
CSV ingestion endpoints.

Provides REST API endpoints for uploading CSV files:
- POST /api/v1/ingest/parcel/csv - Upload parcel CSV
- POST /api/v1/ingest/retr - Upload RETR CSV
"""

import logging
from pathlib import Path
from typing import Literal
import aiofiles
from fastapi import APIRouter, BackgroundTasks, File, Form, HTTPException, UploadFile
from fastapi.responses import JSONResponse
from pydantic import ValidationError

from models.schemas import CSVUploadRequest, IngestResponse, ErrorResponse
from services.csv_processor import process_csv_async, count_csv_rows, validate_csv_format
from services.batch_tracker import create_batch
from config import Settings

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/v1/ingest", tags=["csv-ingestion"])

# Initialize settings
settings = Settings()


async def save_upload_file(upload_file: UploadFile, destination: Path) -> int:
    """
    Save uploaded file to disk.

    Args:
        upload_file: The uploaded file
        destination: Destination path

    Returns:
        int: Number of bytes written

    Raises:
        IOError: If file write fails
    """
    bytes_written = 0

    # Ensure parent directory exists
    destination.parent.mkdir(parents=True, exist_ok=True)

    # Write file in chunks
    async with aiofiles.open(destination, 'wb') as f:
        while chunk := await upload_file.read(1024 * 1024):  # 1MB chunks
            await f.write(chunk)
            bytes_written += len(chunk)

    logger.info(f"Saved upload file to {destination} ({bytes_written} bytes)")
    return bytes_written


def validate_file_extension(
    filename: str,
    allowed_extensions: list[str]
) -> None:
    """
    Validate file has an allowed extension.

    Args:
        filename: The uploaded filename
        allowed_extensions: List of allowed extensions (e.g., ['.csv', '.txt'])

    Raises:
        HTTPException: 400 if extension not allowed
    """
    file_ext = Path(filename).suffix.lower()

    if file_ext not in allowed_extensions:
        raise HTTPException(
            status_code=400,
            detail={
                "error": "InvalidFileType",
                "message": f"Invalid file extension '{file_ext}'. Allowed: {', '.join(allowed_extensions)}",
                "detail": {
                    "filename": filename,
                    "extension": file_ext,
                    "allowed_extensions": allowed_extensions
                }
            }
        )


def validate_file_size(
    file_size: int,
    max_size_bytes: int
) -> None:
    """
    Validate file size is within limits.

    Args:
        file_size: File size in bytes
        max_size_bytes: Maximum allowed size in bytes

    Raises:
        HTTPException: 413 if file too large
    """
    if file_size > max_size_bytes:
        max_mb = max_size_bytes / (1024 * 1024)
        actual_mb = file_size / (1024 * 1024)

        raise HTTPException(
            status_code=413,
            detail={
                "error": "FileTooLarge",
                "message": f"File size ({actual_mb:.1f} MB) exceeds maximum ({max_mb:.0f} MB)",
                "detail": {
                    "file_size_bytes": file_size,
                    "file_size_mb": round(actual_mb, 2),
                    "max_size_mb": int(max_mb)
                }
            }
        )


@router.post(
    "/parcel/csv",
    response_model=IngestResponse,
    status_code=202,
    summary="Upload parcel CSV file",
    description="""
    Upload a CSV file containing Wisconsin V11 parcel records.

    The CSV must include:
    - geometry_wkt: Well-Known Text geometry (required)
    - geometry_type: Geometry type (Polygon, MultiPolygon) (required)
    - V11 fields: STATEID, PARCELID, ADDNUM, STREETNAME, etc. (optional)

    Returns immediately with HTTP 202 Accepted and a batch_id.
    Poll GET /api/v1/ingest/status/{batch_id} for progress.
    """
)
async def upload_parcel_csv(
    background_tasks: BackgroundTasks,
    file: UploadFile = File(..., description="CSV file containing parcel records"),
    source_name: str = Form(..., description="Name of the data source (e.g., 'Dane County 2025')")
) -> IngestResponse:
    """
    Upload and process a parcel CSV file.

    Args:
        background_tasks: FastAPI background tasks
        file: Uploaded CSV file
        source_name: Name of the data source

    Returns:
        IngestResponse with batch_id and status
    """
    logger.info(f"Received parcel CSV upload: {file.filename} from {source_name}")

    try:
        # Validate source_name using Pydantic
        try:
            upload_request = CSVUploadRequest(source_name=source_name)
            validated_source_name = upload_request.source_name
        except ValidationError as e:
            raise HTTPException(
                status_code=422,
                detail={
                    "error": "ValidationError",
                    "message": str(e).split('\n')[0],  # First line of error
                    "detail": None
                }
            )

        # Validate file extension
        validate_file_extension(file.filename, settings.ALLOWED_CSV_EXTENSIONS)

        # Read file size from content-length or by reading
        file_size = 0
        if file.size:
            file_size = file.size
            validate_file_size(file_size, settings.max_upload_size_bytes)

        # Save file to temp storage
        temp_dir = Path(settings.TEMP_STORAGE_PATH) / "csv"
        temp_file = temp_dir / f"{validated_source_name.replace(' ', '_')}_{file.filename}"

        file_size_bytes = await save_upload_file(file, temp_file)
        validate_file_size(file_size_bytes, settings.max_upload_size_bytes)

        # Validate it's actually a CSV
        is_valid, validation_error = validate_csv_format(temp_file)
        if not is_valid:
            temp_file.unlink()  # Delete invalid file
            raise HTTPException(
                status_code=400,
                detail={
                    "error": "InvalidCSVFormat",
                    "message": validation_error or "File is not a valid CSV or cannot be parsed",
                    "detail": {"filename": file.filename}
                }
            )

        # Count rows (for progress tracking)
        try:
            total_rows = await count_csv_rows(temp_file)
        except Exception as e:
            logger.warning(f"Could not count CSV rows: {e}")
            total_rows = None

        # Create batch record
        batch_id = await create_batch(
            source_name=validated_source_name,
            source_type="PARCEL",
            file_format="CSV",
            file_size_bytes=file_size_bytes,
            total_records=total_rows
        )

        # Start background processing
        background_tasks.add_task(
            process_csv_async,
            csv_path=temp_file,
            source_type="PARCEL",
            batch_id=batch_id,
            source_name=validated_source_name,
            chunk_size=settings.BATCH_SIZE
        )

        # Calculate estimated time (rough estimate: 5000 records/sec)
        estimated_minutes = None
        if total_rows:
            estimated_seconds = total_rows / 5000
            estimated_minutes = max(1, int(estimated_seconds / 60))

        logger.info(
            f"Parcel CSV upload accepted: batch_id={batch_id}, rows={total_rows}"
        )

        return IngestResponse(
            batch_id=batch_id,
            status="processing",
            message=f"CSV file upload accepted, processing {total_rows or 'unknown'} records",
            total_records=total_rows,
            source_name=validated_source_name,
            source_type="PARCEL",
            file_format="CSV",
            estimated_time_minutes=estimated_minutes
        )

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error processing parcel CSV upload: {e}", exc_info=True)
        raise HTTPException(
            status_code=500,
            detail={
                "error": "InternalServerError",
                "message": "Failed to process CSV upload",
                "detail": {"error": str(e)}
            }
        )


@router.post(
    "/retr",
    response_model=IngestResponse,
    status_code=202,
    summary="Upload RETR CSV file",
    description="""
    Upload a CSV file containing Real Estate Transfer Return (RETR) records.

    The CSV should include RETR fields:
    - PARCEL_ID: Parcel identifier
    - DOC_NUMBER: Document number
    - TRANSFER_DATE: Transfer date
    - SALE_AMOUNT: Sale amount
    - GRANTOR, GRANTEE: Parties to the transfer
    - etc.

    Returns immediately with HTTP 202 Accepted and a batch_id.
    Poll GET /api/v1/ingest/status/{batch_id} for progress.
    """
)
async def upload_retr_csv(
    background_tasks: BackgroundTasks,
    file: UploadFile = File(..., description="CSV file containing RETR records"),
    source_name: str = Form(..., description="Name of the data source (e.g., 'Wisconsin DOR RETR Q1 2025')")
) -> IngestResponse:
    """
    Upload and process a RETR CSV file.

    Args:
        background_tasks: FastAPI background tasks
        file: Uploaded CSV file
        source_name: Name of the data source

    Returns:
        IngestResponse with batch_id and status
    """
    logger.info(f"Received RETR CSV upload: {file.filename} from {source_name}")

    try:
        # Validate source_name using Pydantic
        upload_request = CSVUploadRequest(source_name=source_name)
        validated_source_name = upload_request.source_name

        # Validate file extension
        validate_file_extension(file.filename, settings.ALLOWED_CSV_EXTENSIONS)

        # Read file size
        file_size = 0
        if file.size:
            file_size = file.size
            validate_file_size(file_size, settings.max_upload_size_bytes)

        # Save file to temp storage
        temp_dir = Path(settings.TEMP_STORAGE_PATH) / "csv"
        temp_file = temp_dir / f"{validated_source_name.replace(' ', '_')}_{file.filename}"

        file_size_bytes = await save_upload_file(file, temp_file)
        validate_file_size(file_size_bytes, settings.max_upload_size_bytes)

        # Validate it's actually a CSV
        is_valid, validation_error = validate_csv_format(temp_file)
        if not is_valid:
            temp_file.unlink()  # Delete invalid file
            raise HTTPException(
                status_code=400,
                detail={
                    "error": "InvalidCSVFormat",
                    "message": validation_error or "File is not a valid CSV or cannot be parsed",
                    "detail": {"filename": file.filename}
                }
            )

        # Count rows
        try:
            total_rows = await count_csv_rows(temp_file)
        except Exception as e:
            logger.warning(f"Could not count CSV rows: {e}")
            total_rows = None

        # Create batch record
        batch_id = await create_batch(
            source_name=validated_source_name,
            source_type="RETR",
            file_format="CSV",
            file_size_bytes=file_size_bytes,
            total_records=total_rows
        )

        # Start background processing
        background_tasks.add_task(
            process_csv_async,
            csv_path=temp_file,
            source_type="RETR",
            batch_id=batch_id,
            source_name=validated_source_name,
            chunk_size=settings.BATCH_SIZE
        )

        # Calculate estimated time
        estimated_minutes = None
        if total_rows:
            estimated_seconds = total_rows / 5000
            estimated_minutes = max(1, int(estimated_seconds / 60))

        logger.info(
            f"RETR CSV upload accepted: batch_id={batch_id}, rows={total_rows}"
        )

        return IngestResponse(
            batch_id=batch_id,
            status="processing",
            message=f"CSV file upload accepted, processing {total_rows or 'unknown'} records",
            total_records=total_rows,
            source_name=validated_source_name,
            source_type="RETR",
            file_format="CSV",
            estimated_time_minutes=estimated_minutes
        )

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error processing RETR CSV upload: {e}", exc_info=True)
        raise HTTPException(
            status_code=500,
            detail={
                "error": "InternalServerError",
                "message": "Failed to process CSV upload",
                "detail": {"error": str(e)}
            }
        )


__all__ = ["router"]
