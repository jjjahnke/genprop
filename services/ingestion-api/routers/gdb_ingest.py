"""
GDB ingestion endpoints.

Provides REST API endpoints for uploading GDB files:
- POST /api/v1/ingest/parcel/gdb - Upload parcel GDB
"""

import logging
from pathlib import Path
from typing import Optional
import aiofiles
from fastapi import APIRouter, BackgroundTasks, File, Form, HTTPException, UploadFile
from fastapi.responses import JSONResponse
from pydantic import ValidationError

from models.schemas import GDBUploadRequest, IngestResponse, ErrorResponse
from services.gdb_processor import (
    extract_gdb,
    inspect_gdb,
    count_features,
    process_gdb_async,
    validate_gdb_format,
    cleanup_gdb
)
from services.batch_tracker import create_batch
from config import Settings

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/v1/ingest", tags=["gdb-ingestion"])

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
        allowed_extensions: List of allowed extensions

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
    "/parcel/gdb",
    response_model=IngestResponse,
    status_code=202,
    summary="Upload parcel GDB file",
    description="""
    Upload a GDB (Geodatabase) file containing Wisconsin V11 parcel records.

    The file should be a .gdb.zip archive containing a valid ESRI Geodatabase.

    Required fields:
    - geometry_wkt: Well-Known Text geometry (WGS84 or Wisconsin TM)
    - geometry_type: Polygon or MultiPolygon
    - V11 fields: STATEID, PARCELID, etc.

    Optional parameters:
    - layer_name: Specific layer to process (default: "V11_Parcels")

    The GDB will be:
    1. Extracted and inspected
    2. Validated for correct CRS and schema
    3. Transformed to EPSG:3071 if needed
    4. Processed in chunks and published to RabbitMQ

    Returns immediately with HTTP 202 Accepted and a batch_id.
    Poll GET /api/v1/ingest/status/{batch_id} for progress.
    """
)
async def upload_parcel_gdb(
    background_tasks: BackgroundTasks,
    file: UploadFile = File(..., description="GDB .zip file containing parcel records"),
    source_name: str = Form(..., description="Name of the data source (e.g., 'Dane County 2025')"),
    layer_name: Optional[str] = Form(None, description="Layer name to process (default: V11_Parcels)")
) -> IngestResponse:
    """
    Upload and process a parcel GDB file.

    Args:
        background_tasks: FastAPI background tasks
        file: Uploaded GDB zip file
        source_name: Name of the data source
        layer_name: Optional layer name (defaults to settings.DEFAULT_LAYER_NAME)

    Returns:
        IngestResponse with batch_id and status
    """
    logger.info(f"Received GDB upload: {file.filename} from {source_name}")

    # Use default layer name if not provided
    if layer_name is None:
        layer_name = settings.DEFAULT_LAYER_NAME

    try:
        # Validate form data using Pydantic
        try:
            upload_request = GDBUploadRequest(
                source_name=source_name,
                layer_name=layer_name
            )
            validated_source_name = upload_request.source_name
            validated_layer_name = upload_request.layer_name or settings.DEFAULT_LAYER_NAME

        except ValidationError as e:
            raise HTTPException(
                status_code=422,
                detail={
                    "error": "ValidationError",
                    "message": str(e).split('\n')[0],
                    "detail": None
                }
            )

        # Validate file extension
        validate_file_extension(file.filename, settings.ALLOWED_GDB_EXTENSIONS)

        # Check file size
        file_size = 0
        if file.size:
            file_size = file.size
            validate_file_size(file_size, settings.max_upload_size_bytes)

        # Save file to temp storage
        temp_dir = Path(settings.TEMP_STORAGE_PATH) / "gdb"
        safe_source_name = validated_source_name.replace(' ', '_').replace('/', '_')
        temp_file = temp_dir / f"{safe_source_name}_{file.filename}"

        file_size_bytes = await save_upload_file(file, temp_file)
        validate_file_size(file_size_bytes, settings.max_upload_size_bytes)

        # Extract the GDB
        extract_dir = temp_dir / f"{safe_source_name}_extract"
        try:
            gdb_path = extract_gdb(temp_file, extract_dir)
        except Exception as e:
            # Cleanup on extraction failure
            temp_file.unlink(missing_ok=True)
            if extract_dir.exists():
                cleanup_gdb(extract_dir)

            raise HTTPException(
                status_code=400,
                detail={
                    "error": "InvalidGDBFormat",
                    "message": f"Failed to extract GDB: {str(e)}",
                    "detail": {"filename": file.filename}
                }
            )

        # Validate it's a valid GDB
        if not validate_gdb_format(gdb_path):
            # Cleanup on validation failure
            temp_file.unlink(missing_ok=True)
            cleanup_gdb(extract_dir)

            raise HTTPException(
                status_code=400,
                detail={
                    "error": "InvalidGDBFormat",
                    "message": "File is not a valid GDB or cannot be read",
                    "detail": {"filename": file.filename}
                }
            )

        # Inspect the GDB
        try:
            gdb_info = inspect_gdb(gdb_path)
        except Exception as e:
            # Cleanup on inspection failure
            temp_file.unlink(missing_ok=True)
            cleanup_gdb(extract_dir)

            raise HTTPException(
                status_code=400,
                detail={
                    "error": "GDBInspectionError",
                    "message": f"Failed to inspect GDB: {str(e)}",
                    "detail": {"filename": file.filename}
                }
            )

        # Verify the requested layer exists
        if validated_layer_name not in gdb_info["layers"]:
            # Cleanup
            temp_file.unlink(missing_ok=True)
            cleanup_gdb(extract_dir)

            raise HTTPException(
                status_code=400,
                detail={
                    "error": "LayerNotFound",
                    "message": f"Layer '{validated_layer_name}' not found in GDB",
                    "detail": {
                        "requested_layer": validated_layer_name,
                        "available_layers": gdb_info["layers"]
                    }
                }
            )

        # Get feature count for the layer
        layer_info = gdb_info["layer_info"].get(validated_layer_name, {})
        total_features = layer_info.get("feature_count")

        logger.info(
            f"GDB inspection complete: {len(gdb_info['layers'])} layer(s), "
            f"processing '{validated_layer_name}' with {total_features:,} features"
        )

        # Create batch record
        batch_id = await create_batch(
            source_name=validated_source_name,
            source_type="PARCEL",
            file_format="GDB",
            file_size_bytes=file_size_bytes,
            total_records=total_features
        )

        # Start background processing
        background_tasks.add_task(
            process_gdb_async,
            gdb_path=gdb_path,
            layer_name=validated_layer_name,
            batch_id=batch_id,
            source_name=validated_source_name,
            chunk_size=settings.BATCH_SIZE
        )

        # Cleanup temp zip after extraction (GDB dir will be cleaned up after processing)
        background_tasks.add_task(temp_file.unlink, missing_ok=True)
        background_tasks.add_task(cleanup_gdb, extract_dir)

        # Calculate estimated time (rough estimate: 2000 features/sec for GDB)
        estimated_minutes = None
        if total_features:
            estimated_seconds = total_features / 2000  # GDB slower than CSV
            estimated_minutes = max(1, int(estimated_seconds / 60))

        logger.info(
            f"GDB upload accepted: batch_id={batch_id}, "
            f"layer={validated_layer_name}, features={total_features:,}"
        )

        return IngestResponse(
            batch_id=batch_id,
            status="processing",
            message=f"GDB file upload accepted, processing {total_features:,} features from layer '{validated_layer_name}'",
            total_records=total_features,
            source_name=validated_source_name,
            source_type="PARCEL",
            file_format="GDB",
            estimated_time_minutes=estimated_minutes
        )

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error processing GDB upload: {e}", exc_info=True)
        raise HTTPException(
            status_code=500,
            detail={
                "error": "InternalServerError",
                "message": "Failed to process GDB upload",
                "detail": {"error": str(e)}
            }
        )


__all__ = ["router"]
