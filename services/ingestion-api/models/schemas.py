"""
Pydantic request/response schemas for ingestion-api.

Defines data models for:
- File upload responses (IngestResponse)
- Batch status queries (BatchStatusResponse)
- Error responses (ErrorResponse)
- Health checks (HealthResponse)
- CSV upload form validation (CSVUploadRequest)
"""

from datetime import datetime, timezone
from typing import Literal, Optional
from uuid import UUID

from pydantic import BaseModel, Field, field_validator, ConfigDict


class IngestResponse(BaseModel):
    """Response for file upload endpoints (HTTP 202 Accepted).

    Returned immediately after file upload is accepted and queued for processing.
    Client should poll GET /api/v1/ingest/status/{batch_id} for progress.
    """

    batch_id: UUID = Field(
        ...,
        description="Unique batch identifier for tracking progress"
    )
    status: Literal["processing", "queued"] = Field(
        ...,
        description="Initial batch status (processing or queued)"
    )
    message: str = Field(
        ...,
        description="Human-readable status message",
        examples=["File upload accepted, processing started"]
    )
    total_records: Optional[int] = Field(
        None,
        description="Total records to process (null if unknown)",
        ge=0
    )
    source_name: str = Field(
        ...,
        description="Name of the data source being imported",
        examples=["Dane County 2025", "Wisconsin DOR RETR Q1 2025"]
    )
    source_type: Literal["PARCEL", "RETR", "DFI"] = Field(
        ...,
        description="Type of source data"
    )
    file_format: Literal["CSV", "GDB"] = Field(
        ...,
        description="Uploaded file format"
    )
    estimated_time_minutes: Optional[int] = Field(
        None,
        description="Estimated processing time in minutes (null if unknown)",
        ge=0
    )

    model_config = ConfigDict(
        json_schema_extra={
            "example": {
                "batch_id": "550e8400-e29b-41d4-a716-446655440000",
                "status": "processing",
                "message": "CSV file upload accepted, processing 12,450 records",
                "total_records": 12450,
                "source_name": "Wisconsin DOR RETR Q1 2025",
                "source_type": "RETR",
                "file_format": "CSV",
                "estimated_time_minutes": 3
            }
        })


class BatchStatusResponse(BaseModel):
    """Response for batch status endpoint (GET /api/v1/ingest/status/{batch_id}).

    Provides detailed progress information for an import batch.
    """

    batch_id: UUID = Field(
        ...,
        description="Unique batch identifier"
    )
    source_name: str = Field(
        ...,
        description="Name of the data source"
    )
    source_type: Literal["PARCEL", "RETR", "DFI"] = Field(
        ...,
        description="Type of source data"
    )
    file_format: Literal["CSV", "GDB"] = Field(
        ...,
        description="File format of the upload"
    )
    status: Literal["processing", "completed", "failed"] = Field(
        ...,
        description="Current batch status"
    )
    total_records: Optional[int] = Field(
        None,
        description="Total records in the batch (null if unknown)",
        ge=0
    )
    processed_records: int = Field(
        ...,
        description="Number of records processed so far",
        ge=0
    )
    new_records: int = Field(
        ...,
        description="Number of new (non-duplicate) records",
        ge=0
    )
    duplicate_records: int = Field(
        ...,
        description="Number of duplicate records skipped",
        ge=0
    )
    failed_records: int = Field(
        ...,
        description="Number of records that failed processing",
        ge=0
    )
    started_at: datetime = Field(
        ...,
        description="Batch processing start time (ISO 8601)"
    )
    completed_at: Optional[datetime] = Field(
        None,
        description="Batch processing completion time (null if still processing)"
    )
    error: Optional[str] = Field(
        None,
        description="Error message if status is 'failed' (null otherwise)"
    )
    progress_percent: Optional[float] = Field(
        None,
        description="Processing progress percentage (0-100, null if total_records unknown)",
        ge=0.0,
        le=100.0
    )

    @field_validator("progress_percent", mode="before")
    @classmethod
    def calculate_progress(cls, v: Optional[float], info) -> Optional[float]:
        """Calculate progress percentage from processed/total if not provided."""
        if v is not None:
            return v

        # Access other fields via info.data
        total = info.data.get("total_records")
        processed = info.data.get("processed_records", 0)

        if total and total > 0:
            return round((processed / total) * 100, 2)

        return None

    model_config = ConfigDict(
        json_schema_extra={
            "example": {
                "batch_id": "550e8400-e29b-41d4-a716-446655440000",
                "source_name": "Dane County 2025",
                "source_type": "PARCEL",
                "file_format": "GDB",
                "status": "processing",
                "total_records": 250000,
                "processed_records": 125000,
                "new_records": 120000,
                "duplicate_records": 5000,
                "failed_records": 0,
                "started_at": "2025-01-15T14:30:00Z",
                "completed_at": None,
                "error": None,
                "progress_percent": 50.0
            }
        })


class ErrorResponse(BaseModel):
    """Standard error response for all API errors.

    Provides consistent error format across all endpoints.
    """

    error: str = Field(
        ...,
        description="Error type or category",
        examples=["ValidationError", "FileUploadError", "DatabaseError"]
    )
    message: str = Field(
        ...,
        description="Human-readable error message",
        examples=["File size exceeds maximum allowed (5000 MB)"]
    )
    detail: Optional[dict] = Field(
        None,
        description="Additional error details (field-specific validation errors, etc.)"
    )
    timestamp: datetime = Field(
        default_factory=lambda: datetime.now(timezone.utc),
        description="Error timestamp (ISO 8601)"
    )

    model_config = ConfigDict(
        json_schema_extra={
            "example": {
                "error": "ValidationError",
                "message": "Invalid source_name: must contain only alphanumeric characters, spaces, hyphens, and underscores",
                "detail": {
                    "field": "source_name",
                    "value": "Test@Data!",
                    "constraint": "regex pattern: ^[a-zA-Z0-9_\\- ]+$"
                },
                "timestamp": "2025-01-15T14:35:22Z"
            }
        })


class HealthResponse(BaseModel):
    """Health check response (GET /health).

    Reports overall service health and dependency status.
    """

    status: Literal["healthy", "unhealthy", "degraded"] = Field(
        ...,
        description="Overall service health status"
    )
    timestamp: datetime = Field(
        default_factory=lambda: datetime.now(timezone.utc),
        description="Health check timestamp (ISO 8601)"
    )
    services: dict[str, str] = Field(
        ...,
        description="Status of individual service dependencies (database, rabbitmq, etc.)"
    )
    version: Optional[str] = Field(
        None,
        description="Service version (from environment or package metadata)"
    )

    model_config = ConfigDict(
        json_schema_extra={
            "example": {
                "status": "healthy",
                "timestamp": "2025-01-15T14:40:00Z",
                "services": {
                    "database": "healthy",
                    "rabbitmq": "healthy",
                    "storage": "healthy"
                },
                "version": "1.0.0"
            }
        }
    )


class CSVUploadRequest(BaseModel):
    """Form data validation for CSV uploads.

    Validates source_name field from multipart form data.
    Note: File validation happens in the router (file size, extension, etc.).
    """

    source_name: str = Field(
        ...,
        description="Name of the data source being imported",
        min_length=1,
        max_length=255,
        examples=["Dane County 2025", "Wisconsin DOR RETR Q1 2025"]
    )

    @field_validator("source_name")
    @classmethod
    def validate_source_name(cls, v: str) -> str:
        """Validate source_name contains only safe characters.

        Allowed: alphanumeric, spaces, hyphens, underscores
        Disallowed: special characters that could cause issues in filenames or queries
        """
        import re

        # Strip leading/trailing whitespace
        v = v.strip()

        # Check pattern: alphanumeric, spaces, hyphens, underscores only
        if not re.match(r'^[a-zA-Z0-9_\- ]+$', v):
            raise ValueError(
                "source_name must contain only alphanumeric characters, spaces, hyphens, and underscores"
            )

        return v

    model_config = ConfigDict(
        json_schema_extra={
            "example": {
                "source_name": "Dane County 2025"
            }
        })


class GDBUploadRequest(BaseModel):
    """Form data validation for GDB uploads.

    Validates source_name and optional layer_name from multipart form data.
    """

    source_name: str = Field(
        ...,
        description="Name of the data source being imported",
        min_length=1,
        max_length=255,
        examples=["Dane County V11 Parcels 2025"]
    )
    layer_name: Optional[str] = Field(
        None,
        description="GDB layer name to process (defaults to 'V11_Parcels')",
        min_length=1,
        max_length=255,
        examples=["V11_Parcels", "County_Parcels"]
    )

    @field_validator("source_name", "layer_name")
    @classmethod
    def validate_name(cls, v: Optional[str]) -> Optional[str]:
        """Validate names contain only safe characters."""
        if v is None:
            return v

        import re

        v = v.strip()

        # Check pattern: alphanumeric, spaces, hyphens, underscores only
        if not re.match(r'^[a-zA-Z0-9_\- ]+$', v):
            raise ValueError(
                "Names must contain only alphanumeric characters, spaces, hyphens, and underscores"
            )

        return v

    model_config = ConfigDict(
        json_schema_extra={
            "example": {
                "source_name": "Dane County V11 Parcels 2025",
                "layer_name": "V11_Parcels"
            }
        })
