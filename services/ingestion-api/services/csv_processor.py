"""
CSV file processing service.

Handles CSV file uploads for PARCEL, RETR, and DFI data:
- Stream processing with memory-efficient chunking
- Encoding detection and validation
- Column validation based on source type
- RabbitMQ message publishing
- Batch progress tracking
"""

import logging
from pathlib import Path
from typing import Literal, Optional
from uuid import UUID
import pandas as pd
import chardet

from shared.models import V11ParcelRecord, RETRRecord, DFIRecord
from shared.rabbitmq import publish_message
from .batch_tracker import update_batch_progress, complete_batch, fail_batch

logger = logging.getLogger(__name__)

# Source type to model mapping
SOURCE_TYPE_MODELS = {
    "PARCEL": V11ParcelRecord,
    "RETR": RETRRecord,
    "DFI": DFIRecord,
}


def detect_encoding(file_path: Path) -> str:
    """
    Detect CSV file encoding.

    Reads first 100KB of file to determine encoding (UTF-8, latin-1, etc.).

    Args:
        file_path: Path to the CSV file

    Returns:
        str: Detected encoding (e.g., 'utf-8', 'iso-8859-1')
    """
    with open(file_path, 'rb') as f:
        raw_data = f.read(100000)  # Read first 100KB
        result = chardet.detect(raw_data)
        encoding = result['encoding']
        confidence = result['confidence']

        logger.info(
            f"Detected encoding: {encoding} (confidence: {confidence:.2%})"
        )

        # Default to UTF-8 if confidence is low
        if confidence < 0.7:
            logger.warning(
                f"Low confidence encoding detection ({confidence:.2%}), defaulting to UTF-8"
            )
            return 'utf-8'

        return encoding


def validate_csv_columns(
    df: pd.DataFrame,
    source_type: Literal["PARCEL", "RETR", "DFI"]
) -> None:
    """
    Validate that CSV has required columns for the source type.

    Args:
        df: DataFrame to validate
        source_type: Type of source data

    Raises:
        ValueError: If required columns are missing
    """
    # Get model fields for this source type
    model = SOURCE_TYPE_MODELS[source_type]
    model_fields = set(model.model_fields.keys())

    # Get CSV columns (case-insensitive matching)
    csv_columns = set(col.upper() for col in df.columns)

    # For PARCEL data, geometry fields are required
    if source_type == "PARCEL":
        required_fields = {"geometry_wkt", "geometry_type"}
        required_upper = {f.upper() for f in required_fields}
        missing = required_upper - csv_columns

        if missing:
            raise ValueError(
                f"Missing required geometry columns: {', '.join(missing)}. "
                f"PARCEL CSV must include geometry_wkt and geometry_type."
            )

    # Log column match statistics
    matched = csv_columns & {f.upper() for f in model_fields}
    logger.info(
        f"CSV validation: {len(matched)}/{len(model_fields)} model fields matched"
    )


async def process_csv_async(
    csv_path: Path,
    source_type: Literal["PARCEL", "RETR", "DFI"],
    batch_id: UUID,
    source_name: str,
    chunk_size: int = 1000
) -> None:
    """
    Process a CSV file asynchronously.

    Reads CSV in chunks, validates data, and publishes to RabbitMQ.
    Updates batch progress throughout processing.

    Args:
        csv_path: Path to the CSV file
        source_type: Type of source data ('PARCEL', 'RETR', 'DFI')
        batch_id: Import batch ID for tracking
        source_name: Name of the data source
        chunk_size: Number of rows to process per chunk (default: 1000)

    Raises:
        Exception: Any processing errors (caller should catch and fail_batch)
    """
    logger.info(
        f"Starting CSV processing: {csv_path} (batch: {batch_id}, type: {source_type})"
    )

    try:
        # Detect encoding
        encoding = detect_encoding(csv_path)

        # Get the appropriate Pydantic model for this source type
        model_class = SOURCE_TYPE_MODELS[source_type]

        # Read CSV in chunks for memory efficiency
        total_processed = 0
        total_failed = 0
        chunk_num = 0

        # First pass: validate columns with first chunk
        first_chunk = pd.read_csv(csv_path, encoding=encoding, nrows=100)
        validate_csv_columns(first_chunk, source_type)

        # Process CSV in chunks
        for chunk in pd.read_csv(
            csv_path,
            encoding=encoding,
            chunksize=chunk_size,
            dtype=str,  # Read all as strings, let Pydantic handle type conversion
            keep_default_na=False  # Don't convert empty strings to NaN
        ):
            chunk_num += 1
            logger.debug(f"Processing chunk {chunk_num} ({len(chunk)} rows)")

            # Normalize column names to match model fields (case-insensitive)
            chunk.columns = chunk.columns.str.strip()

            # Process each row in the chunk
            chunk_failed = 0
            for idx, row in chunk.iterrows():
                try:
                    # Convert row to dict, removing empty strings
                    row_dict = {
                        k: (v if pd.notna(v) and v != '' else None)
                        for k, v in row.to_dict().items()
                    }

                    # Validate with Pydantic model
                    record = model_class(**row_dict)

                    # Publish to RabbitMQ deduplication queue
                    message = {
                        "batch_id": str(batch_id),
                        "source_type": source_type,
                        "source_file": source_name,
                        "source_row_number": int(total_processed + idx + 1),
                        "raw_data": record.model_dump(exclude_none=True)
                    }

                    success = publish_message('deduplication', message)

                    if not success:
                        logger.error(
                            f"Failed to publish message for row {total_processed + idx + 1}"
                        )
                        chunk_failed += 1

                except Exception as e:
                    logger.warning(
                        f"Failed to process row {total_processed + idx + 1}: {e}"
                    )
                    chunk_failed += 1

            # Update batch progress after each chunk
            chunk_processed = len(chunk)
            chunk_successful = chunk_processed - chunk_failed

            await update_batch_progress(
                batch_id=batch_id,
                processed_count=chunk_processed,
                new_count=chunk_successful,  # Will be updated by deduplication service
                failed_count=chunk_failed
            )

            total_processed += chunk_processed
            total_failed += chunk_failed

            logger.info(
                f"Chunk {chunk_num} complete: {chunk_successful}/{chunk_processed} succeeded"
            )

        # Mark batch as completed
        await complete_batch(batch_id, total_processed)

        logger.info(
            f"CSV processing complete: {total_processed} rows processed, "
            f"{total_failed} failed (batch: {batch_id})"
        )

    except Exception as e:
        logger.error(f"CSV processing failed (batch: {batch_id}): {e}", exc_info=True)
        await fail_batch(batch_id, f"CSV processing error: {str(e)}")
        raise


async def count_csv_rows(
    csv_path: Path,
    encoding: Optional[str] = None
) -> int:
    """
    Count total rows in a CSV file efficiently.

    Args:
        csv_path: Path to the CSV file
        encoding: File encoding (auto-detected if None)

    Returns:
        int: Number of rows in the CSV (excluding header)
    """
    if encoding is None:
        encoding = detect_encoding(csv_path)

    # Use wc-style counting for speed (read in chunks)
    row_count = 0
    for chunk in pd.read_csv(csv_path, encoding=encoding, chunksize=10000):
        row_count += len(chunk)

    logger.info(f"CSV row count: {row_count}")
    return row_count


def validate_csv_format(csv_path: Path) -> bool:
    """
    Validate that a file is a valid CSV.

    Args:
        csv_path: Path to the CSV file

    Returns:
        bool: True if valid CSV, False otherwise
    """
    try:
        encoding = detect_encoding(csv_path)
        # Try to read first 10 rows
        pd.read_csv(csv_path, encoding=encoding, nrows=10)
        return True
    except Exception as e:
        logger.error(f"CSV validation failed: {e}")
        return False


__all__ = [
    "process_csv_async",
    "count_csv_rows",
    "validate_csv_format",
    "detect_encoding",
    "validate_csv_columns",
]
