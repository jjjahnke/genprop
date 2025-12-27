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
from .logging_utils import get_logger, set_batch_id
from .background_utils import safe_background_task

logger = get_logger(__name__)

# Source type to model mapping
SOURCE_TYPE_MODELS = {
    "PARCEL": V11ParcelRecord,
    "RETR": RETRRecord,
    "DFI": DFIRecord,
}


def detect_encoding(file_path: Path, sample_size: int = 500000) -> str:
    """
    Detect CSV file encoding with fallback chain.

    Reads a sample of the file to determine encoding, then validates
    by attempting to decode the entire file. Falls back through common
    encodings if detection fails.

    Args:
        file_path: Path to the CSV file
        sample_size: Number of bytes to sample (default: 500KB)

    Returns:
        str: Detected encoding (e.g., 'utf-8', 'iso-8859-1', 'cp1252')
    """
    # Read larger sample for better detection (500KB default)
    with open(file_path, 'rb') as f:
        raw_data = f.read(sample_size)
        result = chardet.detect(raw_data)
        detected_encoding = result['encoding']
        confidence = result['confidence']

    logger.info(
        f"Detected encoding: {detected_encoding} (confidence: {confidence:.2%})"
    )

    # ASCII detection is often incorrect - treat as UTF-8
    if detected_encoding and detected_encoding.lower() == 'ascii':
        logger.info("ASCII detected, treating as UTF-8 for broader compatibility")
        detected_encoding = 'utf-8'

    # Fallback encoding chain (in order of preference)
    encoding_chain = []

    # Start with detected encoding if confidence is reasonable
    if detected_encoding and confidence >= 0.6:
        encoding_chain.append(detected_encoding)

    # Common encodings for US data
    encoding_chain.extend(['utf-8', 'latin-1', 'cp1252', 'iso-8859-1'])

    # Remove duplicates while preserving order
    seen = set()
    encoding_chain = [
        enc for enc in encoding_chain
        if enc and enc.lower() not in seen and not seen.add(enc.lower())
    ]

    # Try each encoding by attempting to read first chunk
    for encoding in encoding_chain:
        try:
            # Test by reading first 10 rows with pandas
            pd.read_csv(file_path, encoding=encoding, nrows=10)
            logger.info(f"Validated encoding: {encoding}")
            return encoding
        except (UnicodeDecodeError, UnicodeError):
            logger.debug(f"Encoding {encoding} failed, trying next...")
            continue
        except Exception as e:
            # Other errors (malformed CSV, etc.) - not encoding related
            logger.debug(f"Non-encoding error with {encoding}: {e}")
            continue

    # Last resort: UTF-8 with error handling
    logger.warning(
        "All encoding attempts failed, using UTF-8 with error replacement. "
        "Some characters may be corrupted."
    )
    return 'utf-8'


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


@safe_background_task
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
    # Set batch_id in logging context
    set_batch_id(batch_id)

    logger.info(
        f"Starting CSV processing: {csv_path} (batch: {batch_id}, type: {source_type})"
    )

    try:
        # Detect encoding with robust fallback
        encoding = detect_encoding(csv_path)

        # Get the appropriate Pydantic model for this source type
        model_class = SOURCE_TYPE_MODELS[source_type]

        # Read CSV in chunks for memory efficiency
        total_processed = 0
        total_failed = 0
        chunk_num = 0

        # First pass: validate columns with first chunk
        try:
            first_chunk = pd.read_csv(
                csv_path,
                encoding=encoding,
                nrows=100,
                encoding_errors='replace'  # Replace bad characters instead of failing
            )
            validate_csv_columns(first_chunk, source_type)
        except UnicodeDecodeError as e:
            # If encoding still fails, try one more time with latin-1 (never fails)
            logger.warning(
                f"Encoding {encoding} failed during validation, falling back to latin-1: {e}"
            )
            encoding = 'latin-1'
            first_chunk = pd.read_csv(csv_path, encoding=encoding, nrows=100)
            validate_csv_columns(first_chunk, source_type)

        # Process CSV in chunks
        for chunk in pd.read_csv(
            csv_path,
            encoding=encoding,
            chunksize=chunk_size,
            dtype=str,  # Read all as strings, let Pydantic handle type conversion
            keep_default_na=False,  # Don't convert empty strings to NaN
            encoding_errors='replace',  # Replace bad characters with � instead of crashing
            on_bad_lines='warn'  # Log bad lines but continue processing
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
    for chunk in pd.read_csv(
        csv_path,
        encoding=encoding,
        chunksize=10000,
        encoding_errors='replace',  # Handle encoding errors gracefully
        on_bad_lines='skip'  # Skip bad lines when counting
    ):
        row_count += len(chunk)

    logger.info(f"CSV row count: {row_count}")
    return row_count


def validate_csv_format(csv_path: Path) -> tuple[bool, Optional[str]]:
    """
    Validate that a file is a valid CSV.

    Args:
        csv_path: Path to the CSV file

    Returns:
        tuple: (is_valid, error_message)
            - is_valid: True if valid CSV, False otherwise
            - error_message: Description of validation failure, or None if valid
    """
    try:
        # Check file is not empty
        if csv_path.stat().st_size == 0:
            return False, "File is empty (0 bytes)"

        # Detect encoding
        encoding = detect_encoding(csv_path)

        # Try to read first 10 rows
        df = pd.read_csv(
            csv_path,
            encoding=encoding,
            nrows=10,
            encoding_errors='replace',
            on_bad_lines='skip'
        )

        # Check that we got some data
        if len(df) == 0:
            return False, "CSV contains no data rows"

        # Check that we have at least one column
        if len(df.columns) == 0:
            return False, "CSV contains no columns"

        logger.info(f"CSV validation passed: {len(df.columns)} columns, encoding={encoding}")
        return True, None

    except pd.errors.EmptyDataError:
        logger.error("CSV validation failed: Empty file")
        return False, "File contains no data"
    except pd.errors.ParserError as e:
        logger.error(f"CSV validation failed: Parser error - {e}")
        return False, f"Invalid CSV format: {str(e)[:100]}"
    except Exception as e:
        logger.error(f"CSV validation failed: {e}", exc_info=True)
        return False, f"Validation error: {str(e)[:100]}"


__all__ = [
    "process_csv_async",
    "count_csv_rows",
    "validate_csv_format",
    "detect_encoding",
    "validate_csv_columns",
]
