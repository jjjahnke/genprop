"""
Import batch tracking service.

Provides CRUD operations for managing import batches in the database.
Tracks upload progress, record counts, and batch status.
"""

from uuid import UUID, uuid4
from datetime import datetime, timezone
from typing import Optional, Dict, Any
import logging

from shared.database import get_db_pool

logger = logging.getLogger(__name__)


async def create_batch(
    source_name: str,
    source_type: str,
    file_format: str,
    file_size_bytes: Optional[int] = None,
    total_records: Optional[int] = None
) -> UUID:
    """
    Create a new import batch record.

    Args:
        source_name: Identifier for the data source (e.g., "Dane_County_2025")
        source_type: Type of data ('PARCEL', 'RETR', 'DFI')
        file_format: File format ('GDB', 'CSV')
        file_size_bytes: Size of the uploaded file in bytes
        total_records: Total number of records to process (if known)

    Returns:
        UUID: The batch_id of the created batch

    Example:
        ```python
        batch_id = await create_batch(
            source_name="Dane_County_2025",
            source_type="PARCEL",
            file_format="GDB",
            total_records=183425
        )
        ```
    """
    batch_id = uuid4()
    pool = await get_db_pool()

    async with pool.acquire() as conn:
        await conn.execute("""
            INSERT INTO import_batches (
                batch_id,
                source_name,
                source_type,
                file_format,
                file_size_bytes,
                total_records,
                status,
                started_at
            ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
        """,
            batch_id,
            source_name,
            source_type,
            file_format,
            file_size_bytes,
            total_records,
            'processing',
            datetime.now(timezone.utc)
        )

    logger.info(f"Created batch {batch_id} for {source_name} ({source_type}/{file_format})")
    return batch_id


async def update_batch_progress(
    batch_id: UUID,
    processed_count: int,
    new_count: Optional[int] = None,
    duplicate_count: Optional[int] = None,
    failed_count: Optional[int] = None
) -> None:
    """
    Update batch progress counters.

    Args:
        batch_id: The batch to update
        processed_count: Number of records processed in this update
        new_count: Number of new (non-duplicate) records
        duplicate_count: Number of duplicate records found
        failed_count: Number of failed records

    Example:
        ```python
        await update_batch_progress(
            batch_id=batch_id,
            processed_count=1000,
            new_count=950,
            duplicate_count=50
        )
        ```
    """
    pool = await get_db_pool()

    async with pool.acquire() as conn:
        await conn.execute("""
            UPDATE import_batches
            SET processed_records = processed_records + $2,
                new_records = new_records + COALESCE($3, 0),
                duplicate_records = duplicate_records + COALESCE($4, 0),
                failed_records = failed_records + COALESCE($5, 0)
            WHERE batch_id = $1
        """,
            batch_id,
            processed_count,
            new_count,
            duplicate_count,
            failed_count
        )


async def complete_batch(batch_id: UUID, total_processed: int) -> None:
    """
    Mark a batch as completed.

    Args:
        batch_id: The batch to complete
        total_processed: Final total number of records processed

    Example:
        ```python
        await complete_batch(batch_id, 183425)
        ```
    """
    pool = await get_db_pool()

    async with pool.acquire() as conn:
        await conn.execute("""
            UPDATE import_batches
            SET status = 'completed',
                completed_at = $2,
                processed_records = $3
            WHERE batch_id = $1
        """,
            batch_id,
            datetime.now(timezone.utc),
            total_processed
        )

    logger.info(f"Batch {batch_id} completed with {total_processed} records processed")


async def fail_batch(batch_id: UUID, error_message: str) -> None:
    """
    Mark a batch as failed with an error message.

    Args:
        batch_id: The batch that failed
        error_message: Description of the failure

    Example:
        ```python
        await fail_batch(batch_id, "GDB file corrupted: unable to read layer")
        ```
    """
    pool = await get_db_pool()

    async with pool.acquire() as conn:
        await conn.execute("""
            UPDATE import_batches
            SET status = 'failed',
                completed_at = $2,
                error = $3
            WHERE batch_id = $1
        """,
            batch_id,
            datetime.now(timezone.utc),
            error_message
        )

    logger.error(f"Batch {batch_id} failed: {error_message}")


async def fetch_batch(batch_id: UUID) -> Optional[Dict[str, Any]]:
    """
    Fetch a batch record by ID.

    Args:
        batch_id: The batch ID to fetch

    Returns:
        Dictionary containing batch data, or None if not found

    Example:
        ```python
        batch = await fetch_batch(batch_id)
        if batch:
            print(f"Status: {batch['status']}")
            print(f"Progress: {batch['processed_records']}/{batch['total_records']}")
        ```
    """
    pool = await get_db_pool()

    async with pool.acquire() as conn:
        row = await conn.fetchrow("""
            SELECT
                batch_id,
                source_name,
                source_type,
                file_format,
                file_size_bytes,
                status,
                total_records,
                processed_records,
                new_records,
                duplicate_records,
                failed_records,
                started_at,
                completed_at,
                error
            FROM import_batches
            WHERE batch_id = $1
        """, batch_id)

    if row:
        return dict(row)
    return None


async def get_batch_statistics() -> Dict[str, Any]:
    """
    Get overall batch processing statistics.

    Returns:
        Dictionary with statistics (total_batches, completed, failed, in_progress)

    Example:
        ```python
        stats = await get_batch_statistics()
        print(f"Total batches: {stats['total_batches']}")
        print(f"Completed: {stats['completed']}")
        ```
    """
    pool = await get_db_pool()

    async with pool.acquire() as conn:
        row = await conn.fetchrow("""
            SELECT
                COUNT(*) as total_batches,
                COUNT(*) FILTER (WHERE status = 'completed') as completed,
                COUNT(*) FILTER (WHERE status = 'failed') as failed,
                COUNT(*) FILTER (WHERE status = 'processing') as in_progress,
                SUM(total_records) as total_records,
                SUM(processed_records) as processed_records,
                SUM(new_records) as new_records,
                SUM(duplicate_records) as duplicate_records
            FROM import_batches
        """)

    return dict(row) if row else {}


__all__ = [
    "create_batch",
    "update_batch_progress",
    "complete_batch",
    "fail_batch",
    "fetch_batch",
    "get_batch_statistics",
]
