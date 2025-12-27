"""
Unit tests for batch status endpoint.

Tests the GET /api/v1/ingest/status/{batch_id} endpoint.
"""

import pytest
from datetime import datetime, timezone
from uuid import uuid4
from unittest.mock import AsyncMock, patch

from routers.status import get_batch_status
from models.schemas import BatchStatusResponse


class TestGetBatchStatus:
    """Tests for GET /api/v1/ingest/status/{batch_id} endpoint."""

    @pytest.mark.asyncio
    @patch('routers.status.fetch_batch', new_callable=AsyncMock)
    async def test_returns_batch_status_successfully(self, mock_fetch):
        """Should return batch status with all fields populated."""
        batch_id = uuid4()

        # Mock batch data from database
        mock_fetch.return_value = {
            "batch_id": batch_id,
            "source_name": "Dane County 2025",
            "source_type": "PARCEL",
            "file_format": "GDB",
            "status": "processing",
            "total_records": 10000,
            "processed_records": 7500,
            "new_records": 7200,
            "duplicate_records": 300,
            "failed_records": 0,
            "started_at": datetime(2025, 1, 15, 14, 30, 0, tzinfo=timezone.utc),
            "completed_at": None,
            "error": None
        }

        # Call endpoint
        response = await get_batch_status(batch_id)

        # Verify response
        assert isinstance(response, BatchStatusResponse)
        assert response.batch_id == batch_id
        assert response.source_name == "Dane County 2025"
        assert response.source_type == "PARCEL"
        assert response.file_format == "GDB"
        assert response.status == "processing"
        assert response.total_records == 10000
        assert response.processed_records == 7500
        assert response.new_records == 7200
        assert response.duplicate_records == 300
        assert response.failed_records == 0
        assert response.progress_percent == 75.0
        assert response.error is None

    @pytest.mark.asyncio
    @patch('routers.status.fetch_batch', new_callable=AsyncMock)
    async def test_returns_completed_batch_status(self, mock_fetch):
        """Should return completed batch with completion timestamp."""
        batch_id = uuid4()

        mock_fetch.return_value = {
            "batch_id": batch_id,
            "source_name": "Test County",
            "source_type": "PARCEL",
            "file_format": "CSV",
            "status": "completed",
            "total_records": 1000,
            "processed_records": 1000,
            "new_records": 950,
            "duplicate_records": 50,
            "failed_records": 0,
            "started_at": datetime(2025, 1, 15, 14, 30, 0, tzinfo=timezone.utc),
            "completed_at": datetime(2025, 1, 15, 14, 35, 0, tzinfo=timezone.utc),
            "error": None
        }

        response = await get_batch_status(batch_id)

        assert response.status == "completed"
        assert response.processed_records == 1000
        assert response.progress_percent == 100.0
        assert response.completed_at is not None

    @pytest.mark.asyncio
    @patch('routers.status.fetch_batch', new_callable=AsyncMock)
    async def test_returns_failed_batch_with_error(self, mock_fetch):
        """Should return failed batch with error message."""
        batch_id = uuid4()

        mock_fetch.return_value = {
            "batch_id": batch_id,
            "source_name": "Failed Upload",
            "source_type": "PARCEL",
            "file_format": "GDB",
            "status": "failed",
            "total_records": 5000,
            "processed_records": 2500,
            "new_records": 2400,
            "duplicate_records": 100,
            "failed_records": 0,
            "started_at": datetime(2025, 1, 15, 14, 30, 0, tzinfo=timezone.utc),
            "completed_at": datetime(2025, 1, 15, 14, 32, 0, tzinfo=timezone.utc),
            "error": "Database connection lost during processing"
        }

        response = await get_batch_status(batch_id)

        assert response.status == "failed"
        assert response.error == "Database connection lost during processing"
        assert response.progress_percent == 50.0

    @pytest.mark.asyncio
    @patch('routers.status.fetch_batch', new_callable=AsyncMock)
    async def test_handles_unknown_total_records(self, mock_fetch):
        """Should handle batch with unknown total_records (null)."""
        batch_id = uuid4()

        mock_fetch.return_value = {
            "batch_id": batch_id,
            "source_name": "Streaming Upload",
            "source_type": "RETR",
            "file_format": "CSV",
            "status": "processing",
            "total_records": None,  # Unknown total
            "processed_records": 5000,
            "new_records": 4800,
            "duplicate_records": 200,
            "failed_records": 0,
            "started_at": datetime(2025, 1, 15, 14, 30, 0, tzinfo=timezone.utc),
            "completed_at": None,
            "error": None
        }

        response = await get_batch_status(batch_id)

        assert response.total_records is None
        assert response.processed_records == 5000
        assert response.progress_percent is None  # Can't calculate without total

    @pytest.mark.asyncio
    @patch('routers.status.fetch_batch', new_callable=AsyncMock)
    async def test_returns_404_for_nonexistent_batch(self, mock_fetch):
        """Should return 404 if batch not found."""
        batch_id = uuid4()
        mock_fetch.return_value = None  # Not found

        from fastapi import HTTPException
        with pytest.raises(HTTPException) as exc_info:
            await get_batch_status(batch_id)

        assert exc_info.value.status_code == 404
        assert "BatchNotFound" in str(exc_info.value.detail)

    @pytest.mark.asyncio
    @patch('routers.status.fetch_batch', new_callable=AsyncMock)
    async def test_handles_database_errors(self, mock_fetch):
        """Should return 500 on database errors."""
        batch_id = uuid4()
        mock_fetch.side_effect = Exception("Database connection failed")

        from fastapi import HTTPException
        with pytest.raises(HTTPException) as exc_info:
            await get_batch_status(batch_id)

        assert exc_info.value.status_code == 500
        assert "InternalServerError" in str(exc_info.value.detail)

    @pytest.mark.asyncio
    @patch('routers.status.fetch_batch', new_callable=AsyncMock)
    async def test_calculates_progress_correctly(self, mock_fetch):
        """Should calculate progress percentage correctly."""
        batch_id = uuid4()

        # Test various progress levels
        test_cases = [
            (0, 10000, 0.0),      # 0%
            (2500, 10000, 25.0),  # 25%
            (5000, 10000, 50.0),  # 50%
            (7500, 10000, 75.0),  # 75%
            (9999, 10000, 99.99), # 99.99%
            (10000, 10000, 100.0) # 100%
        ]

        for processed, total, expected_progress in test_cases:
            mock_fetch.return_value = {
                "batch_id": batch_id,
                "source_name": "Test",
                "source_type": "PARCEL",
                "file_format": "CSV",
                "status": "processing",
                "total_records": total,
                "processed_records": processed,
                "new_records": processed,
                "duplicate_records": 0,
                "failed_records": 0,
                "started_at": datetime.now(timezone.utc),
                "completed_at": None,
                "error": None
            }

            response = await get_batch_status(batch_id)
            assert response.progress_percent == expected_progress

    @pytest.mark.asyncio
    @patch('routers.status.fetch_batch', new_callable=AsyncMock)
    async def test_handles_zero_total_records(self, mock_fetch):
        """Should handle edge case of zero total_records."""
        batch_id = uuid4()

        mock_fetch.return_value = {
            "batch_id": batch_id,
            "source_name": "Empty File",
            "source_type": "PARCEL",
            "file_format": "CSV",
            "status": "completed",
            "total_records": 0,
            "processed_records": 0,
            "new_records": 0,
            "duplicate_records": 0,
            "failed_records": 0,
            "started_at": datetime.now(timezone.utc),
            "completed_at": datetime.now(timezone.utc),
            "error": None
        }

        response = await get_batch_status(batch_id)

        assert response.total_records == 0
        assert response.processed_records == 0
        assert response.progress_percent is None  # Avoid division by zero
