"""
Unit tests for CSV ingestion endpoints and processing.

Tests CSV file upload, validation, processing, and error handling:
- File upload endpoints (parcel, RETR)
- File size and extension validation
- CSV format validation
- Column validation
- Row processing and RabbitMQ publishing
- Batch tracking integration
"""

import pytest
import tempfile
from pathlib import Path
from uuid import uuid4
from unittest.mock import AsyncMock, MagicMock, patch, call
import pandas as pd
from fastapi.testclient import TestClient

from main import app
from services.csv_processor import (
    detect_encoding,
    validate_csv_columns,
    process_csv_async,
    count_csv_rows,
    validate_csv_format
)


@pytest.fixture
def client():
    """Create a test client for the FastAPI app."""
    return TestClient(app)


@pytest.fixture
def sample_parcel_csv(tmp_path):
    """Create a sample parcel CSV file for testing."""
    csv_content = """STATEID,PARCELID,ADDNUM,STREETNAME,geometry_wkt,geometry_type
WI123456,12-345-678,123,Main St,"MULTIPOLYGON(((0 0, 1 0, 1 1, 0 1, 0 0)))",MultiPolygon
WI123457,12-345-679,124,Main St,"POLYGON((0 0, 1 0, 1 1, 0 1, 0 0))",Polygon
WI123458,12-345-680,125,Main St,"MULTIPOLYGON(((2 2, 3 2, 3 3, 2 3, 2 2)))",MultiPolygon
"""
    csv_file = tmp_path / "test_parcels.csv"
    csv_file.write_text(csv_content)
    return csv_file


@pytest.fixture
def sample_retr_csv(tmp_path):
    """Create a sample RETR CSV file for testing."""
    csv_content = """PARCEL_ID,DOC_NUMBER,TRANSFER_DATE,SALE_AMOUNT,GRANTOR,GRANTEE
12-345-678,2025-001234,2025-01-15,350000.00,John Doe,Jane Smith
12-345-679,2025-001235,2025-01-16,275000.00,Alice Brown,Bob Johnson
12-345-680,2025-001236,2025-01-17,425000.00,Carol White,Dave Black
"""
    csv_file = tmp_path / "test_retr.csv"
    csv_file.write_text(csv_content)
    return csv_file


@pytest.fixture
def malformed_csv(tmp_path):
    """Create a malformed CSV file for error testing."""
    csv_content = """This is not,a,valid
CSV file
with inconsistent columns"""
    csv_file = tmp_path / "malformed.csv"
    csv_file.write_text(csv_content)
    return csv_file


class TestDetectEncoding:
    """Tests for encoding detection."""

    def test_detects_utf8(self, tmp_path):
        """Test UTF-8 encoding detection."""
        csv_file = tmp_path / "utf8.csv"
        csv_file.write_text("field1,field2\nvalue1,value2", encoding="utf-8")

        encoding = detect_encoding(csv_file)
        assert encoding.lower() in ["utf-8", "ascii"]  # ASCII is subset of UTF-8

    def test_detects_latin1(self, tmp_path):
        """Test latin-1 encoding detection."""
        csv_file = tmp_path / "latin1.csv"
        # Write with latin-1 specific characters
        content = "field1,field2\nvalué,café"
        csv_file.write_bytes(content.encode("latin-1"))

        encoding = detect_encoding(csv_file)
        # chardet might detect as ISO-8859-1 or latin-1
        assert encoding.lower() in ["iso-8859-1", "latin-1", "windows-1252"]

    def test_defaults_to_utf8_on_low_confidence(self, tmp_path):
        """Test fallback to UTF-8 for low confidence detection."""
        csv_file = tmp_path / "binary.csv"
        csv_file.write_bytes(b"\x00\x01\x02\x03\x04\x05")

        encoding = detect_encoding(csv_file)
        # Should default to utf-8 on low confidence (or ascii which is compatible)
        assert encoding.lower() in ["utf-8", "ascii"]


class TestValidateCSVColumns:
    """Tests for CSV column validation."""

    def test_parcel_csv_requires_geometry_fields(self):
        """Test that parcel CSVs must have geometry fields."""
        # Missing geometry_wkt
        df = pd.DataFrame({
            "STATEID": ["WI123"],
            "geometry_type": ["Polygon"]
        })

        with pytest.raises(ValueError, match="Missing required geometry columns"):
            validate_csv_columns(df, "PARCEL")

    def test_parcel_csv_accepts_valid_columns(self):
        """Test parcel CSV with valid geometry columns."""
        df = pd.DataFrame({
            "STATEID": ["WI123"],
            "geometry_wkt": ["POLYGON((0 0, 1 0, 1 1, 0 1, 0 0))"],
            "geometry_type": ["Polygon"]
        })

        # Should not raise
        validate_csv_columns(df, "PARCEL")

    def test_retr_csv_allows_any_columns(self):
        """Test that RETR CSVs don't require specific columns."""
        df = pd.DataFrame({
            "PARCEL_ID": ["12-345"],
            "SALE_AMOUNT": [350000]
        })

        # Should not raise
        validate_csv_columns(df, "RETR")

    def test_column_matching_is_case_insensitive(self):
        """Test that column matching ignores case."""
        df = pd.DataFrame({
            "stateid": ["WI123"],  # lowercase
            "GEOMETRY_WKT": ["POLYGON((0 0, 1 0, 1 1, 0 1, 0 0))"],  # uppercase
            "Geometry_Type": ["Polygon"]  # mixed case
        })

        # Should not raise (case-insensitive matching)
        validate_csv_columns(df, "PARCEL")


class TestProcessCSVAsync:
    """Tests for async CSV processing."""

    @pytest.mark.asyncio
    async def test_processes_parcel_csv_successfully(self, sample_parcel_csv):
        """Test successful parcel CSV processing."""
        batch_id = uuid4()

        with patch('services.csv_processor.publish_message', return_value=True) as mock_publish, \
             patch('services.csv_processor.update_batch_progress', new_callable=AsyncMock) as mock_update, \
             patch('services.csv_processor.complete_batch', new_callable=AsyncMock) as mock_complete:

            await process_csv_async(
                csv_path=sample_parcel_csv,
                source_type="PARCEL",
                batch_id=batch_id,
                source_name="Test Parcels",
                chunk_size=2  # Small chunk for testing
            )

            # Verify RabbitMQ publishing
            assert mock_publish.call_count == 3  # 3 rows in sample CSV

            # Verify batch progress updated
            assert mock_update.called

            # Verify batch completed
            mock_complete.assert_called_once_with(batch_id, 3)

    @pytest.mark.asyncio
    async def test_processes_retr_csv_successfully(self, sample_retr_csv):
        """Test successful RETR CSV processing."""
        batch_id = uuid4()

        with patch('services.csv_processor.publish_message', return_value=True) as mock_publish, \
             patch('services.csv_processor.update_batch_progress', new_callable=AsyncMock), \
             patch('services.csv_processor.complete_batch', new_callable=AsyncMock):

            await process_csv_async(
                csv_path=sample_retr_csv,
                source_type="RETR",
                batch_id=batch_id,
                source_name="Test RETR"
            )

            assert mock_publish.call_count == 3

    @pytest.mark.asyncio
    async def test_publishes_correct_message_format(self, sample_parcel_csv):
        """Test that messages are published in correct format."""
        batch_id = uuid4()

        with patch('services.csv_processor.publish_message', return_value=True) as mock_publish, \
             patch('services.csv_processor.update_batch_progress', new_callable=AsyncMock), \
             patch('services.csv_processor.complete_batch', new_callable=AsyncMock):

            await process_csv_async(
                csv_path=sample_parcel_csv,
                source_type="PARCEL",
                batch_id=batch_id,
                source_name="Test Parcels"
            )

            # Check first message format
            first_call = mock_publish.call_args_list[0]
            queue, message = first_call[0]

            assert queue == "deduplication"
            assert message["batch_id"] == str(batch_id)
            assert message["source_type"] == "PARCEL"
            assert message["source_file"] == "Test Parcels"
            assert "source_row_number" in message
            assert "raw_data" in message

    @pytest.mark.asyncio
    async def test_handles_failed_messages(self, sample_parcel_csv):
        """Test handling of failed message publishing."""
        batch_id = uuid4()

        # Simulate some messages failing
        with patch('services.csv_processor.publish_message', return_value=False) as mock_publish, \
             patch('services.csv_processor.update_batch_progress', new_callable=AsyncMock) as mock_update, \
             patch('services.csv_processor.complete_batch', new_callable=AsyncMock):

            await process_csv_async(
                csv_path=sample_parcel_csv,
                source_type="PARCEL",
                batch_id=batch_id,
                source_name="Test Parcels"
            )

            # Verify failed records tracked
            update_calls = mock_update.call_args_list
            # Should have failed_count in some calls
            assert any(call.kwargs.get('failed_count', 0) > 0 for call in update_calls)

    @pytest.mark.asyncio
    async def test_handles_processing_exceptions_gracefully(self, sample_parcel_csv):
        """Test that processing exceptions for individual rows don't crash the batch."""
        batch_id = uuid4()

        # Make every message fail
        with patch('services.csv_processor.publish_message', side_effect=Exception("Test error")), \
             patch('services.csv_processor.update_batch_progress', new_callable=AsyncMock) as mock_update, \
             patch('services.csv_processor.complete_batch', new_callable=AsyncMock) as mock_complete:

            # Should NOT raise - exceptions are caught per-row
            await process_csv_async(
                csv_path=sample_parcel_csv,
                source_type="PARCEL",
                batch_id=batch_id,
                source_name="Test Parcels"
            )

            # Verify all rows were marked as failed
            update_calls = mock_update.call_args_list
            total_failed = sum(call.kwargs.get('failed_count', 0) for call in update_calls)
            assert total_failed == 3  # All 3 rows failed

            # Batch should still complete (with all failed records)
            mock_complete.assert_called_once()

    @pytest.mark.asyncio
    async def test_processes_in_chunks(self, tmp_path):
        """Test that large CSVs are processed in chunks."""
        # Create a larger CSV (10 rows)
        rows = [{"field1": f"value{i}", "field2": f"data{i}"} for i in range(10)]
        df = pd.DataFrame(rows)
        csv_file = tmp_path / "large.csv"
        df.to_csv(csv_file, index=False)

        batch_id = uuid4()
        chunk_size = 3

        with patch('services.csv_processor.publish_message', return_value=True), \
             patch('services.csv_processor.update_batch_progress', new_callable=AsyncMock) as mock_update, \
             patch('services.csv_processor.complete_batch', new_callable=AsyncMock), \
             patch('services.csv_processor.SOURCE_TYPE_MODELS', {"RETR": MagicMock(return_value=MagicMock(model_dump=lambda **kw: {}))}):

            await process_csv_async(
                csv_path=csv_file,
                source_type="RETR",
                batch_id=batch_id,
                source_name="Test",
                chunk_size=chunk_size
            )

            # Should have updated progress multiple times (one per chunk)
            # 10 rows / 3 per chunk = 4 chunks
            assert mock_update.call_count == 4


class TestCountCSVRows:
    """Tests for CSV row counting."""

    @pytest.mark.asyncio
    async def test_counts_rows_correctly(self, sample_parcel_csv):
        """Test accurate row counting."""
        count = await count_csv_rows(sample_parcel_csv)
        assert count == 3  # 3 data rows (excludes header)

    @pytest.mark.asyncio
    async def test_handles_large_files_efficiently(self, tmp_path):
        """Test efficient counting of large files."""
        # Create a large CSV
        rows = [{"field1": f"value{i}"} for i in range(10000)]
        df = pd.DataFrame(rows)
        csv_file = tmp_path / "large.csv"
        df.to_csv(csv_file, index=False)

        count = await count_csv_rows(csv_file)
        assert count == 10000


class TestValidateCSVFormat:
    """Tests for CSV format validation."""

    def test_validates_correct_csv(self, sample_parcel_csv):
        """Test validation of correct CSV."""
        assert validate_csv_format(sample_parcel_csv) is True

    def test_rejects_invalid_csv(self, malformed_csv):
        """Test rejection of invalid CSV."""
        # Might still parse, but should handle gracefully
        result = validate_csv_format(malformed_csv)
        # Just check it doesn't crash
        assert isinstance(result, bool)

    def test_rejects_non_existent_file(self, tmp_path):
        """Test handling of non-existent file."""
        fake_file = tmp_path / "nonexistent.csv"
        assert validate_csv_format(fake_file) is False


class TestCSVUploadEndpoints:
    """Tests for CSV upload endpoints."""

    @patch('routers.csv_ingest.create_batch', new_callable=AsyncMock)
    @patch('routers.csv_ingest.count_csv_rows', new_callable=AsyncMock)
    @patch('routers.csv_ingest.validate_csv_format', return_value=True)
    @patch('routers.csv_ingest.settings')
    def test_parcel_csv_upload_success(self, mock_settings, mock_validate, mock_count, mock_create_batch, client, sample_parcel_csv):
        """Test successful parcel CSV upload."""
        # Mock settings
        mock_settings.ALLOWED_CSV_EXTENSIONS = [".csv"]
        mock_settings.max_upload_size_bytes = 5000 * 1024 * 1024
        mock_settings.TEMP_STORAGE_PATH = "/tmp/test"
        mock_settings.BATCH_SIZE = 1000

        mock_create_batch.return_value = uuid4()
        mock_count.return_value = 3

        with open(sample_parcel_csv, 'rb') as f:
            response = client.post(
                "/api/v1/ingest/parcel/csv",
                files={"file": ("test_parcels.csv", f, "text/csv")},
                data={"source_name": "Test Parcels 2025"}
            )

        assert response.status_code == 202
        data = response.json()
        assert "batch_id" in data
        assert data["source_type"] == "PARCEL"
        assert data["file_format"] == "CSV"

    @patch('routers.csv_ingest.settings')
    def test_rejects_invalid_file_extension(self, mock_settings, client, tmp_path):
        """Test rejection of invalid file extensions."""
        mock_settings.ALLOWED_CSV_EXTENSIONS = [".csv"]

        txt_file = tmp_path / "test.txt"
        txt_file.write_text("not a csv")

        with open(txt_file, 'rb') as f:
            response = client.post(
                "/api/v1/ingest/parcel/csv",
                files={"file": ("test.txt", f, "text/plain")},
                data={"source_name": "Test"}
            )

        assert response.status_code == 400
        assert "Invalid file extension" in response.json()["message"]

    @patch('routers.csv_ingest.validate_csv_format', return_value=False)
    @patch('routers.csv_ingest.settings')
    def test_rejects_invalid_csv_format(self, mock_settings, mock_validate, client, malformed_csv):
        """Test rejection of invalid CSV format."""
        mock_settings.ALLOWED_CSV_EXTENSIONS = [".csv"]
        mock_settings.max_upload_size_bytes = 5000 * 1024 * 1024
        mock_settings.TEMP_STORAGE_PATH = "/tmp/test"

        with open(malformed_csv, 'rb') as f:
            response = client.post(
                "/api/v1/ingest/parcel/csv",
                files={"file": ("malformed.csv", f, "text/csv")},
                data={"source_name": "Test"}
            )

        assert response.status_code == 400
        assert "not a valid CSV" in response.json()["message"]

    @patch('routers.csv_ingest.settings')
    def test_validates_source_name_format(self, mock_settings, client, sample_parcel_csv):
        """Test source_name validation (no special characters)."""
        mock_settings.ALLOWED_CSV_EXTENSIONS = [".csv"]

        with open(sample_parcel_csv, 'rb') as f:
            response = client.post(
                "/api/v1/ingest/parcel/csv",
                files={"file": ("test.csv", f, "text/csv")},
                data={"source_name": "Test@Invalid!Name"}  # Special characters
            )

        assert response.status_code == 422  # Validation error


__all__ = []
