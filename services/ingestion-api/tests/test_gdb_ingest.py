"""
Unit tests for GDB ingestion router.

Tests GDB upload endpoint, validation, and error handling.
"""

import pytest
from pathlib import Path
from uuid import UUID, uuid4
from unittest.mock import AsyncMock, MagicMock, patch
from fastapi import UploadFile
from io import BytesIO

from routers.gdb_ingest import router, upload_parcel_gdb
from models.schemas import IngestResponse


@pytest.fixture
def mock_gdb_zip_file():
    """Create a mock GDB zip file for upload."""
    content = b"PK\x03\x04" + b"\x00" * 100  # Minimal zip file signature + data
    file = UploadFile(
        filename="test_parcels.gdb.zip",
        file=BytesIO(content)
    )
    file.size = len(content)
    return file


@pytest.fixture
def mock_gdb_info():
    """Mock GDB inspection info."""
    return {
        "layers": ["V11_Parcels", "Counties"],
        "default_layer": "V11_Parcels",
        "layer_info": {
            "V11_Parcels": {
                "crs": "epsg:3071",
                "bounds": (-90.0, 42.0, -87.0, 47.0),
                "feature_count": 12345,
                "schema": {"geometry": "Polygon"}
            }
        }
    }


class TestGDBUploadEndpoint:
    """Tests for POST /api/v1/ingest/parcel/gdb endpoint."""

    @pytest.mark.asyncio
    @patch('routers.gdb_ingest.settings')
    @patch('routers.gdb_ingest.save_upload_file', new_callable=AsyncMock)
    @patch('routers.gdb_ingest.extract_gdb')
    @patch('routers.gdb_ingest.inspect_gdb')
    @patch('routers.gdb_ingest.validate_gdb_format', return_value=True)
    @patch('routers.gdb_ingest.create_batch', new_callable=AsyncMock)
    async def test_gdb_upload_success(
        self,
        mock_create_batch,
        mock_validate,
        mock_inspect,
        mock_extract,
        mock_save,
        mock_settings,
        mock_gdb_zip_file,
        mock_gdb_info
    ):
        """Should successfully accept GDB upload and return batch_id."""
        # Setup mocks
        mock_settings.ALLOWED_GDB_EXTENSIONS = [".gdb.zip", ".zip"]
        mock_settings.max_upload_size_bytes = 5000 * 1024 * 1024
        mock_settings.TEMP_STORAGE_PATH = "/tmp/gdb-processing"
        mock_settings.DEFAULT_LAYER_NAME = "V11_Parcels"
        mock_settings.BATCH_SIZE = 1000

        mock_save.return_value = 1024  # File size in bytes
        mock_extract.return_value = Path("/tmp/extract/test.gdb")
        mock_inspect.return_value = mock_gdb_info

        batch_id = uuid4()
        mock_create_batch.return_value = batch_id

        # Mock BackgroundTasks
        background_tasks = MagicMock()

        # Call endpoint
        response = await upload_parcel_gdb(
            background_tasks=background_tasks,
            file=mock_gdb_zip_file,
            source_name="Dane County 2025",
            layer_name=None  # Use default
        )

        # Verify response
        assert isinstance(response, IngestResponse)
        assert response.batch_id == batch_id
        assert response.status == "processing"
        assert response.source_name == "Dane County 2025"
        assert response.source_type == "PARCEL"
        assert response.file_format == "GDB"
        assert response.total_records == 12345

        # Verify batch was created
        mock_create_batch.assert_called_once()
        create_args = mock_create_batch.call_args[1]
        assert create_args["source_name"] == "Dane County 2025"
        assert create_args["source_type"] == "PARCEL"
        assert create_args["file_format"] == "GDB"

        # Verify background task was added
        assert background_tasks.add_task.called

    @pytest.mark.asyncio
    @patch('routers.gdb_ingest.settings')
    @patch('routers.gdb_ingest.save_upload_file', new_callable=AsyncMock)
    @patch('routers.gdb_ingest.extract_gdb')
    @patch('routers.gdb_ingest.inspect_gdb')
    @patch('routers.gdb_ingest.validate_gdb_format', return_value=True)
    @patch('routers.gdb_ingest.create_batch', new_callable=AsyncMock)
    async def test_gdb_upload_with_custom_layer(
        self,
        mock_create_batch,
        mock_validate,
        mock_inspect,
        mock_extract,
        mock_save,
        mock_settings,
        mock_gdb_zip_file,
        mock_gdb_info
    ):
        """Should accept custom layer_name parameter."""
        mock_settings.ALLOWED_GDB_EXTENSIONS = [".gdb.zip", ".zip"]
        mock_settings.max_upload_size_bytes = 5000 * 1024 * 1024
        mock_settings.TEMP_STORAGE_PATH = "/tmp/gdb-processing"
        mock_settings.BATCH_SIZE = 1000

        mock_save.return_value = 1024
        mock_extract.return_value = Path("/tmp/extract/test.gdb")

        # Add custom layer to mock info
        custom_layer_info = mock_gdb_info.copy()
        custom_layer_info["layers"] = ["V11_Parcels", "CustomLayer"]
        custom_layer_info["layer_info"]["CustomLayer"] = {
            "crs": "epsg:3071",
            "feature_count": 999
        }
        mock_inspect.return_value = custom_layer_info

        mock_create_batch.return_value = uuid4()

        background_tasks = MagicMock()

        # Call with custom layer
        response = await upload_parcel_gdb(
            background_tasks=background_tasks,
            file=mock_gdb_zip_file,
            source_name="Test County",
            layer_name="CustomLayer"
        )

        # Verify custom layer was used
        assert response.total_records == 999

    @pytest.mark.asyncio
    @patch('routers.gdb_ingest.settings')
    async def test_rejects_invalid_file_extension(self, mock_settings, mock_gdb_zip_file):
        """Should return 400 for invalid file extension."""
        mock_settings.ALLOWED_GDB_EXTENSIONS = [".gdb.zip"]
        mock_settings.max_upload_size_bytes = 5000 * 1024 * 1024
        mock_settings.TEMP_STORAGE_PATH = "/tmp/gdb-processing"
        mock_settings.DEFAULT_LAYER_NAME = "V11_Parcels"

        # Create file with invalid extension
        invalid_file = UploadFile(
            filename="test.txt",
            file=BytesIO(b"not a gdb")
        )

        background_tasks = MagicMock()

        from fastapi import HTTPException
        with pytest.raises(HTTPException) as exc_info:
            await upload_parcel_gdb(
                background_tasks=background_tasks,
                file=invalid_file,
                source_name="Test County 2025",
                layer_name=None
            )

        assert exc_info.value.status_code == 400
        assert "InvalidFileType" in str(exc_info.value.detail)

    @pytest.mark.asyncio
    @patch('routers.gdb_ingest.settings')
    @patch('routers.gdb_ingest.save_upload_file', new_callable=AsyncMock)
    async def test_rejects_oversized_file(self, mock_save, mock_settings, mock_gdb_zip_file):
        """Should return 413 for file exceeding size limit."""
        mock_settings.ALLOWED_GDB_EXTENSIONS = [".gdb.zip", ".zip"]
        mock_settings.max_upload_size_bytes = 100  # Very small limit
        mock_settings.TEMP_STORAGE_PATH = "/tmp/gdb-processing"
        mock_settings.DEFAULT_LAYER_NAME = "V11_Parcels"

        # Mock save to return size exceeding limit
        mock_save.return_value = 1024 * 1024  # 1MB

        background_tasks = MagicMock()

        from fastapi import HTTPException
        with pytest.raises(HTTPException) as exc_info:
            await upload_parcel_gdb(
                background_tasks=background_tasks,
                file=mock_gdb_zip_file,
                source_name="Test County 2025",
                layer_name=None
            )

        assert exc_info.value.status_code == 413
        assert "FileTooLarge" in str(exc_info.value.detail)

    @pytest.mark.asyncio
    @patch('routers.gdb_ingest.settings')
    @patch('routers.gdb_ingest.save_upload_file', new_callable=AsyncMock)
    @patch('routers.gdb_ingest.extract_gdb')
    @patch('routers.gdb_ingest.cleanup_gdb')
    async def test_rejects_invalid_gdb_extraction(
        self,
        mock_cleanup,
        mock_extract,
        mock_save,
        mock_settings,
        mock_gdb_zip_file
    ):
        """Should return 400 if GDB extraction fails."""
        mock_settings.ALLOWED_GDB_EXTENSIONS = [".gdb.zip", ".zip"]
        mock_settings.max_upload_size_bytes = 5000 * 1024 * 1024
        mock_settings.TEMP_STORAGE_PATH = "/tmp/gdb-processing"
        mock_settings.DEFAULT_LAYER_NAME = "V11_Parcels"

        mock_save.return_value = 1024
        mock_extract.side_effect = Exception("Invalid zip format")

        background_tasks = MagicMock()

        from fastapi import HTTPException
        with pytest.raises(HTTPException) as exc_info:
            await upload_parcel_gdb(
                background_tasks=background_tasks,
                file=mock_gdb_zip_file,
                source_name="Test County 2025",
                layer_name=None
            )

        assert exc_info.value.status_code == 400
        assert "InvalidGDBFormat" in str(exc_info.value.detail)

    @pytest.mark.asyncio
    @patch('routers.gdb_ingest.settings')
    @patch('routers.gdb_ingest.save_upload_file', new_callable=AsyncMock)
    @patch('routers.gdb_ingest.extract_gdb')
    @patch('routers.gdb_ingest.validate_gdb_format', return_value=False)
    @patch('routers.gdb_ingest.cleanup_gdb')
    async def test_rejects_invalid_gdb_format(
        self,
        mock_cleanup,
        mock_validate,
        mock_extract,
        mock_save,
        mock_settings,
        mock_gdb_zip_file
    ):
        """Should return 400 if GDB validation fails."""
        mock_settings.ALLOWED_GDB_EXTENSIONS = [".gdb.zip", ".zip"]
        mock_settings.max_upload_size_bytes = 5000 * 1024 * 1024
        mock_settings.TEMP_STORAGE_PATH = "/tmp/gdb-processing"
        mock_settings.DEFAULT_LAYER_NAME = "V11_Parcels"

        mock_save.return_value = 1024
        mock_extract.return_value = Path("/tmp/extract/invalid.gdb")

        background_tasks = MagicMock()

        from fastapi import HTTPException
        with pytest.raises(HTTPException) as exc_info:
            await upload_parcel_gdb(
                background_tasks=background_tasks,
                file=mock_gdb_zip_file,
                source_name="Test County 2025",
                layer_name=None
            )

        assert exc_info.value.status_code == 400
        assert "InvalidGDBFormat" in str(exc_info.value.detail)

    @pytest.mark.asyncio
    @patch('routers.gdb_ingest.settings')
    @patch('routers.gdb_ingest.save_upload_file', new_callable=AsyncMock)
    @patch('routers.gdb_ingest.extract_gdb')
    @patch('routers.gdb_ingest.inspect_gdb')
    @patch('routers.gdb_ingest.validate_gdb_format', return_value=True)
    @patch('routers.gdb_ingest.cleanup_gdb')
    async def test_rejects_missing_layer(
        self,
        mock_cleanup,
        mock_validate,
        mock_inspect,
        mock_extract,
        mock_save,
        mock_settings,
        mock_gdb_zip_file,
        mock_gdb_info
    ):
        """Should return 400 if requested layer not found in GDB."""
        mock_settings.ALLOWED_GDB_EXTENSIONS = [".gdb.zip", ".zip"]
        mock_settings.max_upload_size_bytes = 5000 * 1024 * 1024
        mock_settings.TEMP_STORAGE_PATH = "/tmp/gdb-processing"

        mock_save.return_value = 1024
        mock_extract.return_value = Path("/tmp/extract/test.gdb")
        mock_inspect.return_value = mock_gdb_info  # Contains V11_Parcels, Counties

        background_tasks = MagicMock()

        from fastapi import HTTPException
        with pytest.raises(HTTPException) as exc_info:
            await upload_parcel_gdb(
                background_tasks=background_tasks,
                file=mock_gdb_zip_file,
                source_name="Test",
                layer_name="NonExistentLayer"  # Not in GDB
            )

        assert exc_info.value.status_code == 400
        assert "LayerNotFound" in str(exc_info.value.detail)

    @pytest.mark.asyncio
    @patch('routers.gdb_ingest.settings')
    async def test_validates_source_name(self, mock_settings, mock_gdb_zip_file):
        """Should validate source_name is not empty."""
        mock_settings.ALLOWED_GDB_EXTENSIONS = [".gdb.zip", ".zip"]
        mock_settings.max_upload_size_bytes = 5000 * 1024 * 1024

        background_tasks = MagicMock()

        from fastapi import HTTPException
        with pytest.raises(HTTPException) as exc_info:
            await upload_parcel_gdb(
                background_tasks=background_tasks,
                file=mock_gdb_zip_file,
                source_name="",  # Empty source name
                layer_name=None
            )

        assert exc_info.value.status_code == 422
        assert "ValidationError" in str(exc_info.value.detail)
