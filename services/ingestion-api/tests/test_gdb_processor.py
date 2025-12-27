"""
Unit tests for GDB processor service.

Tests GDB extraction, inspection, transformation, and async processing.
"""

import pytest
import zipfile
from pathlib import Path
from uuid import uuid4
from unittest.mock import Mock, AsyncMock, patch, MagicMock
from datetime import datetime, timezone
import geopandas as gpd
import pandas as pd
from shapely.geometry import Polygon

from services.gdb_processor import (
    extract_gdb,
    inspect_gdb,
    count_features,
    transform_to_wisconsin_crs,
    process_gdb_async,
    validate_gdb_format,
    cleanup_gdb,
    WISCONSIN_CRS
)


class TestExtractGDB:
    """Tests for extract_gdb function."""

    def test_extracts_gdb_from_zip(self, tmp_path):
        """Should extract .gdb directory from zip file."""
        # Create a mock GDB structure
        gdb_dir = tmp_path / "test.gdb"
        gdb_dir.mkdir()
        (gdb_dir / "a00000001.gdbtable").touch()
        (gdb_dir / "a00000001.gdbtablx").touch()

        # Create zip file containing the GDB
        zip_path = tmp_path / "test.gdb.zip"
        with zipfile.ZipFile(zip_path, 'w') as zf:
            for file in gdb_dir.glob("*"):
                zf.write(file, f"test.gdb/{file.name}")

        # Extract
        extract_dir = tmp_path / "extract"
        result = extract_gdb(zip_path, extract_dir)

        # Verify
        assert result.exists()
        assert result.is_dir()
        assert result.name == "test.gdb"
        assert (result / "a00000001.gdbtable").exists()

    def test_raises_error_if_no_gdb_in_zip(self, tmp_path):
        """Should raise ValueError if zip doesn't contain .gdb directory."""
        # Create zip with random files (no .gdb)
        zip_path = tmp_path / "invalid.zip"
        with zipfile.ZipFile(zip_path, 'w') as zf:
            zf.writestr("random.txt", "not a gdb")

        extract_dir = tmp_path / "extract"

        with pytest.raises(ValueError, match="No .gdb directory found"):
            extract_gdb(zip_path, extract_dir)

    def test_handles_nested_gdb_directory(self, tmp_path):
        """Should find .gdb directory even if nested in subdirectories."""
        # Create nested GDB structure
        nested_dir = tmp_path / "data" / "parcels"
        nested_dir.mkdir(parents=True)
        gdb_dir = nested_dir / "parcels.gdb"
        gdb_dir.mkdir()
        (gdb_dir / "a00000001.gdbtable").touch()

        # Create zip with nested structure
        zip_path = tmp_path / "nested.gdb.zip"
        with zipfile.ZipFile(zip_path, 'w') as zf:
            for file in gdb_dir.glob("*"):
                zf.write(file, f"data/parcels/parcels.gdb/{file.name}")

        # Extract
        extract_dir = tmp_path / "extract"
        result = extract_gdb(zip_path, extract_dir)

        # Verify
        assert result.exists()
        assert result.name == "parcels.gdb"


class TestInspectGDB:
    """Tests for inspect_gdb function."""

    @patch('services.gdb_processor.fiona')
    def test_inspects_gdb_layers(self, mock_fiona):
        """Should return layer metadata from GDB."""
        # Mock fiona.listlayers
        mock_fiona.listlayers.return_value = ["V11_Parcels", "Counties"]

        # Mock fiona.open context manager
        mock_src = MagicMock()
        mock_src.crs = {"init": "epsg:3071"}
        mock_src.crs_wkt = "PROJCS[...]"
        mock_src.bounds = (-90.0, 42.0, -87.0, 47.0)
        mock_src.__len__.return_value = 12345
        mock_src.schema = {"geometry": "Polygon", "properties": {"STATEID": "str"}}

        mock_fiona.open.return_value.__enter__.return_value = mock_src

        # Inspect
        gdb_path = Path("/fake/path/to/test.gdb")
        result = inspect_gdb(gdb_path)

        # Verify
        assert result["layers"] == ["V11_Parcels", "Counties"]
        assert result["default_layer"] == "V11_Parcels"
        assert "V11_Parcels" in result["layer_info"]

        layer_info = result["layer_info"]["V11_Parcels"]
        assert layer_info["crs"] == "epsg:3071"
        assert layer_info["feature_count"] == 12345
        assert layer_info["bounds"] == (-90.0, 42.0, -87.0, 47.0)

    @patch('services.gdb_processor.fiona')
    def test_handles_gdb_without_crs(self, mock_fiona):
        """Should handle GDB layers without CRS."""
        mock_fiona.listlayers.return_value = ["layer1"]

        mock_src = MagicMock()
        mock_src.crs = None
        mock_src.bounds = None
        mock_src.__len__.return_value = 100
        mock_src.schema = {}

        mock_fiona.open.return_value.__enter__.return_value = mock_src

        result = inspect_gdb(Path("/fake/test.gdb"))

        layer_info = result["layer_info"]["layer1"]
        assert layer_info["crs"] == "Unknown"

    @patch('services.gdb_processor.fiona')
    def test_raises_error_if_no_layers(self, mock_fiona):
        """Should raise ValueError if GDB has no layers."""
        mock_fiona.listlayers.return_value = []

        with pytest.raises(ValueError, match="No layers found"):
            inspect_gdb(Path("/fake/empty.gdb"))


class TestCountFeatures:
    """Tests for count_features function."""

    @patch('services.gdb_processor.fiona')
    def test_counts_features_in_layer(self, mock_fiona):
        """Should return feature count for specified layer."""
        mock_src = MagicMock()
        mock_src.__len__.return_value = 54321

        mock_fiona.open.return_value.__enter__.return_value = mock_src

        count = count_features(Path("/fake/test.gdb"), "V11_Parcels")

        assert count == 54321
        mock_fiona.open.assert_called_once()


class TestTransformToWisconsinCRS:
    """Tests for transform_to_wisconsin_crs function."""

    def test_transforms_from_wgs84_to_wisconsin(self):
        """Should transform GeoDataFrame from WGS84 to EPSG:3071."""
        # Create sample GeoDataFrame in WGS84 (EPSG:4326)
        gdf = gpd.GeoDataFrame(
            {"id": [1, 2]},
            geometry=[
                Polygon([(-89.4, 43.0), (-89.4, 43.1), (-89.3, 43.1), (-89.3, 43.0)]),
                Polygon([(-89.5, 43.2), (-89.5, 43.3), (-89.4, 43.3), (-89.4, 43.2)])
            ],
            crs="EPSG:4326"
        )

        result = transform_to_wisconsin_crs(gdf)

        # Verify CRS is now Wisconsin
        assert result.crs.to_string() == WISCONSIN_CRS
        # Verify geometries are transformed (coordinates should be very different)
        assert result.geometry[0].bounds != gdf.geometry[0].bounds

    def test_skips_transformation_if_already_wisconsin_crs(self):
        """Should skip transformation if already in EPSG:3071."""
        gdf = gpd.GeoDataFrame(
            {"id": [1]},
            geometry=[Polygon([(500000, 200000), (500000, 200100), (500100, 200100)])],
            crs=WISCONSIN_CRS
        )

        original_geometry = gdf.geometry[0]
        result = transform_to_wisconsin_crs(gdf)

        # Verify no transformation occurred
        assert result.crs.to_string() == WISCONSIN_CRS
        assert result.geometry[0].equals(original_geometry)

    def test_sets_crs_if_none(self):
        """Should set Wisconsin CRS if GeoDataFrame has no CRS."""
        gdf = gpd.GeoDataFrame(
            {"id": [1]},
            geometry=[Polygon([(500000, 200000), (500000, 200100), (500100, 200100)])]
        )
        # No CRS set
        assert gdf.crs is None

        result = transform_to_wisconsin_crs(gdf)

        # Verify CRS is now set to Wisconsin
        assert result.crs.to_string() == WISCONSIN_CRS


class TestValidateGDBFormat:
    """Tests for validate_gdb_format function."""

    @patch('services.gdb_processor.fiona')
    def test_returns_true_for_valid_gdb(self, mock_fiona, tmp_path):
        """Should return True if GDB is valid."""
        gdb_dir = tmp_path / "valid.gdb"
        gdb_dir.mkdir()

        mock_fiona.listlayers.return_value = ["layer1"]

        result = validate_gdb_format(gdb_dir)

        assert result is True

    @patch('services.gdb_processor.fiona')
    def test_returns_false_if_no_layers(self, mock_fiona, tmp_path):
        """Should return False if GDB has no layers."""
        gdb_dir = tmp_path / "empty.gdb"
        gdb_dir.mkdir()

        mock_fiona.listlayers.return_value = []

        result = validate_gdb_format(gdb_dir)

        assert result is False

    def test_returns_false_if_path_does_not_exist(self):
        """Should return False if path doesn't exist."""
        result = validate_gdb_format(Path("/nonexistent/path.gdb"))
        assert result is False

    def test_returns_false_if_path_is_not_directory(self, tmp_path):
        """Should return False if path is a file, not directory."""
        file_path = tmp_path / "file.gdb"
        file_path.touch()

        result = validate_gdb_format(file_path)

        assert result is False


class TestCleanupGDB:
    """Tests for cleanup_gdb function."""

    def test_removes_gdb_directory(self, tmp_path):
        """Should remove .gdb directory and parent extraction directory."""
        extract_dir = tmp_path / "extract"
        extract_dir.mkdir()
        gdb_dir = extract_dir / "test.gdb"
        gdb_dir.mkdir()
        (gdb_dir / "file.txt").touch()

        cleanup_gdb(gdb_dir)

        # Verify parent directory is removed
        assert not extract_dir.exists()

    def test_removes_extraction_directory(self, tmp_path):
        """Should remove extraction directory if given."""
        extract_dir = tmp_path / "extract"
        extract_dir.mkdir()
        (extract_dir / "file.txt").touch()

        cleanup_gdb(extract_dir)

        assert not extract_dir.exists()

    def test_handles_nonexistent_path(self):
        """Should not raise error if path doesn't exist."""
        # Should not raise exception
        cleanup_gdb(Path("/nonexistent/path"))


@pytest.mark.asyncio
class TestProcessGDBAsync:
    """Tests for process_gdb_async function."""

    async def test_processes_gdb_successfully(self, tmp_path):
        """Should process GDB layer and publish to RabbitMQ."""
        # Create mock GeoDataFrame
        gdf = gpd.GeoDataFrame(
            {
                "STATEID": ["WI001", "WI002", "WI003"],
                "PARCELID": ["123", "124", "125"],
                "ADDNUM": ["100", "200", "300"],
                "STREETNAME": ["MAIN ST", "OAK AVE", "ELM RD"]
            },
            geometry=[
                Polygon([(500000, 200000), (500000, 200100), (500100, 200100)]),
                Polygon([(500200, 200000), (500200, 200100), (500300, 200100)]),
                Polygon([(500400, 200000), (500400, 200100), (500500, 200100)])
            ],
            crs=WISCONSIN_CRS
        )

        batch_id = uuid4()

        with patch('services.gdb_processor.gpd.read_file', return_value=gdf), \
             patch('services.gdb_processor.publish_message', return_value=True) as mock_publish, \
             patch('services.gdb_processor.update_batch_progress', new_callable=AsyncMock) as mock_update, \
             patch('services.gdb_processor.complete_batch', new_callable=AsyncMock) as mock_complete:

            await process_gdb_async(
                gdb_path=tmp_path / "test.gdb",
                layer_name="V11_Parcels",
                batch_id=batch_id,
                source_name="Test County",
                chunk_size=2  # Small chunk size to test chunking
            )

            # Verify all 3 features were published
            assert mock_publish.call_count == 3

            # Verify batch progress was updated
            assert mock_update.call_count == 2  # 2 chunks (2 + 1)

            # Verify batch was completed
            mock_complete.assert_called_once_with(batch_id, 3)

    async def test_handles_geometry_transformation(self, tmp_path):
        """Should transform geometry to Wisconsin CRS if needed."""
        # Create GeoDataFrame in WGS84
        gdf = gpd.GeoDataFrame(
            {"STATEID": ["WI001"], "PARCELID": ["123"]},
            geometry=[Polygon([(-89.4, 43.0), (-89.4, 43.1), (-89.3, 43.1)])],
            crs="EPSG:4326"
        )

        batch_id = uuid4()

        with patch('services.gdb_processor.gpd.read_file', return_value=gdf), \
             patch('services.gdb_processor.publish_message', return_value=True) as mock_publish, \
             patch('services.gdb_processor.update_batch_progress', new_callable=AsyncMock), \
             patch('services.gdb_processor.complete_batch', new_callable=AsyncMock):

            await process_gdb_async(
                gdb_path=tmp_path / "test.gdb",
                layer_name="V11_Parcels",
                batch_id=batch_id,
                source_name="Test",
                chunk_size=100
            )

            # Verify message was published with WKT geometry
            assert mock_publish.call_count == 1
            call_args = mock_publish.call_args[0]
            message = call_args[1]

            # Verify geometry was included
            assert "geometry_wkt" in message["raw_data"]
            assert "geometry_type" in message["raw_data"]

    async def test_handles_empty_geometry(self, tmp_path):
        """Should skip features with empty geometry."""
        gdf = gpd.GeoDataFrame(
            {"STATEID": ["WI001", "WI002"], "PARCELID": ["123", "124"]},
            geometry=[
                Polygon([(500000, 200000), (500000, 200100), (500100, 200100)]),
                None  # Empty geometry
            ],
            crs=WISCONSIN_CRS
        )

        batch_id = uuid4()

        with patch('services.gdb_processor.gpd.read_file', return_value=gdf), \
             patch('services.gdb_processor.publish_message', return_value=True) as mock_publish, \
             patch('services.gdb_processor.update_batch_progress', new_callable=AsyncMock), \
             patch('services.gdb_processor.complete_batch', new_callable=AsyncMock):

            await process_gdb_async(
                gdb_path=tmp_path / "test.gdb",
                layer_name="V11_Parcels",
                batch_id=batch_id,
                source_name="Test",
                chunk_size=100
            )

            # Only 1 feature should be published (WI001)
            assert mock_publish.call_count == 1

    async def test_handles_processing_errors(self, tmp_path):
        """Should fail batch if processing encounters errors."""
        batch_id = uuid4()

        with patch('services.gdb_processor.gpd.read_file', side_effect=Exception("Read error")), \
             patch('services.gdb_processor.fail_batch', new_callable=AsyncMock) as mock_fail:

            with pytest.raises(Exception, match="Read error"):
                await process_gdb_async(
                    gdb_path=tmp_path / "test.gdb",
                    layer_name="V11_Parcels",
                    batch_id=batch_id,
                    source_name="Test",
                    chunk_size=100
                )

            # Verify batch was marked as failed
            mock_fail.assert_called_once()
            fail_args = mock_fail.call_args[0]
            assert fail_args[0] == batch_id
            assert "Read error" in fail_args[1]

    async def test_processes_in_chunks(self, tmp_path):
        """Should process features in chunks for memory efficiency."""
        # Create GeoDataFrame with 5 features
        gdf = gpd.GeoDataFrame(
            {"STATEID": [f"WI{i:03d}" for i in range(5)], "PARCELID": [f"{i}" for i in range(5)]},
            geometry=[Polygon([(500000 + i * 100, 200000), (500000 + i * 100, 200100), (500100 + i * 100, 200100)]) for i in range(5)],
            crs=WISCONSIN_CRS
        )

        batch_id = uuid4()

        with patch('services.gdb_processor.gpd.read_file', return_value=gdf), \
             patch('services.gdb_processor.publish_message', return_value=True), \
             patch('services.gdb_processor.update_batch_progress', new_callable=AsyncMock) as mock_update, \
             patch('services.gdb_processor.complete_batch', new_callable=AsyncMock):

            await process_gdb_async(
                gdb_path=tmp_path / "test.gdb",
                layer_name="V11_Parcels",
                batch_id=batch_id,
                source_name="Test",
                chunk_size=2  # Process in chunks of 2
            )

            # Verify batch progress was updated 3 times (2+2+1)
            assert mock_update.call_count == 3
