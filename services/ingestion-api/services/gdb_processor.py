"""
GDB (Geodatabase) file processing service.

Handles GDB file uploads for Wisconsin V11 Parcel data:
- ZIP extraction and GDB inspection
- Layer listing and CRS validation
- Geometry processing with GeoPandas
- CRS transformation to EPSG:3071
- RabbitMQ message publishing
- Batch progress tracking
"""

import logging
import zipfile
from pathlib import Path
from typing import Literal, Optional, Dict, Any, List
from uuid import UUID
import shutil

import fiona
import geopandas as gpd
import pandas as pd
from shapely.geometry import shape

from shared.models import V11ParcelRecord
from shared.rabbitmq import publish_message
from .batch_tracker import update_batch_progress, complete_batch, fail_batch

logger = logging.getLogger(__name__)

# Target CRS for Wisconsin data
WISCONSIN_CRS = "EPSG:3071"  # Wisconsin Transverse Mercator


def extract_gdb(zip_path: Path, extract_to: Path) -> Path:
    """
    Extract a .gdb.zip file to a directory.

    Args:
        zip_path: Path to the .gdb.zip file
        extract_to: Directory to extract to

    Returns:
        Path: Path to the extracted .gdb directory

    Raises:
        ValueError: If the zip doesn't contain a .gdb directory
        zipfile.BadZipFile: If the file is not a valid zip
    """
    logger.info(f"Extracting GDB from {zip_path} to {extract_to}")

    # Ensure extraction directory exists
    extract_to.mkdir(parents=True, exist_ok=True)

    # Extract the zip file
    with zipfile.ZipFile(zip_path, 'r') as zip_ref:
        zip_ref.extractall(extract_to)

    # Find the .gdb directory in the extracted files
    gdb_dirs = list(extract_to.glob("**/*.gdb"))

    if not gdb_dirs:
        raise ValueError(
            f"No .gdb directory found in {zip_path}. "
            "Ensure the zip contains a valid GDB."
        )

    if len(gdb_dirs) > 1:
        logger.warning(
            f"Multiple .gdb directories found in {zip_path}. Using first: {gdb_dirs[0]}"
        )

    gdb_path = gdb_dirs[0]
    logger.info(f"Extracted GDB to: {gdb_path}")

    return gdb_path


def inspect_gdb(gdb_path: Path) -> Dict[str, Any]:
    """
    Inspect a GDB file and return metadata.

    Args:
        gdb_path: Path to the .gdb directory

    Returns:
        Dict containing:
        - layers: List of layer names
        - default_layer: First layer name
        - layer_info: Dict of layer metadata (CRS, bounds, feature count)

    Example:
        ```python
        info = inspect_gdb(Path("/tmp/parcels.gdb"))
        print(info["layers"])  # ['V11_Parcels', 'Counties']
        print(info["layer_info"]["V11_Parcels"]["crs"])  # 'EPSG:3071'
        ```
    """
    logger.info(f"Inspecting GDB: {gdb_path}")

    # List all layers in the GDB
    layers = fiona.listlayers(str(gdb_path))

    if not layers:
        raise ValueError(f"No layers found in GDB: {gdb_path}")

    logger.info(f"Found {len(layers)} layer(s): {layers}")

    # Get detailed info for each layer
    layer_info = {}
    for layer_name in layers:
        try:
            with fiona.open(str(gdb_path), layer=layer_name) as src:
                # Get CRS
                crs = src.crs_wkt if src.crs else "Unknown"
                crs_epsg = src.crs.get('init', 'Unknown') if src.crs else 'Unknown'

                # Get bounds
                bounds = src.bounds if hasattr(src, 'bounds') else None

                # Count features
                feature_count = len(src)

                layer_info[layer_name] = {
                    "crs": crs_epsg,
                    "crs_wkt": crs,
                    "bounds": bounds,
                    "feature_count": feature_count,
                    "schema": dict(src.schema) if hasattr(src, 'schema') else None
                }

                logger.info(
                    f"Layer '{layer_name}': {feature_count} features, CRS: {crs_epsg}"
                )

        except Exception as e:
            logger.error(f"Error inspecting layer '{layer_name}': {e}")
            layer_info[layer_name] = {
                "error": str(e)
            }

    return {
        "layers": layers,
        "default_layer": layers[0] if layers else None,
        "layer_info": layer_info
    }


def count_features(gdb_path: Path, layer_name: str) -> int:
    """
    Fast count of features in a GDB layer.

    Args:
        gdb_path: Path to the .gdb directory
        layer_name: Name of the layer to count

    Returns:
        int: Number of features in the layer

    Raises:
        ValueError: If layer doesn't exist
    """
    with fiona.open(str(gdb_path), layer=layer_name) as src:
        count = len(src)

    logger.info(f"Layer '{layer_name}' has {count:,} features")
    return count


def transform_to_wisconsin_crs(gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    """
    Transform GeoDataFrame to Wisconsin CRS (EPSG:3071).

    Args:
        gdf: GeoDataFrame with any CRS

    Returns:
        GeoDataFrame transformed to EPSG:3071

    Note:
        If already in EPSG:3071, returns the original GeoDataFrame.
    """
    if gdf.crs is None:
        logger.warning("GeoDataFrame has no CRS, assuming EPSG:3071")
        gdf.set_crs(WISCONSIN_CRS, inplace=True)
        return gdf

    current_crs = gdf.crs.to_string()

    if current_crs == WISCONSIN_CRS or current_crs == "EPSG:3071":
        logger.debug("GeoDataFrame already in Wisconsin CRS")
        return gdf

    logger.info(f"Transforming CRS from {current_crs} to {WISCONSIN_CRS}")
    gdf = gdf.to_crs(WISCONSIN_CRS)

    return gdf


async def process_gdb_async(
    gdb_path: Path,
    layer_name: str,
    batch_id: UUID,
    source_name: str,
    chunk_size: int = 1000
) -> None:
    """
    Process a GDB file asynchronously.

    Reads GDB layer in chunks, transforms geometries, validates data,
    and publishes to RabbitMQ.

    Args:
        gdb_path: Path to the .gdb directory
        layer_name: Name of the layer to process
        batch_id: Import batch ID for tracking
        source_name: Name of the data source
        chunk_size: Number of features to process per chunk

    Raises:
        Exception: Any processing errors (caller should catch and fail_batch)

    Example:
        ```python
        await process_gdb_async(
            gdb_path=Path("/tmp/dane.gdb"),
            layer_name="V11_Parcels",
            batch_id=uuid4(),
            source_name="Dane County 2025",
            chunk_size=1000
        )
        ```
    """
    logger.info(
        f"Starting GDB processing: {gdb_path}/{layer_name} "
        f"(batch: {batch_id}, source: {source_name})"
    )

    try:
        # Read the entire layer (we'll process in chunks)
        logger.info(f"Reading layer '{layer_name}' from {gdb_path}")
        gdf = gpd.read_file(str(gdb_path), layer=layer_name)

        total_features = len(gdf)
        logger.info(f"Loaded {total_features:,} features from layer '{layer_name}'")

        # Transform to Wisconsin CRS if needed
        gdf = transform_to_wisconsin_crs(gdf)

        # Process in chunks for memory efficiency
        total_processed = 0
        total_failed = 0
        chunk_num = 0

        for start_idx in range(0, total_features, chunk_size):
            end_idx = min(start_idx + chunk_size, total_features)
            chunk = gdf.iloc[start_idx:end_idx]
            chunk_num += 1

            logger.debug(
                f"Processing chunk {chunk_num}: rows {start_idx}-{end_idx} "
                f"({len(chunk)} features)"
            )

            chunk_failed = 0

            for idx, row in chunk.iterrows():
                try:
                    # Extract geometry
                    geometry = row.geometry
                    if geometry is None or geometry.is_empty:
                        logger.warning(f"Row {idx}: Empty or null geometry, skipping")
                        chunk_failed += 1
                        continue

                    # Convert geometry to WKT
                    geometry_wkt = geometry.wkt
                    geometry_type = geometry.geom_type

                    # Build V11ParcelRecord from row data
                    # Map GDB field names to V11 schema (case-insensitive)
                    row_dict = {
                        k: (v if not (isinstance(v, float) and pd.isna(v)) else None)
                        for k, v in row.to_dict().items()
                        if k != 'geometry'  # Exclude geometry column
                    }

                    # Add geometry fields
                    row_dict['geometry_wkt'] = geometry_wkt
                    row_dict['geometry_type'] = geometry_type

                    # Validate with Pydantic model
                    record = V11ParcelRecord(**row_dict)

                    # Publish to RabbitMQ deduplication queue
                    message = {
                        "batch_id": str(batch_id),
                        "source_type": "PARCEL",
                        "source_file": f"{source_name}/{layer_name}",
                        "source_row_number": int(idx) + 1,
                        "raw_data": record.model_dump(exclude_none=True)
                    }

                    success = publish_message('deduplication', message)

                    if not success:
                        logger.error(
                            f"Failed to publish message for feature {idx + 1}"
                        )
                        chunk_failed += 1

                except Exception as e:
                    logger.warning(
                        f"Failed to process feature {idx + 1}: {e}"
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
                f"Chunk {chunk_num} complete: {chunk_successful}/{chunk_processed} succeeded "
                f"(total: {total_processed}/{total_features})"
            )

        # Mark batch as completed
        await complete_batch(batch_id, total_processed)

        logger.info(
            f"GDB processing complete: {total_processed:,} features processed, "
            f"{total_failed:,} failed (batch: {batch_id})"
        )

    except Exception as e:
        logger.error(
            f"GDB processing failed (batch: {batch_id}): {e}",
            exc_info=True
        )
        await fail_batch(batch_id, f"GDB processing error: {str(e)}")
        raise


def validate_gdb_format(gdb_path: Path) -> bool:
    """
    Validate that a path is a valid GDB directory.

    Args:
        gdb_path: Path to check

    Returns:
        bool: True if valid GDB, False otherwise
    """
    if not gdb_path.exists():
        logger.error(f"GDB path does not exist: {gdb_path}")
        return False

    if not gdb_path.is_dir():
        logger.error(f"GDB path is not a directory: {gdb_path}")
        return False

    # Check for required GDB files
    required_files = ['.gdb']  # Fiona checks for internal structure

    try:
        # Try to list layers (if this works, it's a valid GDB)
        layers = fiona.listlayers(str(gdb_path))
        if not layers:
            logger.error(f"No layers found in GDB: {gdb_path}")
            return False

        logger.debug(f"Valid GDB with {len(layers)} layer(s): {gdb_path}")
        return True

    except Exception as e:
        logger.error(f"Invalid GDB format: {e}")
        return False


def cleanup_gdb(gdb_path: Path) -> None:
    """
    Clean up extracted GDB files.

    Args:
        gdb_path: Path to the .gdb directory (or parent extraction dir)
    """
    try:
        # If given a .gdb directory, remove its parent extraction directory
        if gdb_path.name.endswith('.gdb'):
            cleanup_path = gdb_path.parent
        else:
            cleanup_path = gdb_path

        if cleanup_path.exists():
            shutil.rmtree(cleanup_path)
            logger.info(f"Cleaned up GDB extraction: {cleanup_path}")

    except Exception as e:
        logger.warning(f"Failed to cleanup GDB files at {gdb_path}: {e}")


__all__ = [
    "extract_gdb",
    "inspect_gdb",
    "count_features",
    "transform_to_wisconsin_crs",
    "process_gdb_async",
    "validate_gdb_format",
    "cleanup_gdb",
]
