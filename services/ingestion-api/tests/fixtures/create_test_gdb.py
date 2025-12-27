"""
Script to create a synthetic test GDB file with Wisconsin V11 parcel data.

Creates a small GDB (50-100 features) for testing purposes.
"""

import zipfile
from pathlib import Path
import geopandas as gpd
from shapely.geometry import Polygon
import pandas as pd

# Wisconsin CRS
WISCONSIN_CRS = "EPSG:3071"

def create_synthetic_parcels(count: int = 50) -> gpd.GeoDataFrame:
    """Create synthetic Wisconsin parcel data."""

    # Base coordinates in Wisconsin (around Madison area)
    base_x = 500000  # EPSG:3071 coordinates
    base_y = 200000

    data = []
    for i in range(count):
        # Create a roughly 100m x 100m parcel
        x_offset = (i % 10) * 150
        y_offset = (i // 10) * 150

        x = base_x + x_offset
        y = base_y + y_offset

        # Create polygon (parcel boundary)
        polygon = Polygon([
            (x, y),
            (x + 100, y),
            (x + 100, y + 100),
            (x, y + 100),
            (x, y)
        ])

        # Generate V11 parcel attributes
        parcel = {
            "STATEID": f"WI{55000 + i:06d}",
            "PARCELID": f"TEST{i:05d}",
            "TAXPARCELID": f"251{i:07d}",
            "PARCELDATE": "2025-01-01",
            "TAXROLLYEAR": 2025,
            "OWNERNME1": f"Test Owner {i}",
            "OWNERNME2": None,
            "PSTLADRESS": f"{100 + i * 10}",
            "SITEADRESS": f"{100 + i * 10} TEST ST",
            "ADDNUMPREFIX": None,
            "ADDNUM": str(100 + i * 10),
            "ADDNUMSUFFIX": None,
            "PREFIX": None,
            "STREETNAME": "TEST",
            "STREETTYPE": "ST",
            "SUFFIX": None,
            "LANDMARKNAME": None,
            "UNITTYPE": None,
            "UNITID": None,
            "PLACENAME": "MADISON",
            "ZIPCODE": "53704",
            "ZIP4": None,
            "STATE": "WI",
            "SCHOOLDIST": "MADISON METRO",
            "SCHOOLDISTNO": "3269",
            "IMPROVED": 1 if i % 2 == 0 else 0,
            "CNTASSDVALUE": 150000 + (i * 5000),
            "LNDVALUE": 50000 + (i * 1000),
            "IMPVALUE": 100000 + (i * 4000) if i % 2 == 0 else None,
            "FORESTVALUE": None,
            "ESTFMKVALUE": 200000 + (i * 6000),
            "NETPRPTA": 3000 + (i * 50),
            "GRSPRPTA": 3200 + (i * 55),
            "PROPCLASS": "RESIDENTIAL",
            "AUXCLASS": None,
            "ASSDACRES": round(0.25 + (i * 0.01), 2),
            "DEEDACRES": round(0.25 + (i * 0.01), 2),
            "GISACRES": round(0.25 + (i * 0.01), 2),
            "CONAME": "DANE",
            "LOADDATE": "2025-01-15",
            "PARCELFIPS": "55025",
            "PARCELSRC": "TEST_DATA"
        }

        data.append({**parcel, "geometry": polygon})

    return gpd.GeoDataFrame(data, crs=WISCONSIN_CRS)


def main():
    """Create test GDB and zip it."""
    output_dir = Path(__file__).parent
    gdb_name = "test_parcels.gdb"
    gdb_path = output_dir / gdb_name
    zip_path = output_dir / f"{gdb_name}.zip"

    print(f"Creating synthetic parcel data...")
    gdf = create_synthetic_parcels(count=75)

    print(f"Writing to GDB: {gdb_path}")
    gdf.to_file(gdb_path, layer="V11_Parcels", driver="OpenFileGDB")

    print(f"Creating zip archive: {zip_path}")
    with zipfile.ZipFile(zip_path, 'w', zipfile.ZIP_DEFLATED) as zf:
        for file in gdb_path.rglob("*"):
            if file.is_file():
                zf.write(file, file.relative_to(output_dir))

    print(f"✓ Created test GDB with {len(gdf)} features")
    print(f"  - GDB: {gdb_path}")
    print(f"  - ZIP: {zip_path}")
    print(f"  - Layer: V11_Parcels")
    print(f"  - CRS: {WISCONSIN_CRS}")


if __name__ == "__main__":
    main()
