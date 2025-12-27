"""
Unit tests for Pydantic data models.

Tests validation, field types, and model behavior for:
- V11ParcelRecord (Wisconsin V11 Parcel Database)
- RETRRecord (Real Estate Transfer Returns)
- DFIRecord (DFI Corporate Entities)
"""

import pytest
from pydantic import ValidationError

from shared.models import V11ParcelRecord, RETRRecord, DFIRecord


class TestV11ParcelRecord:
    """Tests for V11ParcelRecord model."""

    def test_minimal_valid_record(self):
        """Test creating a record with only required fields."""
        record = V11ParcelRecord(
            geometry_wkt="MULTIPOLYGON(((0 0, 1 0, 1 1, 0 1, 0 0)))",
            geometry_type="MultiPolygon"
        )
        assert record.geometry_wkt == "MULTIPOLYGON(((0 0, 1 0, 1 1, 0 1, 0 0)))"
        assert record.geometry_type == "MultiPolygon"
        assert record.STATEID is None  # Optional fields default to None

    def test_full_record_with_all_fields(self):
        """Test creating a record with all 42 fields populated."""
        record = V11ParcelRecord(
            # Identifiers
            STATEID="WI123456789",
            PARCELID="12-345-678",
            TAXPARCELID="TAX123456",

            # Address Components
            ADDNUMPREFIX="N",
            ADDNUM="123",
            ADDNUMSUFFIX="A",
            PREFIX="N",
            STREETNAME="Main",
            STREETTYPE="St",
            SUFFIX="E",
            LANDMARKNAME="City Hall",
            UNITTYPE="APT",
            UNITID="101",
            PLACENAME="Madison",
            ZIPCODE="53703",
            ZIP4="1234",
            CONAME="Dane",

            # Ownership
            OWNERNME1="John Doe",
            OWNERNME2="Jane Doe",
            PSTLADRESS="PO Box 123",
            SITEADRESS="123 N Main St",

            # Assessment
            CNTASSDVALUE=250000.00,
            LNDVALUE=50000.00,
            IMPVALUE=200000.00,
            ESTFMKVALUE=275000.00,
            ASSESSEDBY="Dane County Assessor",
            ASSESSYEAR="2025",

            # Property Information
            PROPCLASS="Residential",
            AUXCLASS="Single Family",
            ASSDACRES=0.25,
            GISACRES=0.26,

            # Dates
            PARCELDEED="2024-01-15",
            PARCELSRC="County GIS",
            PARCELSRCDATE="2025-01-01",

            # Legal
            LEGALAREA="Lot 5, Block 2, Madison Subdivision",
            SCHOOLDIST="Madison Metropolitan School District",
            SCHOOLDISTNO="3269",

            # Geometry (required)
            geometry_wkt="MULTIPOLYGON(((0 0, 1 0, 1 1, 0 1, 0 0)))",
            geometry_type="MultiPolygon"
        )

        assert record.STATEID == "WI123456789"
        assert record.ADDNUM == "123"
        assert record.STREETNAME == "Main"
        assert record.CNTASSDVALUE == 250000.00
        assert record.ASSDACRES == 0.25

    def test_missing_required_geometry_fields(self):
        """Test that missing geometry fields raise validation error."""
        with pytest.raises(ValidationError) as exc_info:
            V11ParcelRecord(STATEID="WI123456")

        errors = exc_info.value.errors()
        field_names = [e['loc'][0] for e in errors]
        assert 'geometry_wkt' in field_names
        assert 'geometry_type' in field_names

    def test_whitespace_stripping(self):
        """Test that str_strip_whitespace config works."""
        record = V11ParcelRecord(
            STATEID="  WI123456  ",
            STREETNAME="  Main  ",
            geometry_wkt="POLYGON((0 0, 1 0, 1 1, 0 1, 0 0))",
            geometry_type="Polygon"
        )
        assert record.STATEID == "WI123456"  # Whitespace stripped
        assert record.STREETNAME == "Main"

    def test_numeric_field_types(self):
        """Test that numeric fields accept correct types."""
        record = V11ParcelRecord(
            CNTASSDVALUE=250000.50,
            LNDVALUE=50000,  # int should be accepted for float
            ASSDACRES=0.25,
            GISACRES=0.26,
            geometry_wkt="POLYGON((0 0, 1 0, 1 1, 0 1, 0 0))",
            geometry_type="Polygon"
        )
        assert isinstance(record.CNTASSDVALUE, float)
        assert isinstance(record.LNDVALUE, float)
        assert isinstance(record.ASSDACRES, float)

    def test_polygon_geometry_type(self):
        """Test that Polygon geometry type is accepted."""
        record = V11ParcelRecord(
            geometry_wkt="POLYGON((0 0, 1 0, 1 1, 0 1, 0 0))",
            geometry_type="Polygon"
        )
        assert record.geometry_type == "Polygon"


class TestRETRRecord:
    """Tests for RETRRecord model."""

    def test_empty_record(self):
        """Test creating an empty RETR record (all fields optional)."""
        record = RETRRecord()
        assert record.PARCEL_ID is None
        assert record.DOC_NUMBER is None
        assert record.SALE_AMOUNT is None

    def test_full_retr_record(self):
        """Test creating a complete RETR record."""
        record = RETRRecord(
            PARCEL_ID="12-345-678",
            DOC_NUMBER="2025-001234",
            TRANSFER_DATE="2025-01-15",
            RECORDING_DATE="2025-01-20",
            GRANTOR="John Doe",
            GRANTEE="Jane Smith",
            SALE_AMOUNT=350000.00,
            CONVEYANCE_FEE=1050.00,
            PROPERTY_TYPE="R",
            TRANSFER_TYPE="S",
            MUNICIPALITY="Madison",
            COUNTY="Dane",
            IMPROVED="Y",
            NUM_PARCELS=1
        )

        assert record.PARCEL_ID == "12-345-678"
        assert record.SALE_AMOUNT == 350000.00
        assert record.GRANTOR == "John Doe"
        assert record.GRANTEE == "Jane Smith"
        assert record.NUM_PARCELS == 1

    def test_partial_retr_record(self):
        """Test RETR record with only some fields populated."""
        record = RETRRecord(
            PARCEL_ID="12-345-678",
            SALE_AMOUNT=250000.00,
            COUNTY="Dane"
        )
        assert record.PARCEL_ID == "12-345-678"
        assert record.SALE_AMOUNT == 250000.00
        assert record.GRANTOR is None  # Unpopulated fields are None

    def test_numeric_fields(self):
        """Test numeric field types in RETR record."""
        record = RETRRecord(
            SALE_AMOUNT=350000.50,
            CONVEYANCE_FEE=1050.25,
            NUM_PARCELS=3
        )
        assert isinstance(record.SALE_AMOUNT, float)
        assert isinstance(record.CONVEYANCE_FEE, float)
        assert isinstance(record.NUM_PARCELS, int)

    def test_whitespace_stripping_retr(self):
        """Test whitespace stripping in RETR records."""
        record = RETRRecord(
            PARCEL_ID="  12-345-678  ",
            GRANTOR="  John Doe  ",
            COUNTY="  Dane  "
        )
        assert record.PARCEL_ID == "12-345-678"
        assert record.GRANTOR == "John Doe"
        assert record.COUNTY == "Dane"


class TestDFIRecord:
    """Tests for DFIRecord model."""

    def test_empty_dfi_record(self):
        """Test creating an empty DFI record."""
        record = DFIRecord()
        assert record.ENTITY_ID is None
        assert record.ENTITY_NAME is None
        assert record.STATUS is None

    def test_full_dfi_record(self):
        """Test creating a complete DFI record."""
        record = DFIRecord(
            ENTITY_ID="L123456",
            ENTITY_NAME="Wisconsin Real Estate Holdings LLC",
            ENTITY_TYPE="LLC",
            STATUS="Active",
            FORMATION_DATE="2020-06-15",
            EFFECTIVE_DATE="2020-07-01",
            EXPIRATION_DATE="2030-06-30",
            AGENT_NAME="John Smith",
            AGENT_ADDRESS="123 Main St",
            AGENT_CITY="Madison",
            AGENT_STATE="WI",
            AGENT_ZIP="53703",
            PRINCIPAL_ADDRESS="456 State St",
            PRINCIPAL_CITY="Milwaukee",
            PRINCIPAL_STATE="WI",
            PRINCIPAL_ZIP="53202"
        )

        assert record.ENTITY_ID == "L123456"
        assert record.ENTITY_NAME == "Wisconsin Real Estate Holdings LLC"
        assert record.ENTITY_TYPE == "LLC"
        assert record.STATUS == "Active"
        assert record.AGENT_CITY == "Madison"
        assert record.PRINCIPAL_CITY == "Milwaukee"

    def test_partial_dfi_record(self):
        """Test DFI record with minimal fields."""
        record = DFIRecord(
            ENTITY_ID="C987654",
            ENTITY_NAME="Test Corporation",
            STATUS="Dissolved"
        )
        assert record.ENTITY_ID == "C987654"
        assert record.ENTITY_NAME == "Test Corporation"
        assert record.STATUS == "Dissolved"
        assert record.AGENT_NAME is None

    def test_whitespace_stripping_dfi(self):
        """Test whitespace stripping in DFI records."""
        record = DFIRecord(
            ENTITY_NAME="  Test LLC  ",
            AGENT_CITY="  Madison  ",
            AGENT_STATE="  WI  "
        )
        assert record.ENTITY_NAME == "Test LLC"
        assert record.AGENT_CITY == "Madison"
        assert record.AGENT_STATE == "WI"


class TestModelSerialization:
    """Tests for model serialization and deserialization."""

    def test_v11_model_dump(self):
        """Test V11ParcelRecord serialization to dict."""
        record = V11ParcelRecord(
            STATEID="WI123456",
            ADDNUM="123",
            STREETNAME="Main",
            geometry_wkt="POLYGON((0 0, 1 0, 1 1, 0 1, 0 0))",
            geometry_type="Polygon"
        )
        data = record.model_dump()

        assert isinstance(data, dict)
        assert data['STATEID'] == "WI123456"
        assert data['geometry_wkt'] == "POLYGON((0 0, 1 0, 1 1, 0 1, 0 0))"
        assert 'PARCELID' in data  # All fields present, even if None

    def test_v11_model_dump_exclude_none(self):
        """Test V11ParcelRecord serialization excluding None values."""
        record = V11ParcelRecord(
            STATEID="WI123456",
            geometry_wkt="POLYGON((0 0, 1 0, 1 1, 0 1, 0 0))",
            geometry_type="Polygon"
        )
        data = record.model_dump(exclude_none=True)

        assert 'STATEID' in data
        assert 'geometry_wkt' in data
        assert 'PARCELID' not in data  # None values excluded

    def test_retr_model_json(self):
        """Test RETRRecord JSON serialization."""
        record = RETRRecord(
            PARCEL_ID="12-345-678",
            SALE_AMOUNT=250000.00
        )
        json_str = record.model_dump_json()

        assert isinstance(json_str, str)
        assert "12-345-678" in json_str
        assert "250000" in json_str

    def test_dfi_model_parse(self):
        """Test DFIRecord deserialization from dict."""
        data = {
            "ENTITY_ID": "L123456",
            "ENTITY_NAME": "Test LLC",
            "STATUS": "Active"
        }
        record = DFIRecord(**data)

        assert record.ENTITY_ID == "L123456"
        assert record.ENTITY_NAME == "Test LLC"
        assert record.STATUS == "Active"
