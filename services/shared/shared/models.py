"""
Pydantic models for Wisconsin Real Estate data.

This module contains data models for:
- V11ParcelRecord: Wisconsin V11 Statewide Parcel Database schema (42 fields)
- RETRRecord: Real Estate Transfer Return records
- DFIRecord: Department of Financial Institutions corporate entity records
"""

from typing import Optional
from pydantic import BaseModel, Field, ConfigDict


class V11ParcelRecord(BaseModel):
    """
    Wisconsin V11 Statewide Parcel Database schema.

    Represents a parcel record from Wisconsin's statewide parcel database with
    complete address, ownership, assessment, and legal information.

    Reference: Wisconsin V11 Parcel Schema Specification
    """

    model_config = ConfigDict(str_strip_whitespace=True)

    # === Identifiers ===
    STATEID: Optional[str] = Field(None, description="Unique state parcel ID (primary)")
    PARCELID: Optional[str] = Field(None, description="Local parcel ID")
    TAXPARCELID: Optional[str] = Field(None, description="Tax parcel ID")

    # === Address Components ===
    ADDNUMPREFIX: Optional[str] = Field(None, description="Address number prefix")
    ADDNUM: Optional[str] = Field(None, description="Address number")
    ADDNUMSUFFIX: Optional[str] = Field(None, description="Address number suffix")
    PREFIX: Optional[str] = Field(None, description="Street prefix (N, S, E, W)")
    STREETNAME: Optional[str] = Field(None, description="Street name")
    STREETTYPE: Optional[str] = Field(None, description="Street type (ST, AVE, RD)")
    SUFFIX: Optional[str] = Field(None, description="Street suffix")
    LANDMARKNAME: Optional[str] = Field(None, description="Landmark name")
    UNITTYPE: Optional[str] = Field(None, description="Unit type (APT, STE)")
    UNITID: Optional[str] = Field(None, description="Unit ID")
    PLACENAME: Optional[str] = Field(None, description="Municipality name")
    ZIPCODE: Optional[str] = Field(None, description="5-digit ZIP code")
    ZIP4: Optional[str] = Field(None, description="ZIP+4 extension")
    CONAME: Optional[str] = Field(None, description="County name")

    # === Ownership ===
    OWNERNME1: Optional[str] = Field(None, description="Owner name line 1")
    OWNERNME2: Optional[str] = Field(None, description="Owner name line 2")
    PSTLADRESS: Optional[str] = Field(None, description="Tax bill mailing address")
    SITEADRESS: Optional[str] = Field(None, description="Site address")

    # === Assessment ===
    CNTASSDVALUE: Optional[float] = Field(None, description="Total assessed value")
    LNDVALUE: Optional[float] = Field(None, description="Land value")
    IMPVALUE: Optional[float] = Field(None, description="Improvement value")
    ESTFMKVALUE: Optional[float] = Field(None, description="Estimated fair market value")
    ASSESSEDBY: Optional[str] = Field(None, description="Assessor")
    ASSESSYEAR: Optional[str] = Field(None, description="Assessment year")

    # === Property Information ===
    PROPCLASS: Optional[str] = Field(None, description="Property class")
    AUXCLASS: Optional[str] = Field(None, description="Auxiliary class")
    ASSDACRES: Optional[float] = Field(None, description="Assessed acres")
    GISACRES: Optional[float] = Field(None, description="GIS-calculated acres")

    # === Dates ===
    PARCELDEED: Optional[str] = Field(None, description="Deed date")
    PARCELSRC: Optional[str] = Field(None, description="Data source")
    PARCELSRCDATE: Optional[str] = Field(None, description="Source date")

    # === Legal ===
    LEGALAREA: Optional[str] = Field(None, description="Legal description")
    SCHOOLDIST: Optional[str] = Field(None, description="School district name")
    SCHOOLDISTNO: Optional[str] = Field(None, description="School district number")

    # === Geometry (required) ===
    geometry_wkt: str = Field(..., description="Well-Known Text geometry")
    geometry_type: str = Field(..., description="Geometry type (MultiPolygon, Polygon, etc.)")


class RETRRecord(BaseModel):
    """
    Real Estate Transfer Return (RETR) record.

    Represents a real estate transfer transaction from Wisconsin DOR.
    Contains information about property sales, transfers, and deed recordings.

    Reference: Wisconsin RETR CSV Specification
    """

    model_config = ConfigDict(str_strip_whitespace=True)

    # === Identifiers ===
    PARCEL_ID: Optional[str] = Field(None, description="Parcel identifier")
    DOC_NUMBER: Optional[str] = Field(None, description="Document number")

    # === Transfer Information ===
    TRANSFER_DATE: Optional[str] = Field(None, description="Transfer date (YYYY-MM-DD)")
    RECORDING_DATE: Optional[str] = Field(None, description="Recording date (YYYY-MM-DD)")

    # === Parties ===
    GRANTOR: Optional[str] = Field(None, description="Grantor name (seller)")
    GRANTEE: Optional[str] = Field(None, description="Grantee name (buyer)")

    # === Financial ===
    SALE_AMOUNT: Optional[float] = Field(None, description="Sale amount")
    CONVEYANCE_FEE: Optional[float] = Field(None, description="Conveyance fee")

    # === Property Details ===
    PROPERTY_TYPE: Optional[str] = Field(None, description="Property type code")
    TRANSFER_TYPE: Optional[str] = Field(None, description="Transfer type code")

    # === Location ===
    MUNICIPALITY: Optional[str] = Field(None, description="Municipality name")
    COUNTY: Optional[str] = Field(None, description="County name")

    # === Additional Fields ===
    IMPROVED: Optional[str] = Field(None, description="Improved property flag (Y/N)")
    NUM_PARCELS: Optional[int] = Field(None, description="Number of parcels in transaction")


class DFIRecord(BaseModel):
    """
    Department of Financial Institutions (DFI) corporate entity record.

    Represents a corporate entity registered with Wisconsin DFI.
    Used for linking property ownership to corporations and LLCs.

    Reference: Wisconsin DFI Entity Database
    """

    model_config = ConfigDict(str_strip_whitespace=True)

    # === Identifiers ===
    ENTITY_ID: Optional[str] = Field(None, description="Unique entity ID")
    ENTITY_NAME: Optional[str] = Field(None, description="Legal entity name")

    # === Entity Information ===
    ENTITY_TYPE: Optional[str] = Field(None, description="Entity type (LLC, Corp, etc.)")
    STATUS: Optional[str] = Field(None, description="Entity status (Active, Dissolved, etc.)")

    # === Registration ===
    FORMATION_DATE: Optional[str] = Field(None, description="Formation/registration date")
    EFFECTIVE_DATE: Optional[str] = Field(None, description="Effective date")
    EXPIRATION_DATE: Optional[str] = Field(None, description="Expiration date")

    # === Registered Agent ===
    AGENT_NAME: Optional[str] = Field(None, description="Registered agent name")
    AGENT_ADDRESS: Optional[str] = Field(None, description="Registered agent address")
    AGENT_CITY: Optional[str] = Field(None, description="Registered agent city")
    AGENT_STATE: Optional[str] = Field(None, description="Registered agent state")
    AGENT_ZIP: Optional[str] = Field(None, description="Registered agent ZIP code")

    # === Principal Office ===
    PRINCIPAL_ADDRESS: Optional[str] = Field(None, description="Principal office address")
    PRINCIPAL_CITY: Optional[str] = Field(None, description="Principal office city")
    PRINCIPAL_STATE: Optional[str] = Field(None, description="Principal office state")
    PRINCIPAL_ZIP: Optional[str] = Field(None, description="Principal office ZIP code")


__all__ = [
    "V11ParcelRecord",
    "RETRRecord",
    "DFIRecord",
]
