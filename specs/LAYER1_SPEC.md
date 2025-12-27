# Layer 1: Ingestion & Raw Storage - Implementation Specification

## Overview

Layer 1 is the entry point for all data into the system. It receives data files (GDB, CSV), computes content hashes, deduplicates records, and stores raw data in PostgreSQL. Successfully processed records are published to RabbitMQ for downstream processing.

**Success Criteria**:
- No duplicate records processed (100% deduplication accuracy)
- Complete audit trail (every record traceable to source)
- Fast ingestion (5,000+ records/second)
- Resilient to failures (retry logic, dead letter queues)

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                        LAYER 1 SERVICES                          │
├─────────────────────────────────────────────────────────────────┤
│                                                                  │
│  ┌──────────────────┐         ┌─────────────────────────┐      │
│  │  ingestion-api   │────────▶│  deduplication-service  │      │
│  │  (FastAPI)       │         │  (RabbitMQ consumer)    │      │
│  │                  │         │                         │      │
│  │  - Upload GDB    │         │  - Hash computation     │      │
│  │  - Upload CSV    │         │  - Duplicate check      │      │
│  │  - Extract/parse │         │  - Insert raw_imports   │      │
│  │  - Stream to MQ  │         │  - Publish to Layer 2   │      │
│  └──────────────────┘         └─────────────────────────┘      │
│         │                                │                      │
│         ▼                                ▼                      │
│  ┌────────────────────────────────────────────────────┐        │
│  │              RabbitMQ Queues                       │        │
│  │  - deduplication (ingestion → dedupe)             │        │
│  │  - processing.parcel (dedupe → Layer 2)           │        │
│  │  - processing.retr                                 │        │
│  │  - processing.dfi                                  │        │
│  └────────────────────────────────────────────────────┘        │
│                                │                                │
│                                ▼                                │
│  ┌────────────────────────────────────────────────────┐        │
│  │         PostgreSQL: raw_imports table              │        │
│  │  - Stores all original records                     │        │
│  │  - Content hash for deduplication                  │        │
│  │  - Processing status tracking                      │        │
│  └────────────────────────────────────────────────────┘        │
└─────────────────────────────────────────────────────────────────┘
```

---

## Service 1: ingestion-api

### Purpose
REST API for uploading data files. Handles GDB extraction, CSV parsing, and streaming records to the deduplication queue.

### Technology Stack
- **Framework**: FastAPI (Python 3.11+)
- **GDB Parsing**: GDAL, Fiona, GeoPandas
- **CSV Parsing**: Python csv module, pandas
- **Message Queue**: pika (RabbitMQ client)
- **Database**: asyncpg (PostgreSQL async client)

### Directory Structure
```
services/ingestion-api/
├── Dockerfile
├── requirements.txt
├── main.py                    # FastAPI app entry point
├── routers/
│   ├── __init__.py
│   ├── gdb_ingest.py         # POST /api/v1/ingest/parcel/gdb
│   ├── csv_ingest.py         # POST /api/v1/ingest/parcel/csv, /retr, /dfi
│   └── status.py             # GET /api/v1/ingest/status/{batch_id}
├── services/
│   ├── __init__.py
│   ├── gdb_processor.py      # GDB extraction and parsing
│   ├── csv_processor.py      # CSV parsing
│   └── batch_tracker.py      # Import batch management
├── models/
│   ├── __init__.py
│   └── schemas.py            # Pydantic models
└── tests/
    ├── test_gdb_ingest.py
    ├── test_csv_ingest.py
    └── fixtures/
        ├── sample.gdb.zip
        └── sample_parcels.csv
```

### API Endpoints

#### 1. Upload GDB File

```http
POST /api/v1/ingest/parcel/gdb
Content-Type: multipart/form-data

Parameters:
- file: UploadFile (required) - .gdb.zip or .gdb folder
- source_name: str (required) - Identifier (e.g., "Dane_County_2025")
- layer_name: str (default: "V11_Parcels") - Feature class name

Response 202 Accepted:
{
  "batch_id": "550e8400-e29b-41d4-a716-446655440000",
  "status": "processing",
  "total_features": 183425,
  "layer_info": {
    "layers": ["V11_Parcels", "V11_Addresses"],
    "crs": "EPSG:3071",
    "bounds": [32000.0, 222000.0, 168000.0, 398000.0]
  },
  "estimated_time_minutes": 5
}

Error Responses:
- 400 Bad Request: Invalid GDB, layer not found
- 413 Payload Too Large: File > 5GB
- 500 Internal Server Error: Processing failure
```

**Implementation**:
```python
# routers/gdb_ingest.py
from fastapi import APIRouter, File, UploadFile, Form, HTTPException
from uuid import uuid4
import asyncio

router = APIRouter()

@router.post("/api/v1/ingest/parcel/gdb")
async def ingest_gdb(
    file: UploadFile = File(...),
    source_name: str = Form(...),
    layer_name: str = Form(default="V11_Parcels")
):
    # 1. Validate file size
    if file.size > 5_000_000_000:  # 5GB limit
        raise HTTPException(413, "File too large")
    
    # 2. Create batch record
    batch_id = uuid4()
    await create_batch(batch_id, source_name, "PARCEL", "GDB")
    
    # 3. Save to temp location
    temp_path = await save_upload(file)
    
    # 4. Extract GDB (if zipped)
    gdb_path = await extract_gdb(temp_path)
    
    # 5. Inspect layers
    layer_info = inspect_gdb(gdb_path)
    if layer_name not in layer_info['layers']:
        raise HTTPException(400, f"Layer '{layer_name}' not found")
    
    # 6. Count features
    feature_count = count_features(gdb_path, layer_name)
    
    # 7. Start async processing
    asyncio.create_task(
        process_gdb_async(gdb_path, layer_name, batch_id, source_name)
    )
    
    return {
        "batch_id": str(batch_id),
        "status": "processing",
        "total_features": feature_count,
        "layer_info": layer_info,
        "estimated_time_minutes": estimate_time(feature_count)
    }
```

#### 2. Upload CSV File

```http
POST /api/v1/ingest/parcel/csv
POST /api/v1/ingest/retr
POST /api/v1/ingest/dfi
Content-Type: multipart/form-data

Parameters:
- file: UploadFile (required) - CSV file
- source_name: str (required) - Identifier

Response 202 Accepted:
{
  "batch_id": "uuid",
  "status": "processing",
  "total_rows": 50000,
  "estimated_time_minutes": 2
}
```

#### 3. Check Import Status

```http
GET /api/v1/ingest/status/{batch_id}

Response 200 OK:
{
  "batch_id": "uuid",
  "status": "processing",  // processing, completed, failed
  "progress": 67.3,
  "total_records": 183425,
  "processed_records": 123456,
  "new_records": 112000,
  "duplicate_records": 11456,
  "failed_records": 0,
  "started_at": "2025-01-15T14:30:00Z",
  "completed_at": null,
  "estimated_completion": "2025-01-15T14:35:00Z",
  "error": null
}
```

### GDB Processing Logic

```python
# services/gdb_processor.py
import fiona
import geopandas as gpd
from pathlib import Path
import zipfile

async def process_gdb_async(
    gdb_path: Path,
    layer_name: str,
    batch_id: UUID,
    source_name: str
):
    """Process GDB file and publish records to deduplication queue."""
    
    try:
        # Read GDB layer
        gdf = gpd.read_file(gdb_path, layer=layer_name)
        
        # Ensure Wisconsin CRS (EPSG:3071)
        if gdf.crs.to_epsg() != 3071:
            gdf = gdf.to_crs(epsg=3071)
        
        # Process in batches
        batch_size = 1000
        rabbitmq = await get_rabbitmq_connection()
        
        for i in range(0, len(gdf), batch_size):
            batch = gdf.iloc[i:i+batch_size]
            
            # Convert to records
            for idx, row in batch.iterrows():
                record = {
                    'geometry_wkt': row.geometry.wkt,
                    'geometry_type': row.geometry.geom_type,
                    
                    # V11 Schema fields (see full list below)
                    'STATEID': row.get('STATEID'),
                    'PARCELID': row.get('PARCELID'),
                    'ADDNUM': row.get('ADDNUM'),
                    'STREETNAME': row.get('STREETNAME'),
                    # ... all V11 fields
                }
                
                # Publish to deduplication queue
                message = {
                    'batch_id': str(batch_id),
                    'source_type': 'PARCEL',
                    'source_file': f"{source_name}/{layer_name}",
                    'source_row_number': idx + 1,
                    'raw_data': record
                }
                
                await rabbitmq.publish('deduplication', message)
            
            # Update progress
            progress = ((i + len(batch)) / len(gdf)) * 100
            await update_batch_progress(batch_id, progress)
        
        # Mark complete
        await complete_batch(batch_id, len(gdf))
        
    except Exception as e:
        await fail_batch(batch_id, str(e))
        raise
    finally:
        # Cleanup temp files
        shutil.rmtree(gdb_path.parent, ignore_errors=True)
```

### V11 Schema Fields (Complete List)

```python
# models/schemas.py
from pydantic import BaseModel
from typing import Optional

class V11ParcelRecord(BaseModel):
    """Wisconsin V11 Statewide Parcel Database schema."""
    
    # Identifiers
    STATEID: Optional[str]           # Unique parcel ID (primary)
    PARCELID: Optional[str]          # Local parcel ID
    TAXPARCELID: Optional[str]       # Tax parcel ID
    
    # Address Components
    ADDNUMPREFIX: Optional[str]      # Address number prefix
    ADDNUM: Optional[str]            # Address number
    ADDNUMSUFFIX: Optional[str]      # Address number suffix
    PREFIX: Optional[str]            # Street prefix (N, S, E, W)
    STREETNAME: Optional[str]        # Street name
    STREETTYPE: Optional[str]        # Street type (ST, AVE, RD)
    SUFFIX: Optional[str]            # Street suffix
    LANDMARKNAME: Optional[str]      # Landmark name
    UNITTYPE: Optional[str]          # Unit type (APT, STE)
    UNITID: Optional[str]            # Unit ID
    PLACENAME: Optional[str]         # Municipality name
    ZIPCODE: Optional[str]           # 5-digit ZIP
    ZIP4: Optional[str]              # ZIP+4
    CONAME: Optional[str]            # County name
    
    # Ownership
    OWNERNME1: Optional[str]         # Owner name line 1
    OWNERNME2: Optional[str]         # Owner name line 2
    PSTLADRESS: Optional[str]        # Tax bill mailing address
    SITEADRESS: Optional[str]        # Site address
    
    # Assessment
    CNTASSDVALUE: Optional[float]    # Total assessed value
    LNDVALUE: Optional[float]        # Land value
    IMPVALUE: Optional[float]        # Improvement value
    ESTFMKVALUE: Optional[float]     # Estimated fair market value
    ASSESSEDBY: Optional[str]        # Assessor
    ASSESSYEAR: Optional[str]        # Assessment year
    
    # Property Information
    PROPCLASS: Optional[str]         # Property class
    AUXCLASS: Optional[str]          # Auxiliary class
    ASSDACRES: Optional[float]       # Assessed acres
    GISACRES: Optional[float]        # GIS-calculated acres
    
    # Dates
    PARCELDEED: Optional[str]        # Deed date
    PARCELSRC: Optional[str]         # Data source
    PARCELSRCDATE: Optional[str]     # Source date
    
    # Legal
    LEGALAREA: Optional[str]         # Legal description
    SCHOOLDIST: Optional[str]        # School district name
    SCHOOLDISTNO: Optional[str]      # School district number
    
    # Geometry
    geometry_wkt: str                # Well-Known Text geometry
    geometry_type: str               # MultiPolygon, Polygon, etc.
```

### Configuration

```python
# config.py
from pydantic_settings import BaseSettings

class Settings(BaseSettings):
    # Database
    DATABASE_URL: str
    DB_POOL_SIZE: int = 20
    
    # RabbitMQ
    RABBITMQ_URL: str
    RABBITMQ_EXCHANGE: str = "ingestion.direct"
    
    # Upload limits
    MAX_UPLOAD_SIZE_MB: int = 5000
    TEMP_STORAGE_PATH: str = "/tmp/gdb-processing"
    
    # Processing
    BATCH_SIZE: int = 1000
    
    class Config:
        env_file = ".env"

settings = Settings()
```

### Dockerfile

```dockerfile
FROM python:3.11-slim

# Install GDAL for GDB support
RUN apt-get update && apt-get install -y \
    gdal-bin \
    libgdal-dev \
    python3-gdal \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY . .

# Expose port
EXPOSE 8080

# Run application
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8080"]
```

### requirements.txt

```
fastapi==0.109.0
uvicorn[standard]==0.27.0
python-multipart==0.0.6
pydantic==2.5.3
pydantic-settings==2.1.0

# GDB processing
fiona==1.9.5
geopandas==0.14.2
shapely==2.0.2
pyproj==3.6.1

# CSV processing
pandas==2.2.0

# Database
asyncpg==0.29.0

# Message queue
pika==1.3.2

# Utilities
python-dateutil==2.8.2
```

---

## Service 2: deduplication-service

### Purpose
Consumes records from RabbitMQ, computes content hashes, checks for duplicates in PostgreSQL, and either inserts new records or skips duplicates. Publishes new records to processing queues.

### Directory Structure
```
services/deduplication-service/
├── Dockerfile
├── requirements.txt
├── main.py                    # RabbitMQ consumer
├── hash_functions.py          # Hash computation
├── database.py                # Database operations
└── tests/
    ├── test_hash_functions.py
    └── test_deduplication.py
```

### Hash Functions

```python
# hash_functions.py
import hashlib
import json
from typing import Dict, Any

def compute_parcel_hash(raw_data: Dict[str, Any]) -> str:
    """
    Compute SHA-256 hash for parcel record.
    
    Includes semantic fields that define uniqueness:
    - Location: STATEID, address components, PLACENAME, ZIPCODE, CONAME
    - Ownership: OWNERNME1, OWNERNME2, PSTLADRESS
    - Assessment: ASSESSYEAR, CNTASSDVALUE, PROPCLASS
    
    Version: v1 (prepended to hash)
    """
    
    # Extract fields in canonical order
    canonical = {
        'STATEID': normalize_string(raw_data.get('STATEID')),
        'ADDNUM': normalize_string(raw_data.get('ADDNUM')),
        'STREETNAME': normalize_string(raw_data.get('STREETNAME')),
        'STREETTYPE': normalize_string(raw_data.get('STREETTYPE')),
        'PLACENAME': normalize_string(raw_data.get('PLACENAME')),
        'ZIPCODE': normalize_string(raw_data.get('ZIPCODE')),
        'CONAME': normalize_string(raw_data.get('CONAME')),
        'OWNERNME1': normalize_string(raw_data.get('OWNERNME1')),
        'OWNERNME2': normalize_string(raw_data.get('OWNERNME2')),
        'PSTLADRESS': normalize_string(raw_data.get('PSTLADRESS')),
        'ASSESSYEAR': normalize_string(raw_data.get('ASSESSYEAR')),
        'CNTASSDVALUE': normalize_number(raw_data.get('CNTASSDVALUE')),
        'PROPCLASS': normalize_string(raw_data.get('PROPCLASS')),
    }
    
    # Serialize to JSON (sorted keys for stability)
    canonical_json = json.dumps(canonical, sort_keys=True)
    
    # Compute hash
    hash_obj = hashlib.sha256(canonical_json.encode('utf-8'))
    hash_hex = hash_obj.hexdigest()
    
    # Prepend version
    return f"v1:{hash_hex}"


def compute_retr_hash(raw_data: Dict[str, Any]) -> str:
    """
    Compute SHA-256 hash for RETR (Real Estate Transfer Return) record.
    
    Includes:
    - Parcel ID (normalized)
    - Transfer date
    - Document number
    - Grantor/grantee names
    - Sale amount
    """
    
    canonical = {
        'parcel_id': normalize_parcel_id(raw_data.get('PARCEL_ID')),
        'transfer_date': normalize_date(raw_data.get('TRANSFER_DATE')),
        'doc_number': normalize_string(raw_data.get('DOC_NUMBER')),
        'grantor': normalize_string(raw_data.get('GRANTOR')),
        'grantee': normalize_string(raw_data.get('GRANTEE')),
        'sale_amount': normalize_number(raw_data.get('SALE_AMOUNT')),
    }
    
    canonical_json = json.dumps(canonical, sort_keys=True)
    hash_obj = hashlib.sha256(canonical_json.encode('utf-8'))
    return f"v1:{hash_obj.hexdigest()}"


def compute_dfi_hash(raw_data: Dict[str, Any]) -> str:
    """
    Compute SHA-256 hash for DFI (corporate entity) record.
    
    Includes:
    - Entity ID
    - Entity name
    - Entity type
    - Status
    - Registered agent
    - Effective date
    """
    
    canonical = {
        'entity_id': normalize_string(raw_data.get('ENTITY_ID')),
        'entity_name': normalize_string(raw_data.get('ENTITY_NAME')),
        'entity_type': normalize_string(raw_data.get('ENTITY_TYPE')),
        'status': normalize_string(raw_data.get('STATUS')),
        'agent_name': normalize_string(raw_data.get('AGENT_NAME')),
        'agent_address': normalize_string(raw_data.get('AGENT_ADDRESS')),
        'effective_date': normalize_date(raw_data.get('EFFECTIVE_DATE')),
    }
    
    canonical_json = json.dumps(canonical, sort_keys=True)
    hash_obj = hashlib.sha256(canonical_json.encode('utf-8'))
    return f"v1:{hash_obj.hexdigest()}"


# Normalization helpers
def normalize_string(value: Any) -> str:
    """Uppercase, trim, handle None."""
    if value is None:
        return ""
    return str(value).upper().strip()

def normalize_number(value: Any) -> str:
    """Convert to string with fixed precision."""
    if value is None:
        return "0"
    try:
        return f"{float(value):.2f}"
    except (ValueError, TypeError):
        return "0"

def normalize_date(value: Any) -> str:
    """Convert to ISO date string."""
    if value is None:
        return ""
    # Parse and format as YYYY-MM-DD
    # ... date parsing logic
    return str(value)

def normalize_parcel_id(value: Any) -> str:
    """Remove dashes, spaces, normalize."""
    if value is None:
        return ""
    return str(value).upper().replace('-', '').replace(' ', '').strip()
```

### Main Consumer Logic

```python
# main.py
import asyncio
import asyncpg
import pika
import json
from uuid import uuid4
from datetime import datetime
from hash_functions import (
    compute_parcel_hash,
    compute_retr_hash,
    compute_dfi_hash
)

async def process_message(message: dict, db_pool: asyncpg.Pool, rabbitmq_channel):
    """
    Process a single message from deduplication queue.
    
    Steps:
    1. Compute content hash
    2. Check if hash exists in raw_imports
    3. If duplicate: log and skip
    4. If new: insert to raw_imports and publish to processing queue
    """
    
    source_type = message['source_type']
    raw_data = message['raw_data']
    batch_id = message['batch_id']
    
    # Step 1: Compute hash based on source type
    hash_functions = {
        'PARCEL': compute_parcel_hash,
        'RETR': compute_retr_hash,
        'DFI': compute_dfi_hash
    }
    
    content_hash = hash_functions[source_type](raw_data)
    
    # Step 2: Check for duplicate
    async with db_pool.acquire() as conn:
        existing = await conn.fetchrow(
            "SELECT record_id FROM raw_imports WHERE content_hash = $1",
            content_hash
        )
        
        if existing:
            # Duplicate found - log and skip
            await log_duplicate(conn, batch_id, content_hash, existing['record_id'])
            return None  # Skip processing
        
        # Step 3: Insert new record
        record_id = uuid4()
        await conn.execute("""
            INSERT INTO raw_imports (
                record_id,
                content_hash,
                import_batch_id,
                source_type,
                source_file,
                source_row_number,
                imported_at,
                raw_data,
                processing_status
            ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
        """,
            record_id,
            content_hash,
            batch_id,
            source_type,
            message['source_file'],
            message['source_row_number'],
            datetime.utcnow(),
            json.dumps(raw_data),
            'pending'
        )
    
    # Step 4: Publish to processing queue
    processing_message = {
        'record_id': str(record_id),
        'source_type': source_type,
        'batch_id': batch_id
    }
    
    queue_name = f"processing.{source_type.lower()}"
    rabbitmq_channel.basic_publish(
        exchange='',
        routing_key=queue_name,
        body=json.dumps(processing_message),
        properties=pika.BasicProperties(
            delivery_mode=2,  # Persistent
            content_type='application/json'
        )
    )
    
    return record_id


async def main():
    """Main consumer loop."""
    
    # Connect to database
    db_pool = await asyncpg.create_pool(
        'postgresql://user:pass@timescaledb:5432/realestate',
        min_size=10,
        max_size=20
    )
    
    # Connect to RabbitMQ
    connection = pika.BlockingConnection(
        pika.ConnectionParameters('rabbitmq')
    )
    channel = connection.channel()
    
    # Declare queue
    channel.queue_declare(queue='deduplication', durable=True)
    
    # Consume messages
    def callback(ch, method, properties, body):
        message = json.loads(body)
        
        # Process message (async)
        loop = asyncio.get_event_loop()
        result = loop.run_until_complete(
            process_message(message, db_pool, channel)
        )
        
        # Acknowledge
        ch.basic_ack(delivery_tag=method.delivery_tag)
    
    channel.basic_qos(prefetch_count=100)  # Batch acknowledgments
    channel.basic_consume(queue='deduplication', on_message_callback=callback)
    
    print("Deduplication service started. Waiting for messages...")
    channel.start_consuming()


if __name__ == "__main__":
    asyncio.run(main())
```

---

## Database Schema

### raw_imports Table

```sql
-- Layer 1: Raw data storage with deduplication
CREATE TABLE raw_imports (
    -- Primary key
    record_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    
    -- Deduplication
    content_hash VARCHAR(64) NOT NULL,
    
    -- Import tracking
    import_batch_id UUID NOT NULL,
    source_type VARCHAR(20) NOT NULL CHECK (source_type IN ('PARCEL', 'RETR', 'DFI')),
    source_file TEXT NOT NULL,
    source_row_number INTEGER,
    imported_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    
    -- Original data (preserved as JSONB)
    raw_data JSONB NOT NULL,
    
    -- Processing status
    processing_status VARCHAR(20) DEFAULT 'pending' 
        CHECK (processing_status IN ('pending', 'processing', 'processed', 'failed', 'skipped')),
    processed_at TIMESTAMPTZ,
    processing_error TEXT,
    
    -- Matching results (populated by Layer 2)
    matched_parcel_id UUID,
    match_confidence NUMERIC(5,4),
    match_method VARCHAR(50)
);

-- Unique index on content hash (enforces deduplication)
CREATE UNIQUE INDEX idx_raw_imports_hash ON raw_imports(content_hash);

-- Index on batch for queries
CREATE INDEX idx_raw_imports_batch ON raw_imports(import_batch_id);

-- Index on processing status
CREATE INDEX idx_raw_imports_status ON raw_imports(processing_status) 
    WHERE processing_status = 'pending';

-- Index on source type for filtering
CREATE INDEX idx_raw_imports_source_type ON raw_imports(source_type);

-- Composite index for batch progress queries
CREATE INDEX idx_raw_imports_batch_status ON raw_imports(import_batch_id, processing_status);
```

### import_batches Table

```sql
-- Tracks import batches
CREATE TABLE import_batches (
    batch_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    source_name TEXT NOT NULL,
    source_type VARCHAR(20) NOT NULL CHECK (source_type IN ('PARCEL', 'RETR', 'DFI')),
    file_format VARCHAR(10) NOT NULL CHECK (file_format IN ('GDB', 'CSV')),
    file_size_bytes BIGINT,
    
    -- Status
    status VARCHAR(20) DEFAULT 'processing'
        CHECK (status IN ('processing', 'completed', 'failed')),
    
    -- Counts
    total_records INTEGER,
    processed_records INTEGER DEFAULT 0,
    new_records INTEGER DEFAULT 0,
    duplicate_records INTEGER DEFAULT 0,
    failed_records INTEGER DEFAULT 0,
    
    -- Timing
    started_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    completed_at TIMESTAMPTZ,
    
    -- Error tracking
    error TEXT
);

CREATE INDEX idx_import_batches_status ON import_batches(status);
CREATE INDEX idx_import_batches_started ON import_batches(started_at DESC);
```

### duplicate_log Table (Optional - for analytics)

```sql
-- Log duplicate detections for analysis
CREATE TABLE duplicate_log (
    log_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    batch_id UUID NOT NULL,
    content_hash VARCHAR(64) NOT NULL,
    existing_record_id UUID NOT NULL,
    detected_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_duplicate_log_batch ON duplicate_log(batch_id);
CREATE INDEX idx_duplicate_log_hash ON duplicate_log(content_hash);
```

---

## Message Formats

### Message: Deduplication Queue

```json
{
  "batch_id": "550e8400-e29b-41d4-a716-446655440000",
  "source_type": "PARCEL",
  "source_file": "Dane_County_2025/V11_Parcels",
  "source_row_number": 12345,
  "raw_data": {
    "STATEID": "WI123456",
    "PARCELID": "251-1234-567",
    "ADDNUM": "123",
    "STREETNAME": "MAIN",
    "STREETTYPE": "ST",
    "PLACENAME": "MADISON",
    "ZIPCODE": "53703",
    "CONAME": "DANE",
    "OWNERNME1": "SMITH JOHN",
    "PSTLADRESS": "456 OAK AVE MADISON WI 53704",
    "CNTASSDVALUE": 450000,
    "ASSESSYEAR": "2024",
    "geometry_wkt": "MULTIPOLYGON(...)",
    "geometry_type": "MultiPolygon"
  }
}
```

### Message: Processing Queue (Output)

```json
{
  "record_id": "660e8400-e29b-41d4-a716-446655440000",
  "source_type": "PARCEL",
  "batch_id": "550e8400-e29b-41d4-a716-446655440000"
}
```

---

## RabbitMQ Configuration

### Queue Declarations

```python
# Deduplication input queue
channel.queue_declare(
    queue='deduplication',
    durable=True,
    arguments={
        'x-queue-type': 'quorum',  # High availability
        'x-max-length': 1000000,    # Max 1M messages
        'x-message-ttl': 86400000,  # 24 hour TTL
        'x-dead-letter-exchange': 'dlx.dead-letter',
        'x-dead-letter-routing-key': 'dlq.deduplication'
    }
)

# Processing output queues
for source_type in ['parcel', 'retr', 'dfi']:
    channel.queue_declare(
        queue=f'processing.{source_type}',
        durable=True,
        arguments={
            'x-queue-type': 'quorum',
            'x-message-ttl': 86400000,
            'x-dead-letter-exchange': 'dlx.dead-letter',
            'x-dead-letter-routing-key': f'dlq.processing.{source_type}'
        }
    )
```

---

## Testing Requirements

### Unit Tests

```python
# tests/test_hash_functions.py
import pytest
from hash_functions import compute_parcel_hash

def test_parcel_hash_deterministic():
    """Same data should produce same hash."""
    data1 = {'STATEID': 'WI123456', 'ADDNUM': '123', 'STREETNAME': 'MAIN'}
    data2 = {'STATEID': 'WI123456', 'ADDNUM': '123', 'STREETNAME': 'MAIN'}
    
    assert compute_parcel_hash(data1) == compute_parcel_hash(data2)

def test_parcel_hash_case_insensitive():
    """Hash should be case-insensitive."""
    data1 = {'STATEID': 'wi123456', 'STREETNAME': 'main'}
    data2 = {'STATEID': 'WI123456', 'STREETNAME': 'MAIN'}
    
    assert compute_parcel_hash(data1) == compute_parcel_hash(data2)

def test_parcel_hash_whitespace_normalized():
    """Whitespace should be normalized."""
    data1 = {'STATEID': ' WI123456 ', 'STREETNAME': ' MAIN '}
    data2 = {'STATEID': 'WI123456', 'STREETNAME': 'MAIN'}
    
    assert compute_parcel_hash(data1) == compute_parcel_hash(data2)

def test_different_data_different_hash():
    """Different data should produce different hash."""
    data1 = {'STATEID': 'WI123456'}
    data2 = {'STATEID': 'WI999999'}
    
    assert compute_parcel_hash(data1) != compute_parcel_hash(data2)
```

### Integration Tests

```python
# tests/test_deduplication.py
import pytest
import asyncpg

@pytest.mark.asyncio
async def test_deduplication_skips_duplicate():
    """Duplicate records should be skipped."""
    
    # Insert first record
    message1 = {
        'batch_id': 'test-batch-1',
        'source_type': 'PARCEL',
        'source_file': 'test.gdb',
        'source_row_number': 1,
        'raw_data': {'STATEID': 'WI123456', 'ADDNUM': '123'}
    }
    
    result1 = await process_message(message1, db_pool, rabbitmq_channel)
    assert result1 is not None  # First record inserted
    
    # Try to insert duplicate
    message2 = {
        'batch_id': 'test-batch-2',
        'source_type': 'PARCEL',
        'source_file': 'test.gdb',
        'source_row_number': 2,
        'raw_data': {'STATEID': 'WI123456', 'ADDNUM': '123'}
    }
    
    result2 = await process_message(message2, db_pool, rabbitmq_channel)
    assert result2 is None  # Duplicate skipped
```

### Performance Tests

```python
# tests/test_performance.py
import pytest
import time

def test_hash_computation_performance():
    """Hash computation should be fast (<1ms per record)."""
    
    data = {
        'STATEID': 'WI123456',
        'ADDNUM': '123',
        'STREETNAME': 'MAIN',
        # ... full record
    }
    
    start = time.time()
    for _ in range(10000):
        compute_parcel_hash(data)
    elapsed = time.time() - start
    
    # Should process 10k hashes in < 1 second
    assert elapsed < 1.0
```

---

## Deployment

### Kubernetes Manifests

```yaml
# infrastructure/k8s/deployments/ingestion-api.yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: ingestion-api
  namespace: realestate
spec:
  replicas: 2
  selector:
    matchLabels:
      app: ingestion-api
  template:
    metadata:
      labels:
        app: ingestion-api
    spec:
      containers:
      - name: ingestion-api
        image: realestate/ingestion-api:latest
        ports:
        - containerPort: 8080
        env:
        - name: DATABASE_URL
          valueFrom:
            secretKeyRef:
              name: db-credentials
              key: connection-string
        - name: RABBITMQ_URL
          value: "amqp://rabbitmq:5672"
        resources:
          requests:
            memory: "4Gi"
            cpu: "2000m"
          limits:
            memory: "8Gi"
            cpu: "4000m"
        volumeMounts:
        - name: temp-storage
          mountPath: /tmp/gdb-processing
      volumes:
      - name: temp-storage
        emptyDir:
          sizeLimit: 20Gi
---
apiVersion: v1
kind: Service
metadata:
  name: ingestion-api
  namespace: realestate
spec:
  selector:
    app: ingestion-api
  ports:
  - port: 8080
    targetPort: 8080
  type: LoadBalancer
```

```yaml
# infrastructure/k8s/deployments/deduplication-service.yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: deduplication-service
  namespace: realestate
spec:
  replicas: 4
  selector:
    matchLabels:
      app: deduplication-service
  template:
    metadata:
      labels:
        app: deduplication-service
    spec:
      containers:
      - name: deduplication-service
        image: realestate/deduplication-service:latest
        env:
        - name: DATABASE_URL
          valueFrom:
            secretKeyRef:
              name: db-credentials
              key: connection-string
        - name: RABBITMQ_URL
          value: "amqp://rabbitmq:5672"
        resources:
          requests:
            memory: "1Gi"
            cpu: "500m"
          limits:
            memory: "2Gi"
            cpu: "1000m"
```

---

## Monitoring & Metrics

### Prometheus Metrics

```python
# Expose Prometheus metrics
from prometheus_client import Counter, Histogram, Gauge

# Ingestion API metrics
uploads_total = Counter('ingestion_uploads_total', 'Total file uploads', ['source_type', 'file_format'])
upload_errors = Counter('ingestion_upload_errors_total', 'Upload errors', ['error_type'])
upload_duration = Histogram('ingestion_upload_duration_seconds', 'Upload processing time')

# Deduplication metrics
records_processed = Counter('deduplication_records_processed_total', 'Total records processed', ['source_type'])
duplicates_found = Counter('deduplication_duplicates_found_total', 'Duplicates detected', ['source_type'])
hash_computation_duration = Histogram('deduplication_hash_duration_seconds', 'Hash computation time')
db_insert_duration = Histogram('deduplication_db_insert_duration_seconds', 'DB insert time')

# Queue metrics (from RabbitMQ)
queue_depth = Gauge('rabbitmq_queue_depth', 'Messages in queue', ['queue_name'])
```

### Health Checks

```python
# main.py (ingestion-api)
@app.get("/health")
async def health_check():
    """Health check endpoint for k8s liveness/readiness probes."""
    
    # Check database connection
    try:
        async with db_pool.acquire() as conn:
            await conn.fetchval("SELECT 1")
        db_healthy = True
    except Exception as e:
        db_healthy = False
    
    # Check RabbitMQ connection
    try:
        rabbitmq_channel.queue_declare(queue='health-check', passive=True)
        mq_healthy = True
    except Exception as e:
        mq_healthy = False
    
    if db_healthy and mq_healthy:
        return {"status": "healthy", "database": "ok", "rabbitmq": "ok"}
    else:
        raise HTTPException(503, {"status": "unhealthy"})
```

---

## Success Metrics

**Layer 1 is complete when**:

1. ✅ GDB files can be uploaded and processed
2. ✅ CSV files can be uploaded and processed
3. ✅ 100% deduplication accuracy (no duplicate hashes in raw_imports)
4. ✅ Throughput ≥ 5,000 records/second
5. ✅ < 1% message loss (RabbitMQ persistence + dead letter queues)
6. ✅ Health checks passing in k8s
7. ✅ Prometheus metrics exported
8. ✅ Integration tests passing (95%+ coverage)

---

## Next Steps

After Layer 1 is implemented:
1. Deploy to k8s cluster
2. Test with real Wisconsin parcel data (~430k records)
3. Validate deduplication works across multiple imports
4. Monitor performance metrics
5. Begin Layer 2 implementation (address normalization & matching)
