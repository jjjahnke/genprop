# Raw Storage Layer Implementation Guide
## Record-by-Record Deduplication and Source Preservation

This guide provides the complete implementation for the Raw Storage Layer that acts as the deduplication gate at your pipeline entrance. Each record is hashed on arrival, checked for duplicates, and stored with full audit trail.

---

## Architecture Overview

```
┌─────────────────────────────────────────────────────────────────────────┐
│                         DATA INGESTION FLOW                              │
│                                                                          │
│  Source File (CSV/GDB)                                                   │
│         │                                                                │
│         ▼                                                                │
│  ┌─────────────────────────────────────────────────────────────────┐   │
│  │ 1. PARSE RECORD                                                  │   │
│  │    Extract fields from source format                             │   │
│  └────────────────────────────────┬─────────────────────────────────┘   │
│                                   │                                     │
│                                   ▼                                     │
│  ┌─────────────────────────────────────────────────────────────────┐   │
│  │ 2. COMPUTE HASH                                                  │   │
│  │    Deterministic hash from canonical field values                │   │
│  └────────────────────────────────┬─────────────────────────────────┘   │
│                                   │                                     │
│                                   ▼                                     │
│  ┌─────────────────────────────────────────────────────────────────┐   │
│  │ 3. DEDUPLICATION CHECK                                           │   │
│  │    Query: SELECT 1 FROM raw_imports WHERE content_hash = ?       │   │
│  └────────────────────────────────┬─────────────────────────────────┘   │
│                                   │                                     │
│         ┌─────────────────────────┴──────────────────────┐              │
│         │                                                │              │
│    Hash Exists                                    Hash New             │
│         │                                                │              │
│         ▼                                                ▼              │
│  ┌─────────────────┐                        ┌──────────────────────┐   │
│  │ SKIP            │                        │ INSERT TO            │   │
│  │ Log duplicate   │                        │ raw_imports          │   │
│  │ Return NULL     │                        │ Return record_id     │   │
│  └─────────────────┘                        └──────────┬───────────┘   │
│                                                        │               │
│                                                        ▼               │
│                                             ┌──────────────────────┐   │
│                                             │ DOWNSTREAM PROCESSING│   │
│                                             │ (matching, linking)  │   │
│                                             └──────────────────────┘   │
└─────────────────────────────────────────────────────────────────────────┘
```

---

## Database Schema

### Core Table: raw_imports

This table stores every unique record ever imported with its original data preserved.

```sql
-- Enable required extensions
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- Main raw imports table
CREATE TABLE raw_imports (
    -- Primary key
    record_id           UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    
    -- Deduplication key
    content_hash        VARCHAR(64) NOT NULL,  -- SHA-256 hex digest
    
    -- Source tracking
    import_batch_id     UUID NOT NULL,         -- Groups records from same import operation
    source_type         VARCHAR(20) NOT NULL,  -- 'PARCEL', 'RETR', 'DFI'
    source_file         TEXT NOT NULL,         -- Original filename
    source_row_number   INTEGER,               -- Row number in source file
    imported_at         TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    
    -- Original record data (preserved for audit/reprocessing)
    raw_data            JSONB NOT NULL,        -- Complete original record as JSON
    
    -- Processing metadata
    processing_status   VARCHAR(20) NOT NULL DEFAULT 'pending',  
                        -- Values: 'pending', 'processed', 'failed', 'skipped'
    processed_at        TIMESTAMPTZ,
    error_message       TEXT,
    
    -- Matching results (populated after matching stage)
    matched_parcel_id   UUID,                  -- Links to parcels table after matching
    match_confidence    NUMERIC(5,4),          -- 0.0000 to 1.0000
    match_method        VARCHAR(50),           -- 'exact_id', 'probabilistic', 'manual'
    
    -- Audit fields
    created_by          VARCHAR(100) DEFAULT CURRENT_USER,
    updated_at          TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
);

-- Indexes for performance
CREATE UNIQUE INDEX idx_raw_imports_hash ON raw_imports(content_hash);
CREATE INDEX idx_raw_imports_batch ON raw_imports(import_batch_id);
CREATE INDEX idx_raw_imports_source ON raw_imports(source_type, imported_at DESC);
CREATE INDEX idx_raw_imports_status ON raw_imports(processing_status) 
    WHERE processing_status IN ('pending', 'failed');
CREATE INDEX idx_raw_imports_parcel ON raw_imports(matched_parcel_id) 
    WHERE matched_parcel_id IS NOT NULL;

-- Partitioning by import month (optional but recommended for large scale)
CREATE TABLE raw_imports (
    -- ... same columns as above ...
    PARTITION BY RANGE (imported_at)
);

-- Create partitions
CREATE TABLE raw_imports_2025_01 PARTITION OF raw_imports
    FOR VALUES FROM ('2025-01-01') TO ('2025-02-01');
CREATE TABLE raw_imports_2025_02 PARTITION OF raw_imports
    FOR VALUES FROM ('2025-02-01') TO ('2025-03-01');
-- ... create as needed
```

### Import Batch Tracking

```sql
-- Track each import operation
CREATE TABLE import_batches (
    batch_id            UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    source_type         VARCHAR(20) NOT NULL,
    source_file         TEXT NOT NULL,
    file_size_bytes     BIGINT,
    file_hash           VARCHAR(64),  -- SHA-256 of entire source file
    started_at          TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    completed_at        TIMESTAMPTZ,
    status              VARCHAR(20) DEFAULT 'running',  -- 'running', 'completed', 'failed'
    
    -- Statistics
    total_records       INTEGER,
    new_records         INTEGER,
    duplicate_records   INTEGER,
    failed_records      INTEGER,
    
    -- Metadata
    imported_by         VARCHAR(100) DEFAULT CURRENT_USER,
    notes               TEXT
);

CREATE INDEX idx_batches_date ON import_batches(started_at DESC);
CREATE INDEX idx_batches_file ON import_batches(file_hash);
```

### Deduplication Statistics

```sql
-- Track duplicate patterns for monitoring
CREATE TABLE deduplication_stats (
    stat_id             SERIAL PRIMARY KEY,
    source_type         VARCHAR(20),
    duplicate_count     INTEGER,
    first_seen          TIMESTAMPTZ,
    last_seen           TIMESTAMPTZ,
    content_hash        VARCHAR(64),
    sample_record_id    UUID REFERENCES raw_imports(record_id)
);

CREATE INDEX idx_dedup_stats_hash ON deduplication_stats(content_hash);
```

---

## Hash Computation Logic

The hash must be **deterministic** and **canonical** - same logical record always produces same hash regardless of format variations.

### Hash Function Design Principles

1. **Normalize before hashing**: Uppercase text, trim whitespace, standardize formats
2. **Include only semantic fields**: Exclude timestamps, import IDs, metadata
3. **Stable field ordering**: Always hash fields in same order
4. **Handle nulls consistently**: Use empty string or special marker
5. **Version the hash algorithm**: Include version prefix for future changes

### Wisconsin Parcel Hash (V11 Schema)

```python
import hashlib
import json
from typing import Dict, Any, Optional

def compute_parcel_hash(record: Dict[str, Any], version: str = 'v1') -> str:
    """
    Compute deterministic hash for Wisconsin V11 parcel record.
    
    Hash includes semantic identity fields:
    - STATEID (primary unique identifier)
    - Address components (for records without STATEID)
    - County and municipality
    - Ownership (to detect ownership changes as new records)
    - Assessment date (to detect annual reassessments)
    
    Args:
        record: Parsed parcel record as dict
        version: Hash algorithm version (for future changes)
    
    Returns:
        64-character hex string (SHA-256)
    """
    
    # Canonical field extraction with normalization
    canonical = {
        # Primary identifier
        'stateid': (record.get('STATEID') or '').strip().upper(),
        
        # Address components (normalized)
        'addnumprefix': (record.get('ADDNUMPREFIX') or '').strip().upper(),
        'addnum': str(record.get('ADDNUM') or '').strip(),
        'addnumsuffix': (record.get('ADDNUMSUFFIX') or '').strip().upper(),
        'prefix': (record.get('PREFIX') or '').strip().upper(),
        'streetname': (record.get('STREETNAME') or '').strip().upper(),
        'streettype': (record.get('STREETTYPE') or '').strip().upper(),
        'suffix': (record.get('SUFFIX') or '').strip().upper(),
        'unittype': (record.get('UNITTYPE') or '').strip().upper(),
        'unitid': (record.get('UNITID') or '').strip().upper(),
        
        # Location
        'placename': (record.get('PLACENAME') or '').strip().upper(),
        'zipcode': (record.get('ZIPCODE') or '').strip(),
        'coname': (record.get('CONAME') or '').strip().upper(),
        
        # Ownership (changes = new record in bitemporal system)
        'ownernme1': (record.get('OWNERNME1') or '').strip().upper(),
        'ownernme2': (record.get('OWNERNME2') or '').strip().upper(),
        'pstladress': (record.get('PSTLADRESS') or '').strip().upper(),
        
        # Assessment period (to distinguish annual updates)
        'assessyear': str(record.get('ASSESSYEAR') or '').strip(),
        
        # Key value (changes = new record)
        'cntassdvalue': str(record.get('CNTASSDVALUE') or '').strip(),
        
        # Property classification
        'propclass': (record.get('PROPCLASS') or '').strip().upper(),
    }
    
    # Create stable JSON representation (sorted keys)
    canonical_json = json.dumps(canonical, sort_keys=True, separators=(',', ':'))
    
    # Prepend version for future algorithm changes
    versioned = f"{version}:{canonical_json}"
    
    # Compute SHA-256
    return hashlib.sha256(versioned.encode('utf-8')).hexdigest()


def compute_parcel_hash_fast(record: Dict[str, Any]) -> str:
    """
    Optimized version using direct string concatenation.
    ~3x faster than JSON serialization approach.
    """
    # Normalize and concatenate with delimiters
    parts = [
        (record.get('STATEID') or '').strip().upper(),
        (record.get('ADDNUM') or '').strip(),
        (record.get('STREETNAME') or '').strip().upper(),
        (record.get('PLACENAME') or '').strip().upper(),
        (record.get('ZIPCODE') or '').strip(),
        (record.get('CONAME') or '').strip().upper(),
        (record.get('OWNERNME1') or '').strip().upper(),
        (record.get('ASSESSYEAR') or '').strip(),
        (record.get('CNTASSDVALUE') or '').strip(),
    ]
    
    canonical_str = '|'.join(parts)
    return hashlib.sha256(f"v1:{canonical_str}".encode('utf-8')).hexdigest()
```

### RETR Event Hash

```python
def compute_retr_hash(record: Dict[str, Any]) -> str:
    """
    Compute hash for Real Estate Transfer Return event.
    
    RETR events are immutable once recorded, so hash includes:
    - Parcel identifier
    - Transfer date
    - Conveyance document number (unique per transaction)
    - Buyer/seller information
    """
    
    canonical = {
        # Parcel identification (normalize parcel ID)
        'parcel_id': normalize_parcel_id(record.get('PARCEL_ID', '')),
        
        # Transfer details
        'transfer_date': str(record.get('TRANSFER_DATE', '')).strip(),
        'doc_number': (record.get('DOC_NUMBER') or '').strip().upper(),
        'doc_type': (record.get('DOC_TYPE') or '').strip().upper(),
        
        # Parties
        'grantor_name': (record.get('GRANTOR_NAME') or '').strip().upper(),
        'grantee_name': (record.get('GRANTEE_NAME') or '').strip().upper(),
        
        # Financial
        'sale_amount': str(record.get('SALE_AMOUNT') or '').strip(),
        
        # Property details
        'property_class': (record.get('PROPERTY_CLASS') or '').strip().upper(),
    }
    
    canonical_json = json.dumps(canonical, sort_keys=True, separators=(',', ':'))
    return hashlib.sha256(f"v1:{canonical_json}".encode('utf-8')).hexdigest()


def normalize_parcel_id(parcel_id: str) -> str:
    """
    Normalize parcel ID for consistent hashing.
    Removes dashes, spaces, dots, converts to uppercase.
    """
    return parcel_id.replace('-', '').replace(' ', '').replace('.', '').upper()
```

### DFI Corporate Entity Hash

```python
def compute_dfi_hash(record: Dict[str, Any]) -> str:
    """
    Compute hash for Wisconsin DFI corporate entity record.
    
    Entities can change (name, registered agent, status), so hash includes
    state on effective date.
    """
    
    canonical = {
        # Unique identifier
        'entity_id': (record.get('ENTITY_ID') or '').strip().upper(),
        
        # Entity information (as of record date)
        'entity_name': (record.get('ENTITY_NAME') or '').strip().upper(),
        'entity_type': (record.get('ENTITY_TYPE') or '').strip().upper(),
        'status': (record.get('STATUS') or '').strip().upper(),
        
        # Registered agent
        'agent_name': (record.get('AGENT_NAME') or '').strip().upper(),
        'agent_address': (record.get('AGENT_ADDRESS') or '').strip().upper(),
        
        # Record state date
        'effective_date': str(record.get('EFFECTIVE_DATE', '')).strip(),
    }
    
    canonical_json = json.dumps(canonical, sort_keys=True, separators=(',', ':'))
    return hashlib.sha256(f"v1:{canonical_json}".encode('utf-8')).hexdigest()
```

---

## Deduplication Check Workflow

### Python Implementation

```python
import psycopg2
from psycopg2.extras import RealDictCursor
from typing import Optional, Dict, Any
import uuid
from datetime import datetime

class RawStorageLayer:
    """
    Manages record-by-record deduplication and storage.
    """
    
    def __init__(self, connection_string: str):
        self.conn = psycopg2.connect(connection_string)
    
    def check_duplicate(self, content_hash: str) -> Optional[uuid.UUID]:
        """
        Check if record with this hash already exists.
        
        Args:
            content_hash: SHA-256 hash of canonical record
        
        Returns:
            record_id if duplicate exists, None if new record
        """
        with self.conn.cursor() as cur:
            cur.execute(
                "SELECT record_id FROM raw_imports WHERE content_hash = %s",
                (content_hash,)
            )
            result = cur.fetchone()
            return result[0] if result else None
    
    def insert_record(
        self,
        content_hash: str,
        raw_data: Dict[str, Any],
        batch_id: uuid.UUID,
        source_type: str,
        source_file: str,
        source_row_number: Optional[int] = None
    ) -> uuid.UUID:
        """
        Insert new record into raw storage.
        
        Args:
            content_hash: SHA-256 hash
            raw_data: Complete original record
            batch_id: Import batch identifier
            source_type: 'PARCEL', 'RETR', or 'DFI'
            source_file: Original filename
            source_row_number: Row in source file
        
        Returns:
            record_id of inserted record
        """
        record_id = uuid.uuid4()
        
        with self.conn.cursor() as cur:
            cur.execute("""
                INSERT INTO raw_imports (
                    record_id, content_hash, import_batch_id,
                    source_type, source_file, source_row_number,
                    raw_data, processing_status
                ) VALUES (
                    %s, %s, %s, %s, %s, %s, %s, 'pending'
                )
                RETURNING record_id
            """, (
                record_id, content_hash, batch_id,
                source_type, source_file, source_row_number,
                psycopg2.extras.Json(raw_data)
            ))
            
            self.conn.commit()
            return record_id
    
    def ingest_record(
        self,
        raw_data: Dict[str, Any],
        batch_id: uuid.UUID,
        source_type: str,
        source_file: str,
        source_row_number: Optional[int] = None,
        hash_func = None
    ) -> Optional[uuid.UUID]:
        """
        Complete ingestion workflow: hash -> check -> insert.
        
        Returns:
            record_id if new record inserted, None if duplicate skipped
        """
        # Compute hash based on source type
        if hash_func:
            content_hash = hash_func(raw_data)
        else:
            content_hash = self._default_hash(raw_data, source_type)
        
        # Check for duplicate
        existing_id = self.check_duplicate(content_hash)
        if existing_id:
            # Log duplicate for statistics
            self._log_duplicate(content_hash, existing_id, batch_id)
            return None
        
        # Insert new record
        return self.insert_record(
            content_hash, raw_data, batch_id,
            source_type, source_file, source_row_number
        )
    
    def _default_hash(self, raw_data: Dict[str, Any], source_type: str) -> str:
        """Route to appropriate hash function based on source type."""
        if source_type == 'PARCEL':
            return compute_parcel_hash(raw_data)
        elif source_type == 'RETR':
            return compute_retr_hash(raw_data)
        elif source_type == 'DFI':
            return compute_dfi_hash(raw_data)
        else:
            raise ValueError(f"Unknown source type: {source_type}")
    
    def _log_duplicate(self, content_hash: str, existing_id: uuid.UUID, batch_id: uuid.UUID):
        """Record duplicate encounter for monitoring."""
        with self.conn.cursor() as cur:
            cur.execute("""
                INSERT INTO deduplication_stats (
                    content_hash, duplicate_count, first_seen, last_seen, sample_record_id
                ) VALUES (
                    %s, 1, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, %s
                )
                ON CONFLICT (content_hash) DO UPDATE SET
                    duplicate_count = deduplication_stats.duplicate_count + 1,
                    last_seen = CURRENT_TIMESTAMP
            """, (content_hash, existing_id))
            self.conn.commit()
```

### Example Usage

```python
# Initialize storage layer
storage = RawStorageLayer("postgresql://user:pass@localhost/realestate")

# Create import batch
batch_id = uuid.uuid4()
with storage.conn.cursor() as cur:
    cur.execute("""
        INSERT INTO import_batches (batch_id, source_type, source_file)
        VALUES (%s, %s, %s)
    """, (batch_id, 'PARCEL', 'wisconsin_parcels_2025.csv'))
    storage.conn.commit()

# Process CSV file
import csv

new_count = 0
dup_count = 0

with open('wisconsin_parcels_2025.csv', 'r') as f:
    reader = csv.DictReader(f)
    for row_num, row in enumerate(reader, start=1):
        record_id = storage.ingest_record(
            raw_data=row,
            batch_id=batch_id,
            source_type='PARCEL',
            source_file='wisconsin_parcels_2025.csv',
            source_row_number=row_num,
            hash_func=compute_parcel_hash
        )
        
        if record_id:
            new_count += 1
        else:
            dup_count += 1
        
        if (row_num % 1000) == 0:
            print(f"Processed {row_num} rows: {new_count} new, {dup_count} duplicates")

# Update batch statistics
with storage.conn.cursor() as cur:
    cur.execute("""
        UPDATE import_batches SET
            completed_at = CURRENT_TIMESTAMP,
            status = 'completed',
            total_records = %s,
            new_records = %s,
            duplicate_records = %s
        WHERE batch_id = %s
    """, (new_count + dup_count, new_count, dup_count, batch_id))
    storage.conn.commit()

print(f"Import complete: {new_count} new records, {dup_count} duplicates skipped")
```

---

## Foreign Key Relationships

Other tables in your system reference back to `raw_imports.record_id` to maintain full audit trail.

### Parcels Table with Raw Import Reference

```sql
CREATE TABLE parcels (
    parcel_id           UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    
    -- Link to original source record
    source_record_id    UUID NOT NULL REFERENCES raw_imports(record_id),
    
    -- Wisconsin V11 fields
    stateid             TEXT NOT NULL,
    parcelid            TEXT,
    taxparcelid         TEXT,
    ownernme1           TEXT,
    cntassdvalue        NUMERIC(14,2),
    -- ... other fields ...
    
    -- Bitemporal dimensions
    valid_from          DATE NOT NULL,
    valid_to            DATE NOT NULL DEFAULT '9999-12-31',
    tx_start            TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    tx_end              TIMESTAMPTZ NOT NULL DEFAULT 'infinity'
);

CREATE INDEX idx_parcels_source ON parcels(source_record_id);
```

### RETR Events Table

```sql
CREATE TABLE retr_events (
    event_id            UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    
    -- Link to original source record
    source_record_id    UUID NOT NULL REFERENCES raw_imports(record_id),
    
    -- Link to matched parcel (populated after matching)
    matched_parcel_id   UUID REFERENCES parcels(parcel_id),
    match_confidence    NUMERIC(5,4),
    
    -- RETR fields
    transfer_date       DATE NOT NULL,
    doc_number          TEXT,
    grantor_name        TEXT,
    grantee_name        TEXT,
    sale_amount         NUMERIC(14,2),
    -- ... other fields ...
    
    -- Event time
    event_date          DATE NOT NULL,
    recorded_at         TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_retr_source ON retr_events(source_record_id);
CREATE INDEX idx_retr_parcel ON retr_events(matched_parcel_id);
```

### Audit Trail Queries

With these foreign keys, you can trace any derived record back to its original source:

```sql
-- Find original source for a parcel
SELECT ri.raw_data, ri.source_file, ri.imported_at
FROM parcels p
JOIN raw_imports ri ON p.source_record_id = ri.record_id
WHERE p.parcel_id = 'abc123...';

-- Find all parcels derived from a specific import batch
SELECT p.*
FROM parcels p
JOIN raw_imports ri ON p.source_record_id = ri.record_id
WHERE ri.import_batch_id = 'batch-uuid...';

-- Track reprocessing: find all records from same source
SELECT p.parcel_id, p.stateid, p.tx_start
FROM parcels p
WHERE p.source_record_id IN (
    SELECT record_id FROM raw_imports
    WHERE content_hash = 'abc123...'
)
ORDER BY p.tx_start DESC;
```

---

## Performance Optimization

### Batch Insertion

For large imports (430k+ records), use batch insertion:

```python
def batch_ingest(records: list[Dict[str, Any]], batch_id: uuid.UUID, 
                 source_type: str, source_file: str, 
                 batch_size: int = 1000) -> tuple[int, int]:
    """
    Ingest records in batches for better performance.
    
    Returns:
        (new_count, duplicate_count)
    """
    new_count = 0
    dup_count = 0
    
    # Compute all hashes first
    hashed_records = [
        (compute_parcel_hash(r), r) for r in records
    ]
    
    # Extract hashes for bulk duplicate check
    hashes = [h for h, _ in hashed_records]
    
    # Bulk duplicate check
    with storage.conn.cursor() as cur:
        cur.execute("""
            SELECT content_hash FROM raw_imports
            WHERE content_hash = ANY(%s)
        """, (hashes,))
        existing_hashes = {row[0] for row in cur.fetchall()}
    
    # Filter to new records only
    new_records = [
        (h, r, i) for i, (h, r) in enumerate(hashed_records, start=1)
        if h not in existing_hashes
    ]
    dup_count = len(hashed_records) - len(new_records)
    
    # Batch insert new records
    for i in range(0, len(new_records), batch_size):
        batch = new_records[i:i + batch_size]
        
        values = [
            (
                uuid.uuid4(),  # record_id
                hash_val,      # content_hash
                batch_id,      # import_batch_id
                source_type,
                source_file,
                row_num,
                psycopg2.extras.Json(rec_data)
            )
            for hash_val, rec_data, row_num in batch
        ]
        
        with storage.conn.cursor() as cur:
            psycopg2.extras.execute_values(
                cur,
                """
                INSERT INTO raw_imports (
                    record_id, content_hash, import_batch_id,
                    source_type, source_file, source_row_number,
                    raw_data, processing_status
                ) VALUES %s
                """,
                values,
                template="(%s, %s, %s, %s, %s, %s, %s, 'pending')"
            )
            storage.conn.commit()
        
        new_count += len(batch)
    
    return new_count, dup_count
```

### Index Performance

Monitor index usage and size:

```sql
-- Check index size
SELECT
    schemaname,
    tablename,
    indexname,
    pg_size_pretty(pg_relation_size(indexrelid)) AS index_size
FROM pg_stat_user_indexes
WHERE schemaname = 'public' AND tablename = 'raw_imports'
ORDER BY pg_relation_size(indexrelid) DESC;

-- Check index usage
SELECT
    indexrelname,
    idx_scan,
    idx_tup_read,
    idx_tup_fetch
FROM pg_stat_user_indexes
WHERE schemaname = 'public' AND tablename = 'raw_imports';
```

---

## Monitoring and Maintenance

### Key Metrics Queries

```sql
-- Daily import summary
SELECT
    DATE(imported_at) as import_date,
    source_type,
    COUNT(*) as records_imported,
    COUNT(DISTINCT import_batch_id) as batch_count
FROM raw_imports
WHERE imported_at >= CURRENT_DATE - INTERVAL '30 days'
GROUP BY DATE(imported_at), source_type
ORDER BY import_date DESC, source_type;

-- Duplicate rate by source
SELECT
    source_type,
    COUNT(*) as total_encounters,
    COUNT(DISTINCT content_hash) as unique_records,
    ROUND(100.0 * (COUNT(*) - COUNT(DISTINCT content_hash)) / COUNT(*), 2) as dup_rate_pct
FROM raw_imports
GROUP BY source_type;

-- Processing status
SELECT
    processing_status,
    source_type,
    COUNT(*) as count,
    MIN(imported_at) as oldest,
    MAX(imported_at) as newest
FROM raw_imports
GROUP BY processing_status, source_type;

-- Largest batches
SELECT
    ib.batch_id,
    ib.source_file,
    ib.started_at,
    ib.total_records,
    ib.new_records,
    ib.duplicate_records,
    ROUND(100.0 * ib.duplicate_records / NULLIF(ib.total_records, 0), 2) as dup_pct
FROM import_batches ib
ORDER BY ib.total_records DESC
LIMIT 20;
```

### Cleanup Policy

Old records can be archived after downstream processing completes:

```sql
-- Archive processed records older than 2 years
CREATE TABLE raw_imports_archive (LIKE raw_imports INCLUDING ALL);

INSERT INTO raw_imports_archive
SELECT * FROM raw_imports
WHERE processing_status = 'processed'
  AND processed_at < CURRENT_DATE - INTERVAL '2 years';

DELETE FROM raw_imports
WHERE processing_status = 'processed'
  AND processed_at < CURRENT_DATE - INTERVAL '2 years';
```

---

## RabbitMQ Integration

Connect raw storage to downstream processing pipeline:

```python
import pika
import json

def publish_to_processing_queue(record_id: uuid.UUID, source_type: str):
    """
    Publish new record to RabbitMQ for downstream processing.
    """
    connection = pika.BlockingConnection(
        pika.ConnectionParameters('localhost')
    )
    channel = connection.channel()
    
    # Declare processing queue
    channel.queue_declare(queue='record_processing', durable=True)
    
    message = {
        'record_id': str(record_id),
        'source_type': source_type,
        'timestamp': datetime.utcnow().isoformat()
    }
    
    channel.basic_publish(
        exchange='',
        routing_key='record_processing',
        body=json.dumps(message),
        properties=pika.BasicProperties(
            delivery_mode=2,  # Make message persistent
        )
    )
    
    connection.close()


# Modified ingest function with queue publishing
def ingest_and_queue(
    raw_data: Dict[str, Any],
    batch_id: uuid.UUID,
    source_type: str,
    source_file: str
) -> Optional[uuid.UUID]:
    """Ingest record and publish to processing queue if new."""
    
    record_id = storage.ingest_record(
        raw_data, batch_id, source_type, source_file
    )
    
    if record_id:
        # New record - send to processing pipeline
        publish_to_processing_queue(record_id, source_type)
    
    return record_id
```

---

## Summary

This Raw Storage Layer implementation provides:

✅ **Deduplication gate** - Hash computed on entry, duplicates rejected immediately  
✅ **Source preservation** - Original records stored in JSONB for audit/reprocessing  
✅ **Foreign key references** - Downstream tables link back via `source_record_id`  
✅ **Batch tracking** - Import operations grouped with statistics  
✅ **Performance optimized** - Unique index on hash, batch insertion support  
✅ **Monitoring ready** - Metrics queries for duplicate rates, processing status  

The next phase will build the matching pipeline that reads from `raw_imports` where `processing_status = 'pending'`, performs address/parcel ID matching, and updates the `matched_parcel_id` and `match_confidence` fields.
