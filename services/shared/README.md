# Shared Utilities Package

Common libraries and utilities shared across all Wisconsin Real Estate Database services.

## Overview

This package provides shared functionality for:
- **Data Models**: Pydantic models for V11 parcels, RETR records, DFI entities
- **Database**: asyncpg connection pool management
- **Message Queue**: RabbitMQ connection and publishing utilities
- **Hash Functions**: Content hashing for deduplication
- **Normalization**: Wisconsin address normalization utilities

## Installation

This package is designed to be installed as a local dependency:

```toml
# In your service's pyproject.toml
[tool.poetry.dependencies]
shared = {path = "../shared", develop = true}
```

## Usage

### Importing Models

```python
from shared.models import V11ParcelRecord, RETRRecord, DFIRecord

# Create a V11 parcel record
parcel = V11ParcelRecord(
    STATEID="WI123456",
    PARCELID="251-1234-567",
    ADDNUM="123",
    STREETNAME="MAIN",
    STREETTYPE="ST",
    geometry_wkt="MULTIPOLYGON(...)",
    geometry_type="MultiPolygon"
)
```

### Database Connection

```python
from shared.database import get_db_pool

# Get connection pool
pool = await get_db_pool()

# Execute query
async with pool.acquire() as conn:
    result = await conn.fetchrow("SELECT * FROM import_batches LIMIT 1")
```

### RabbitMQ Publishing

```python
from shared.rabbitmq import get_rabbitmq_connection

# Get RabbitMQ connection
rabbitmq = await get_rabbitmq_connection()

# Publish message
await rabbitmq.publish('deduplication', {
    'batch_id': 'uuid',
    'source_type': 'PARCEL',
    'raw_data': {...}
})
```

## Modules

### `shared.models`

Pydantic models for data validation:
- `V11ParcelRecord` - Wisconsin V11 Statewide Parcel Database (42 fields)
- `RETRRecord` - Real Estate Transfer Return
- `DFIRecord` - Department of Financial Institutions corporate entity

### `shared.database`

Database utilities:
- `get_db_pool()` - Get/create asyncpg connection pool
- Connection pool configuration and lifecycle management

### `shared.rabbitmq`

Message queue utilities:
- `get_rabbitmq_connection()` - Get/create RabbitMQ connection
- `publish()` - Publish message to queue with retry logic
- Queue declarations for Layer 1 queues

### `shared.hash_utils`

Content hashing utilities (placeholder for deduplication-service):
- Hash computation functions
- Normalization helpers

### `shared.wisconsin_normalizer`

Wisconsin-specific address normalization:
- Fire number patterns (N1234 STATE RD 67)
- Rural route handling
- 8-component address parsing

## Development

### Running Tests

```bash
cd services/shared
poetry run pytest
```

### Type Checking

```bash
poetry run mypy shared/
```

## Dependencies

- `pydantic` ^2.5.3 - Data validation
- `pydantic-settings` ^2.1.0 - Settings management
- `asyncpg` ^0.29.0 - Async PostgreSQL client
- `pika` ^1.3.2 - RabbitMQ client
- `python-dateutil` ^2.8.2 - Date utilities
