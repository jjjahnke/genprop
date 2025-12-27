# Ingestion API Service

FastAPI service for uploading and processing Wisconsin parcel data (GDB files) and real estate transfer returns (CSV files).

## Overview

The Ingestion API is the entry point (Layer 1) for all data into the Wisconsin Real Estate Database. It:
- Accepts GDB and CSV file uploads via REST API
- Extracts and validates data from geodatabases
- Streams records to RabbitMQ for deduplication
- Tracks import batch progress in PostgreSQL

## Features

- ✅ GDB file upload and extraction (File Geodatabase)
- ✅ CSV file upload and parsing (RETR, Parcel data)
- ✅ Async background processing
- ✅ Batch progress tracking
- ✅ File size validation (up to 5GB)
- ✅ CRS transformation (to EPSG:3071)
- ✅ Health checks for k8s deployment

## API Endpoints

### Upload GDB File

```bash
POST /api/v1/ingest/parcel/gdb
Content-Type: multipart/form-data

Parameters:
  - file: UploadFile (.gdb.zip or .gdb folder)
  - source_name: str (e.g., "Dane_County_2025")
  - layer_name: str (default: "V11_Parcels")

Response 202 Accepted:
{
  "batch_id": "uuid",
  "status": "processing",
  "total_features": 183425,
  "layer_info": {
    "layers": ["V11_Parcels", "V11_Addresses"],
    "crs": "EPSG:3071",
    "bounds": [32000.0, 222000.0, 168000.0, 398000.0]
  },
  "estimated_time_minutes": 5
}
```

### Upload CSV File

```bash
POST /api/v1/ingest/retr
POST /api/v1/ingest/parcel/csv
Content-Type: multipart/form-data

Parameters:
  - file: UploadFile (.csv)
  - source_name: str (e.g., "RETR_January_2020")

Response 202 Accepted:
{
  "batch_id": "uuid",
  "status": "processing",
  "total_rows": 50000,
  "estimated_time_minutes": 2
}
```

### Check Import Status

```bash
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
  "started_at": "2025-01-15T14:30:00Z",
  "estimated_completion": "2025-01-15T14:35:00Z"
}
```

### Health Check

```bash
GET /health

Response 200 OK:
{
  "status": "healthy",
  "database": "ok",
  "rabbitmq": "ok"
}
```

## Quick Start

### Prerequisites

- Python 3.12+
- Poetry
- Docker Compose (for local dependencies)
- GDAL installed locally (for development)

### Installation

1. **Install dependencies**:
   ```bash
   cd services/ingestion-api
   poetry install
   ```

2. **Start local services**:
   ```bash
   # From project root
   docker-compose up -d
   ```

3. **Run database migrations**:
   ```bash
   # From project root
   poetry run alembic upgrade head
   ```

4. **Start the API**:
   ```bash
   poetry run uvicorn main:app --reload --host 0.0.0.0 --port 8080
   ```

5. **Access the API**:
   - API: http://localhost:8080
   - Interactive docs: http://localhost:8080/docs
   - ReDoc: http://localhost:8080/redoc

## Configuration

Environment variables (see `.env.example`):

```bash
# Database
DATABASE_URL=postgresql://realestate:devpassword@localhost:5432/realestate

# RabbitMQ
RABBITMQ_URL=amqp://realestate:devpassword@localhost:5672/

# File Upload
MAX_UPLOAD_SIZE_MB=5000
TEMP_STORAGE_PATH=/tmp/gdb-processing

# Processing
BATCH_SIZE=1000
DEFAULT_LAYER_NAME=V11_Parcels

# API Server
API_HOST=0.0.0.0
API_PORT=8080
```

## Development

### Running Tests

```bash
# All tests
poetry run pytest

# With coverage
poetry run pytest --cov --cov-report=html

# Specific test
poetry run pytest tests/test_gdb_ingest.py -v
```

### Test Fixtures

Located in `tests/fixtures/`:
- `sample_retr.csv` - RETR test data (1000 rows)
- `sample_parcels.gdb.zip` - Synthetic parcel GDB (100 features)

### Linting

```bash
# Check code
poetry run ruff check .

# Auto-fix
poetry run ruff check . --fix
```

## Architecture

### Request Flow

```
1. Client uploads file (GDB/CSV)
   ↓
2. API validates file size and format
   ↓
3. File saved to temp storage
   ↓
4. Batch record created in import_batches table
   ↓
5. Background task started (FastAPI BackgroundTasks)
   ↓
6. File processed in chunks (1000 records/batch)
   ↓
7. Records published to RabbitMQ 'deduplication' queue
   ↓
8. Batch progress updated in database
   ↓
9. Temp files cleaned up
```

### Directory Structure

```
services/ingestion-api/
├── main.py                    # FastAPI app entry point
├── config.py                  # Pydantic Settings
├── routers/
│   ├── gdb_ingest.py         # GDB upload endpoint
│   ├── csv_ingest.py         # CSV upload endpoints
│   └── status.py             # Status check endpoint
├── services/
│   ├── gdb_processor.py      # GDB extraction and parsing
│   ├── csv_processor.py      # CSV parsing
│   └── batch_tracker.py      # Import batch management
├── models/
│   └── schemas.py            # Pydantic request/response models
└── tests/
    ├── test_gdb_ingest.py
    ├── test_csv_ingest.py
    └── fixtures/
```

## GDB Processing

The service handles Wisconsin V11 Statewide Parcel Database files:

1. **Upload**: Accept .gdb.zip or .gdb folder
2. **Extract**: Unzip if needed
3. **Inspect**: List layers, get CRS and bounds
4. **Validate**: Check layer exists
5. **Transform**: Convert to EPSG:3071 if needed
6. **Parse**: Extract all 42 V11 fields
7. **Publish**: Stream to RabbitMQ in batches

### Supported Formats

- File Geodatabase (.gdb folder in .zip)
- Layer: `V11_Parcels` (default)
- CRS: Auto-detected, transformed to EPSG:3071
- Max size: 5GB

## CSV Processing

Handles CSV files for:
- Wisconsin parcel data
- RETR (Real Estate Transfer Returns)

Processing includes:
- Encoding detection (UTF-8, latin-1)
- Streaming (pandas chunks of 1000 rows)
- Column validation
- Memory-efficient processing

## Error Handling

The API returns appropriate HTTP status codes:

- `200 OK` - Successful status check
- `202 Accepted` - Upload accepted, processing started
- `400 Bad Request` - Invalid file format or parameters
- `404 Not Found` - Batch ID not found
- `413 Payload Too Large` - File exceeds size limit
- `422 Unprocessable Entity` - Validation error
- `500 Internal Server Error` - Processing failure
- `503 Service Unavailable` - Database/RabbitMQ unavailable

Failed batches are marked in the database with error details.

## Performance

Target performance (per Layer 1 spec):
- **Throughput**: 5,000+ records/second
- **Upload Size**: Up to 5GB GDB files
- **Concurrency**: 2-3 simultaneous uploads
- **Response Time**: < 100ms for status checks

## Docker Deployment

### Build Image

```bash
docker build -t realestate/ingestion-api:latest .
```

### Run Container

```bash
docker run -d \
  --name ingestion-api \
  -p 8080:8080 \
  -e DATABASE_URL=postgresql://... \
  -e RABBITMQ_URL=amqp://... \
  realestate/ingestion-api:latest
```

## Kubernetes Deployment

See `infrastructure/k8s/deployments/ingestion-api.yaml` for k8s manifests.

```bash
kubectl apply -f infrastructure/k8s/deployments/ingestion-api.yaml
```

## Troubleshooting

### GDAL Issues

If you get GDAL import errors:

**macOS**:
```bash
brew install gdal
export GDAL_DATA=$(gdal-config --datadir)
```

**Ubuntu/Debian**:
```bash
sudo apt-get install gdal-bin libgdal-dev python3-gdal
```

### Memory Issues

For large GDB files, ensure sufficient memory:
- Docker: Increase memory limit to 8GB
- k8s: Resource limits set to 4Gi (per spec)

### RabbitMQ Connection

Check RabbitMQ is running:
```bash
docker-compose ps rabbitmq
curl http://localhost:15672/api/overview
```

## Contributing

See main project [CONTRIBUTING.md](../../CONTRIBUTING.md) for guidelines.

## License

See main project LICENSE file.
