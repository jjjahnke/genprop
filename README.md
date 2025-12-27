# Wisconsin Real Estate Database

Bitemporal real estate database for Wisconsin parcels with semantic search, ownership tracking, and natural language query interface.

## Overview

This system processes Wisconsin parcel data (GDB files) and real estate transfer returns (CSV) through a 5-layer architecture:

1. **Layer 1 (Ingestion)**: Deduplicate and store raw data
2. **Layer 2 (Matching)**: Match and normalize records (GPU-accelerated)
3. **Layer 3 (Database)**: TimescaleDB bitemporal storage (source of truth)
4. **Layer 4 (Intelligence)**: Vectorization (Qdrant) and analytics
5. **Layer 5 (API/Agent)**: Query API and conversational interface

## Technology Stack

- **Languages**: Python 3.12+, SQL
- **Frameworks**: FastAPI, Pydantic v2
- **Databases**: TimescaleDB (PostgreSQL + PostGIS), Qdrant (vectors)
- **Message Queue**: RabbitMQ
- **Testing**: Pytest, pytest-asyncio
- **Linting**: Ruff
- **Dependency Management**: Poetry
- **Container**: Docker + Kubernetes
- **GPU**: RAPIDS (Layer 2 matching), sentence-transformers (Layer 4 vectorization)

## Project Structure

```
wisconsin-realestate/
├── services/                           # Microservices (Layers 1-5)
│   ├── shared/                         # Common libraries (models, DB, queues)
│   ├── ingestion-api/                  # Layer 1: GDB/CSV upload endpoint
│   ├── deduplication-service/          # Layer 1: Hash & dedupe
│   └── ... (Layer 2-5 services)
│
├── infrastructure/                     # Deployment configs
│   ├── k8s/                           # Kubernetes manifests
│   └── terraform/                     # Infrastructure as code
│
├── database/                          # Layer 3 schemas & migrations
│   ├── migrations/                    # Alembic migrations
│   └── schemas/                       # SQL schema definitions
│
├── docs/                              # Documentation
├── tests/                             # Integration & E2E tests
└── docker-compose.yml                 # Local development stack
```

## Quick Start

### Prerequisites

- Python 3.12+
- Poetry 1.7+
- Docker & Docker Compose
- GDAL (for GDB processing)

### Installation

1. **Clone the repository**:
   ```bash
   git clone <repository-url>
   cd wisconsin-realestate
   ```

2. **Install Poetry** (if not already installed):
   ```bash
   curl -sSL https://install.python-poetry.org | python3 -
   ```

3. **Install dependencies**:
   ```bash
   poetry install
   ```

4. **Copy environment configuration**:
   ```bash
   cp .env.example .env
   # Edit .env with your local configuration if needed
   ```

5. **Start local development stack**:
   ```bash
   docker-compose up -d
   ```

   This starts:
   - TimescaleDB on `localhost:5432`
   - RabbitMQ on `localhost:5672` (Management UI: `http://localhost:15672`)
   - Redis on `localhost:6379`
   - Qdrant on `localhost:6333`

6. **Run database migrations**:
   ```bash
   poetry run alembic upgrade head
   ```

7. **Verify setup**:
   ```bash
   # Check database
   psql postgresql://realestate:devpassword@localhost:5432/realestate -c "SELECT * FROM import_batches LIMIT 1;"

   # Check RabbitMQ (username: realestate, password: devpassword)
   open http://localhost:15672
   ```

## Development

### Running the Ingestion API

```bash
cd services/ingestion-api
poetry run uvicorn main:app --reload --host 0.0.0.0 --port 8080
```

API will be available at: `http://localhost:8080`

Interactive docs: `http://localhost:8080/docs`

### Running Tests

```bash
# All tests
poetry run pytest

# With coverage
poetry run pytest --cov --cov-report=html

# Specific service
cd services/ingestion-api
poetry run pytest

# Specific test file
poetry run pytest tests/test_gdb_ingest.py
```

### Linting and Formatting

```bash
# Check code quality
poetry run ruff check .

# Auto-fix issues
poetry run ruff check . --fix

# Type checking
poetry run mypy services/
```

### Database Migrations

```bash
# Create a new migration
poetry run alembic revision -m "description of changes"

# Apply migrations
poetry run alembic upgrade head

# Rollback one migration
poetry run alembic downgrade -1

# View migration history
poetry run alembic history
```

## API Endpoints

### Ingestion API (Layer 1)

- `POST /api/v1/ingest/parcel/gdb` - Upload File Geodatabase
- `POST /api/v1/ingest/parcel/csv` - Upload parcel CSV
- `POST /api/v1/ingest/retr` - Upload RETR CSV
- `GET /api/v1/ingest/status/{batch_id}` - Check import status
- `GET /health` - Health check

### Example: Upload RETR CSV

```bash
curl -X POST http://localhost:8080/api/v1/ingest/retr \
  -F "file=@data/202001CSV.csv" \
  -F "source_name=RETR_January_2020"
```

Response:
```json
{
  "batch_id": "550e8400-e29b-41d4-a716-446655440000",
  "status": "processing",
  "total_rows": 50000,
  "estimated_time_minutes": 2
}
```

### Check Status

```bash
curl http://localhost:8080/api/v1/ingest/status/550e8400-e29b-41d4-a716-446655440000
```

## Data Formats

### Wisconsin V11 Parcel Schema

File Geodatabase (.gdb or .gdb.zip) with layer `V11_Parcels`:
- **Coordinate System**: EPSG:3071 (Wisconsin Transverse Mercator)
- **Geometry Type**: Polygon or MultiPolygon
- **Attributes**: 42 fields including STATEID, PARCELID, addresses, ownership, assessment values

### RETR (Real Estate Transfer Returns)

CSV files from Wisconsin DOR with monthly real estate transfer data.

## Environment Variables

See `.env.example` for all configuration options. Key variables:

- `DATABASE_URL` - PostgreSQL connection string
- `RABBITMQ_URL` - RabbitMQ AMQP connection
- `MAX_UPLOAD_SIZE_MB` - Maximum file upload size (default: 5000)
- `TEMP_STORAGE_PATH` - Temporary file storage location
- `LOG_LEVEL` - Logging verbosity (DEBUG, INFO, WARN, ERROR)

## Architecture

### Data Flow

```
GDB File Upload (ingestion-api)
    ↓
Hash & Dedupe (deduplication-service) → raw_imports table
    ↓
RabbitMQ: processing.parcel queue
    ↓
Normalize Addresses (address-normalizer, GPU)
    ↓
Match Parcel IDs (deterministic-matcher → splink-matcher, GPU)
    ↓
Write to Database (match-resolver) → parcels table (TimescaleDB)
    ↓
PostgreSQL Trigger → RabbitMQ: vectorization queue
    ↓
Generate Embeddings (vectorization-worker, GPU) → Qdrant
    ↓
Available for Queries (query-api, agent-service)
```

### Key Databases

**TimescaleDB (Layer 3)**:
- `raw_imports` - Original source records (Layer 1)
- `import_batches` - Import tracking
- `parcels` - Bitemporal parcel data (hypertable)
- `retr_events` - Real estate transfer events

**Qdrant (Layer 4)**:
- `normalized_addresses` - 4 vectors per parcel
- `properties` - Property descriptions for semantic search

## Contributing

### Adding a New Service

1. Create service directory: `services/{service-name}/`
2. Add `pyproject.toml` with dependencies
3. Import shared code: `from shared.models import ...`
4. Create `Dockerfile`, `main.py`, `tests/`
5. Update `docker-compose.yml` if needed

### Adding a Database Table

1. Create Alembic migration: `alembic revision -m "add new table"`
2. Edit generated file in `database/migrations/versions/`
3. Test migration: `alembic upgrade head`
4. Update schema docs in `database/schemas/`

## Deployment

### Docker Build

```bash
# Build ingestion-api
cd services/ingestion-api
docker build -t realestate/ingestion-api:latest .
```

### Kubernetes

```bash
# Apply configurations
kubectl apply -k infrastructure/k8s/

# Check status
kubectl get pods -n realestate
```

## Troubleshooting

### Docker Compose Issues

```bash
# View logs
docker-compose logs -f timescaledb
docker-compose logs -f rabbitmq

# Restart services
docker-compose restart

# Clean slate
docker-compose down -v
docker-compose up -d
```

### Database Connection Issues

```bash
# Test connection
psql postgresql://realestate:devpassword@localhost:5432/realestate

# Check if TimescaleDB is ready
docker-compose exec timescaledb pg_isready
```

### GDAL Installation (for local development)

**macOS**:
```bash
brew install gdal
```

**Ubuntu/Debian**:
```bash
sudo apt-get install gdal-bin libgdal-dev python3-gdal
```

## Documentation

- [Layer 1 Specification](specs/LAYER1_SPEC.md) - Ingestion & Raw Storage
- [Project Structure](specs/PROJECT_STRUCTURE.md) - Overall architecture
- [CLAUDE.md](CLAUDE.md) - Development instructions for AI assistants

## License

[Add your license here]

## Support

For issues and questions, please open a GitHub issue or contact the development team.
