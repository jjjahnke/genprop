# Testing Guide

## Test Structure

The ingestion-api has three types of tests:

### 1. Unit Tests (Fast, No Dependencies)

Unit tests mock all external dependencies (database, RabbitMQ, file system). They run quickly and don't require docker-compose.

**Location**: `test_*.py` (except `test_integration.py`)

**Run**:
```bash
pytest -m "not integration"
```

**Coverage Target**: 85%+

### 2. Integration Tests (Requires Docker)

Integration tests use real docker-compose services (PostgreSQL, RabbitMQ) and test full upload flows.

**Location**: `test_integration.py`

**Prerequisites**:
```bash
# Start services
docker-compose up -d postgres rabbitmq

# Run migrations
poetry run alembic upgrade head
```

**Run**:
```bash
# Run only integration tests
pytest -m integration

# Run all tests (unit + integration)
pytest
```

### 3. E2E Tests (Manual, Production-like)

E2E tests use real data files and validate end-to-end flows.

**Manual Test**:
```bash
# 1. Start all services
docker-compose up -d

# 2. Start API
poetry run fastapi dev main.py

# 3. Upload real GDB file
curl -X POST http://localhost:8000/api/v1/ingest/parcel/gdb \
  -F "file=@/path/to/parcels.gdb.zip" \
  -F "source_name=Dane County 2025"

# 4. Monitor status
curl http://localhost:8000/api/v1/ingest/status/{batch_id}

# 5. Check RabbitMQ
docker exec -it rabbitmq rabbitmqctl list_queues
```

## Test Fixtures

### CSV Fixtures
- `fixtures/sample_retr.csv` - 1000 RETR records from production data
- `fixtures/sample_parcel.csv` - Sample parcel CSV (used in unit tests)

### GDB Fixtures
- `fixtures/test_parcels.gdb.zip` - 75 synthetic Wisconsin parcels
- Layer: `V11_Parcels`
- CRS: `EPSG:3071` (Wisconsin Transverse Mercator)

**Regenerate GDB Fixture**:
```bash
cd tests/fixtures
poetry run python create_test_gdb.py
```

## Running Tests

### Quick Unit Tests (No Docker)
```bash
poetry run pytest -m "not integration" -v
```

### All Tests with Coverage
```bash
# Ensure docker-compose is running
docker-compose up -d postgres rabbitmq

# Run migrations
poetry run alembic upgrade head

# Run all tests
poetry run pytest --cov=. --cov-report=html
```

### Specific Test File
```bash
poetry run pytest tests/test_gdb_processor.py -v
```

### Specific Test
```bash
poetry run pytest tests/test_integration.py::TestCSVUploadIntegration::test_csv_upload_full_flow -v
```

### Skip Slow Tests
```bash
poetry run pytest -m "not integration" --maxfail=1
```

## Debugging Tests

### View Database State
```bash
# Connect to test database
docker exec -it postgres psql -U genprop -d genprop

# Query batches
SELECT batch_id, source_name, status, processed_records
FROM import_batches
ORDER BY started_at DESC
LIMIT 10;
```

### View RabbitMQ Queues
```bash
# List queues
docker exec -it rabbitmq rabbitmqctl list_queues

# Purge test queue
docker exec -it rabbitmq rabbitmqctl purge_queue deduplication
```

### Enable Debug Logging
```python
# In test file
import logging
logging.basicConfig(level=logging.DEBUG)
```

## CI/CD Integration

### GitHub Actions (Example)
```yaml
name: Tests

on: [push, pull_request]

jobs:
  test:
    runs-on: ubuntu-latest

    services:
      postgres:
        image: timescale/timescaledb:latest-pg16
        env:
          POSTGRES_PASSWORD: password

      rabbitmq:
        image: rabbitmq:3-management

    steps:
      - uses: actions/checkout@v3
      - uses: actions/setup-python@v4
        with:
          python-version: '3.12'

      - name: Install dependencies
        run: poetry install

      - name: Run migrations
        run: poetry run alembic upgrade head

      - name: Run tests
        run: poetry run pytest --cov=. --cov-report=xml

      - name: Upload coverage
        uses: codecov/codecov-action@v3
```

## Test Coverage

Current coverage: **85%+**

**High Coverage** (>90%):
- `services/gdb_processor.py` - 88%
- `models/schemas.py` - 96%
- `config.py` - 93%

**Areas for Improvement** (<60%):
- `main.py` - 39% (startup code, hard to test)
- `exceptions.py` - 51% (error handlers)
- `routers/csv_ingest.py` - 60% (error paths)

## Common Issues

### Issue: Integration tests fail with "connection refused"
**Solution**: Ensure docker-compose is running:
```bash
docker-compose up -d postgres rabbitmq
docker-compose ps  # Verify services are up
```

### Issue: Tests fail with "table does not exist"
**Solution**: Run migrations:
```bash
poetry run alembic upgrade head
```

### Issue: RabbitMQ queue full
**Solution**: Purge test queues:
```bash
docker exec -it rabbitmq rabbitmqctl purge_queue deduplication
```

### Issue: Port already in use
**Solution**: Stop conflicting services:
```bash
docker-compose down
lsof -ti:5432 | xargs kill  # PostgreSQL
lsof -ti:5672 | xargs kill  # RabbitMQ
```
