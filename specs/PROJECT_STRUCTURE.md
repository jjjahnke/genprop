# Wisconsin Real Estate Database - Project Structure

## Overview

Bitemporal real estate database for Wisconsin parcels with semantic search, ownership tracking, and natural language query interface. Data flows through 5 layers from raw ingestion to user-facing agent.

## Architecture Layers

1. **Layer 1 (Ingestion)**: Deduplicate and store raw data
2. **Layer 2 (Matching)**: Match and normalize records (GPU-accelerated)
3. **Layer 3 (Database)**: TimescaleDB bitemporal storage (source of truth)
4. **Layer 4 (Intelligence)**: Vectorization (Qdrant) and analytics
5. **Layer 5 (API/Agent)**: Query API and conversational interface

## Technology Stack

- **Languages**: Python, SQL
- **Databases**: TimescaleDB (PostgreSQL + PostGIS), Qdrant (vectors)
- **Message Queue**: RabbitMQ
- **Orchestration**: Prefect
- **Container**: Docker + Kubernetes
- **GPU**: RAPIDS (Layer 2 matching), sentence-transformers (Layer 4 vectorization)

## Repository Structure

```
wisconsin-realestate/
├── services/                           # Microservices (Layers 1-5)
│   ├── shared/                         # Common libraries (models, DB, queues, hash utils)
│   ├── ingestion-api/                  # Layer 1: GDB/CSV upload endpoint
│   ├── deduplication-service/          # Layer 1: Hash & dedupe
│   ├── address-normalizer/             # Layer 2: GPU address normalization
│   ├── deterministic-matcher/          # Layer 2: Exact parcel ID matching
│   ├── splink-matcher/                 # Layer 2: GPU probabilistic matching
│   ├── match-resolver/                 # Layer 2: Write to TimescaleDB
│   ├── vectorization-worker/           # Layer 4: GPU embedding generation
│   ├── postgres-event-listener/        # Layer 3: Bridge DB triggers → RabbitMQ
│   ├── query-api/                      # Layer 5: REST API
│   └── agent-service/                  # Layer 5: NL query processing
│
├── infrastructure/                     # Deployment configs
│   ├── k8s/                           # Kubernetes manifests (deployments, services, configmaps)
│   └── terraform/                     # Infrastructure as code
│
├── database/                          # Layer 3 schemas & migrations
│   ├── migrations/                    # Alembic/Flyway SQL migrations
│   └── schemas/                       # Complete SQL schema definitions
│
├── workflows/                         # Prefect workflow definitions
│   └── prefect/                       # Orchestration flows (imports, backfills)
│
├── scripts/                           # Utility scripts
│   ├── import_gdb.py                 # CLI for GDB imports
│   └── backfill_vectors.py           # One-time vectorization jobs
│
├── docs/                              # Documentation
│   ├── architecture.md               # System architecture
│   ├── api.md                        # API reference
│   └── deployment.md                 # Deployment guide
│
└── tests/                             # Integration & E2E tests
    ├── integration/
    └── e2e/
```

## Data Flow

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

## Key Databases & Tables

**TimescaleDB (Layer 3)**:
- `raw_imports` - Original source records (Layer 1)
- `parcels` - Bitemporal parcel data (hypertable)
- `retr_events` - Real estate transfer events
- `corporate_entities` - LLC/company ownership data

**Qdrant (Layer 4)**:
- `normalized_addresses` - 4 vectors per parcel (address, street, owner, tax_address)
- `properties` - Property descriptions for semantic search
- `corporate_entities` - Entity name fuzzy matching
- `knowledge_base` - RAG documents (WI property law, guides)

## Message Queues

**RabbitMQ Queues**:
- `processing.parcel` - Layer 1 → Layer 2
- `processing.retr` - RETR events
- `matching.deterministic` - Exact ID matching
- `matching.probabilistic` - Splink matching
- `matching.resolved` - Final matches → DB
- `vectorization` - Layer 3 → Layer 4
- `dlq.*` - Dead letter queues for failures

## Where Things Go

**New microservice?** → `services/{service-name}/`
- Include: `Dockerfile`, `requirements.txt`, `main.py`, `tests/`
- Import shared code: `from shared.models import ...`

**New database table?** → `database/migrations/versions/{nnn}_description.sql`
- Write Alembic migration
- Update `database/schemas/` documentation

**New workflow?** → `workflows/prefect/{workflow_name}_flow.py`
- Define as Prefect `@flow` with `@task` stages

**New API endpoint?** → `services/query-api/routers/{resource}.py`
- Use FastAPI router pattern

**New shared utility?** → `services/shared/{utility_name}.py`
- Import from all services that need it

## Wisconsin Data Specifics

**Parcel Data Format**: File Geodatabase (.gdb or .gdb.zip)
- Layer: `V11_Parcels` (main parcel polygons)
- Schema: Wisconsin V11 Statewide Parcel Database
- CRS: EPSG:3071 (Wisconsin Transverse Mercator)

**Address Structure**: 8-component normalized format
- ADDNUM, STREETNAME, STREETTYPE, PLACENAME, ZIPCODE, etc.
- Special patterns: Fire numbers (N1234 STATE RD 67), rural routes

**RETR Data Format**: CSV files from Wisconsin DOR
- Monthly real estate transfer returns
- Parcel IDs don't match V11 directly (requires fuzzy matching)

## Development Workflow

1. **Local development**: `docker-compose up` (databases + queues)
2. **Build service**: `docker build -t realestate/{service}:latest services/{service}/`
3. **Run tests**: `pytest tests/`
4. **Deploy to k8s**: `kubectl apply -k infrastructure/k8s/`
5. **Run workflow**: `python workflows/prefect/{flow}.py`

## GPU Usage

- **Layer 2 (4x P100)**: RAPIDS cuDF (address normalization), Splink (matching)
- **Layer 4 (2x RTX 4000)**: sentence-transformers (embeddings)
- Node selectors: `gpu-type: p100` or `gpu-type: rtx4000`

## External Dependencies

- **TimescaleDB**: Primary database (Docker Compose VM)
- **Qdrant**: Vector database (Docker Compose VM)
- **RabbitMQ**: Message queue (Docker Compose VM)
- **Redis**: Cache (Docker Compose VM)
- **Prefect**: Workflow orchestration (k8s pod)

## Reference Documents

See `docs/` for:
- Complete architecture diagrams
- Layer-by-layer implementation details
- API specifications
- Deployment procedures
