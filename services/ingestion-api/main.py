"""
Ingestion API - Main FastAPI Application.

REST API for uploading and processing Wisconsin real estate data files (GDB and CSV).
Provides endpoints for:
- CSV file uploads (parcels, RETR)
- GDB file uploads (parcels)
- Batch status tracking
- Health monitoring
"""

import logging
from contextlib import asynccontextmanager
from datetime import datetime, timezone

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
from fastapi.middleware.cors import CORSMiddleware

from shared.database import get_db_pool, close_db_pool, check_db_health
from shared.rabbitmq import check_rabbitmq_health, close_rabbitmq_connection
from routers import csv_ingest, gdb_ingest, status
from models.schemas import HealthResponse, ErrorResponse
from config import Settings
from exceptions import register_exception_handlers
from middleware import RequestIDMiddleware

# Configure structured logging
from services.logging_utils import StructuredFormatter

# Create console handler with structured formatter
console_handler = logging.StreamHandler()
console_handler.setLevel(logging.INFO)
console_handler.setFormatter(
    StructuredFormatter(
        datefmt='%Y-%m-%d %H:%M:%S'
    )
)

# Configure root logger
root_logger = logging.getLogger()
root_logger.setLevel(logging.INFO)
root_logger.addHandler(console_handler)

logger = logging.getLogger(__name__)

# Load settings
settings = Settings()


@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    Lifespan context manager for startup and shutdown events.

    Startup:
        - Initialize database connection pool
        - Initialize RabbitMQ connection
        - Log service configuration

    Shutdown:
        - Close database pool
        - Close RabbitMQ connection
    """
    # Startup
    logger.info("=" * 80)
    logger.info("Ingestion API - Starting up")
    logger.info(f"Environment: {settings.ENVIRONMENT}")
    logger.info(f"Debug mode: {settings.DEBUG}")
    logger.info(f"Max upload size: {settings.MAX_UPLOAD_SIZE_MB} MB")
    logger.info(f"Temp storage: {settings.TEMP_STORAGE_PATH}")
    logger.info("=" * 80)

    try:
        # Initialize database connection pool
        logger.info("Initializing database connection pool...")
        await get_db_pool()
        logger.info("Database connection pool initialized")

        # Test RabbitMQ connection
        logger.info("Testing RabbitMQ connection...")
        if check_rabbitmq_health():
            logger.info("RabbitMQ connection established")
        else:
            logger.warning("RabbitMQ health check failed (will retry on first use)")

        logger.info("Startup complete")

    except Exception as e:
        logger.error(f"Startup failed: {e}", exc_info=True)
        raise

    yield

    # Shutdown
    logger.info("Shutting down Ingestion API...")

    try:
        # Close database pool
        logger.info("Closing database connection pool...")
        await close_db_pool()
        logger.info("Database connection pool closed")

        # Close RabbitMQ connection
        logger.info("Closing RabbitMQ connection...")
        close_rabbitmq_connection()
        logger.info("RabbitMQ connection closed")

        logger.info("Shutdown complete")

    except Exception as e:
        logger.error(f"Error during shutdown: {e}", exc_info=True)


# Create FastAPI application
app = FastAPI(
    title="Ingestion API",
    description="""
    Wisconsin Real Estate Data Ingestion Service

    Upload and process large-scale property data files:
    - **CSV Uploads**: Parcel data, RETR (Real Estate Transfer Returns)
    - **GDB Uploads**: Wisconsin V11 Statewide Parcel Database
    - **Batch Tracking**: Monitor upload progress and statistics
    - **Health Monitoring**: Service and dependency health checks

    All uploads are processed asynchronously with batch tracking.
    Poll the status endpoint for progress updates.
    """,
    version="1.0.0",
    lifespan=lifespan,
    docs_url="/docs",
    redoc_url="/redoc",
    openapi_url="/openapi.json"
)

# Add Request ID middleware (first, so it wraps everything)
app.add_middleware(RequestIDMiddleware)
logger.info("Registered middleware: Request ID tracking")

# Add CORS middleware (configure allowed origins based on environment)
if settings.DEBUG or settings.ENVIRONMENT == "development":
    # Allow all origins in development
    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )
    logger.info("CORS: Allowing all origins (development mode)")
else:
    # Restrict origins in production (configure as needed)
    app.add_middleware(
        CORSMiddleware,
        allow_origins=[
            "https://yourdomain.com",  # Replace with actual domain
        ],
        allow_credentials=True,
        allow_methods=["GET", "POST"],
        allow_headers=["*"],
    )
    logger.info("CORS: Restricted origins (production mode)")


# Register exception handlers
register_exception_handlers(app)

# Register routers
app.include_router(csv_ingest.router)
logger.info("Registered router: CSV Ingestion")

app.include_router(gdb_ingest.router)
logger.info("Registered router: GDB Ingestion")

app.include_router(status.router)
logger.info("Registered router: Batch Status")


@app.get(
    "/",
    summary="API Root",
    description="Returns basic API information and available endpoints"
)
async def root():
    """Root endpoint with API information."""
    return {
        "service": "Ingestion API",
        "version": "1.0.0",
        "status": "running",
        "docs": "/docs",
        "health": "/health"
    }


@app.get(
    "/health",
    response_model=HealthResponse,
    summary="Health Check",
    description="Check service health and dependency status (database, RabbitMQ, storage)"
)
async def health_check() -> HealthResponse:
    """
    Health check endpoint.

    Checks:
    - Database connectivity
    - RabbitMQ connectivity
    - Storage availability

    Returns:
    - 200 OK: All services healthy
    - 200 OK (degraded): Some services unhealthy
    - 503 Service Unavailable: Critical services down
    """
    services = {}

    # Check database
    try:
        db_healthy = await check_db_health()
        services["database"] = "healthy" if db_healthy else "unhealthy"
    except Exception as e:
        logger.error(f"Database health check failed: {e}")
        services["database"] = "unhealthy"

    # Check RabbitMQ
    try:
        rabbitmq_healthy = check_rabbitmq_health()
        services["rabbitmq"] = "healthy" if rabbitmq_healthy else "unhealthy"
    except Exception as e:
        logger.error(f"RabbitMQ health check failed: {e}")
        services["rabbitmq"] = "unhealthy"

    # Check storage
    try:
        from pathlib import Path
        storage_path = Path(settings.TEMP_STORAGE_PATH)
        storage_path.mkdir(parents=True, exist_ok=True)
        services["storage"] = "healthy"
    except Exception as e:
        logger.error(f"Storage health check failed: {e}")
        services["storage"] = "unhealthy"

    # Determine overall status
    unhealthy_count = sum(1 for s in services.values() if s == "unhealthy")

    if unhealthy_count == 0:
        status = "healthy"
    elif unhealthy_count >= 2:  # Database or RabbitMQ critical
        status = "unhealthy"
    else:
        status = "degraded"

    response = HealthResponse(
        status=status,
        timestamp=datetime.now(timezone.utc),
        services=services,
        version="1.0.0"
    )

    # Return 503 if unhealthy (for k8s liveness probes)
    if status == "unhealthy":
        return JSONResponse(
            status_code=503,
            content=response.model_dump()
        )

    return response


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(
        "main:app",
        host=settings.API_HOST,
        port=settings.API_PORT,
        reload=settings.RELOAD,
        workers=1 if settings.RELOAD else settings.API_WORKERS,
        log_level=settings.LOG_LEVEL.lower()
    )
