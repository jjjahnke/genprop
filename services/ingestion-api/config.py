"""
Configuration settings for the ingestion API service.

Inherits common settings from shared.config.BaseServiceSettings and adds
ingestion-api specific configuration.
"""

from pydantic import Field
from shared.config import BaseServiceSettings


class Settings(BaseServiceSettings):
    """
    Ingestion API configuration.

    Inherits common settings (DATABASE_URL, RABBITMQ_URL, logging, etc.)
    from BaseServiceSettings and adds ingestion-api specific settings.

    Local Development:
        ```bash
        # Ensure .env exists at repo root with required settings
        cp .env.example .env
        vim .env  # Set DATABASE_URL and RABBITMQ_URL

        # Run service
        cd services/ingestion-api
        poetry run uvicorn main:app --reload
        ```

    Kubernetes Deployment:
        ```yaml
        # ConfigMap provides all settings as environment variables
        apiVersion: v1
        kind: ConfigMap
        metadata:
          name: ingestion-api-config
        data:
          DATABASE_URL: "postgresql://user@timescaledb:5432/db"
          RABBITMQ_URL: "amqp://user@rabbitmq:5672/"
          MAX_UPLOAD_SIZE_MB: "5000"
          API_PORT: "8080"
        ```
    """

    # === File Upload Configuration ===
    MAX_UPLOAD_SIZE_MB: int = Field(
        5000,
        description="Maximum upload size in MB",
        ge=1,
        le=10000
    )
    TEMP_STORAGE_PATH: str = Field(
        "/tmp/gdb-processing",
        description="Temporary file storage path for uploaded files"
    )
    ALLOWED_GDB_EXTENSIONS: list[str] = Field(
        default=[".gdb", ".zip"],
        description="Allowed file extensions for GDB uploads"
    )
    ALLOWED_CSV_EXTENSIONS: list[str] = Field(
        default=[".csv"],
        description="Allowed file extensions for CSV uploads"
    )

    # === Processing Configuration ===
    BATCH_SIZE: int = Field(
        1000,
        description="Number of records to process per batch",
        ge=100,
        le=10000
    )
    DEFAULT_LAYER_NAME: str = Field(
        "V11_Parcels",
        description="Default GDB layer name for Wisconsin V11 parcels"
    )
    CRS_TARGET: int = Field(
        3071,
        description="Target CRS EPSG code (Wisconsin Transverse Mercator)"
    )

    # === RabbitMQ Configuration ===
    RABBITMQ_EXCHANGE: str = Field(
        "ingestion.direct",
        description="RabbitMQ exchange name for ingestion messages"
    )
    RABBITMQ_PREFETCH_COUNT: int = Field(
        100,
        description="RabbitMQ consumer prefetch count",
        ge=1,
        le=1000
    )

    # === API Server Configuration ===
    API_HOST: str = Field(
        "0.0.0.0",
        description="API server host address"
    )
    API_PORT: int = Field(
        8080,
        description="API server port",
        ge=1,
        le=65535
    )
    API_WORKERS: int = Field(
        4,
        description="Number of Uvicorn worker processes",
        ge=1,
        le=32
    )
    API_TITLE: str = Field(
        "Wisconsin Real Estate - Ingestion API",
        description="API documentation title"
    )
    API_VERSION: str = Field(
        "1.0.0",
        description="API version"
    )
    API_DESCRIPTION: str = Field(
        "Layer 1: Data ingestion service for GDB and CSV file uploads",
        description="API documentation description"
    )

    # === Development Configuration ===
    RELOAD: bool = Field(
        False,
        description="Enable auto-reload on code changes (development only)"
    )

    @property
    def max_upload_size_bytes(self) -> int:
        """Get maximum upload size in bytes."""
        return self.MAX_UPLOAD_SIZE_MB * 1024 * 1024

    @property
    def is_development(self) -> bool:
        """Check if running in development mode."""
        return self.DEBUG or self.RELOAD


# Global settings instance
settings = Settings()


__all__ = ["settings", "Settings"]
