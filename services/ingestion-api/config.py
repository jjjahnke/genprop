"""
Configuration settings for the ingestion API service.

Uses Pydantic Settings to load configuration from environment variables.
"""

from pydantic_settings import BaseSettings, SettingsConfigDict
from typing import Optional


class Settings(BaseSettings):
    """
    Application configuration loaded from environment variables.

    All settings can be overridden via environment variables or .env file.
    """

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore"
    )

    # === Database Configuration ===
    DATABASE_URL: str = "postgresql://realestate:devpassword@localhost:5432/realestate"
    DB_POOL_MIN_SIZE: int = 5
    DB_POOL_MAX_SIZE: int = 20

    # === RabbitMQ Configuration ===
    RABBITMQ_URL: str = "amqp://realestate:devpassword@localhost:5672/"
    RABBITMQ_EXCHANGE: str = "ingestion.direct"
    RABBITMQ_PREFETCH_COUNT: int = 100

    # === File Upload Configuration ===
    MAX_UPLOAD_SIZE_MB: int = 5000
    TEMP_STORAGE_PATH: str = "/tmp/gdb-processing"
    ALLOWED_GDB_EXTENSIONS: list[str] = [".gdb", ".zip"]
    ALLOWED_CSV_EXTENSIONS: list[str] = [".csv"]

    # === Processing Configuration ===
    BATCH_SIZE: int = 1000
    DEFAULT_LAYER_NAME: str = "V11_Parcels"
    CRS_TARGET: int = 3071  # Wisconsin Transverse Mercator (EPSG:3071)

    # === API Server Configuration ===
    API_HOST: str = "0.0.0.0"
    API_PORT: int = 8080
    API_WORKERS: int = 4
    API_TITLE: str = "Wisconsin Real Estate - Ingestion API"
    API_VERSION: str = "1.0.0"
    API_DESCRIPTION: str = "Layer 1: Data ingestion service for GDB and CSV file uploads"

    # === Logging Configuration ===
    LOG_LEVEL: str = "INFO"
    LOG_FORMAT: str = "json"  # json or text

    # === Development Configuration ===
    DEBUG: bool = False
    RELOAD: bool = False

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
