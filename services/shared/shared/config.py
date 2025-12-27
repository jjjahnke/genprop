"""
Shared configuration utilities for all services.

Provides base settings class with common configuration (DATABASE_URL, RABBITMQ_URL)
that all services inherit from.
"""

from pathlib import Path
from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


def find_dotenv() -> str | None:
    """
    Search upward from calling file to find .env file.

    Searches up the directory tree from the location where this function
    is called, looking for a .env file. Useful for finding .env at repo
    root when running from nested service directories.

    Returns:
        Path to .env file if found, None otherwise.

    Note:
        Used for local development only. In k8s, configuration comes
        from ConfigMaps and Secrets as environment variables.

    Example:
        ```python
        # Called from services/ingestion-api/config.py
        # Searches: services/ingestion-api/ -> services/ -> repo-root/
        # Finds: /repo-root/.env
        ```
    """
    # Start from the calling module's location
    import inspect
    frame = inspect.currentframe()
    if frame and frame.f_back:
        caller_file = frame.f_back.f_globals.get('__file__')
        if caller_file:
            current = Path(caller_file).resolve().parent
        else:
            current = Path.cwd()
    else:
        current = Path.cwd()

    # Search up directory tree (max 5 levels to reach repo root)
    for _ in range(5):
        env_file = current / ".env"
        if env_file.exists():
            return str(env_file)
        current = current.parent

    return None


class BaseServiceSettings(BaseSettings):
    """
    Base configuration for all services.

    Provides common settings (database, message queue, logging) that
    all services need. Individual services should inherit from this
    and add service-specific settings.

    Configuration priority (highest to lowest):
    1. Environment variables (from k8s ConfigMap/Secret or shell)
    2. .env file (local development only)
    3. Default values (where provided)

    Required settings have no defaults and will cause the application
    to fail at startup if not provided. This prevents misconfiguration
    in production (e.g., accidentally using localhost).

    Example:
        ```python
        # In services/ingestion-api/config.py
        from shared.config import BaseServiceSettings

        class Settings(BaseServiceSettings):
            # Service-specific settings
            MAX_UPLOAD_SIZE_MB: int = 5000
            API_PORT: int = 8080

        settings = Settings()
        ```

    Local Development:
        ```bash
        # Copy .env.example to .env at repo root
        cp .env.example .env

        # Edit .env with your values
        vim .env

        # Run service - Pydantic will find and load .env
        cd services/ingestion-api
        poetry run uvicorn main:app
        ```

    Kubernetes Deployment:
        ```yaml
        # ConfigMap provides required settings as env vars
        apiVersion: v1
        kind: ConfigMap
        metadata:
          name: service-config
        data:
          DATABASE_URL: "postgresql://user@timescaledb:5432/db"
          RABBITMQ_URL: "amqp://user@rabbitmq:5672/"
        ```
    """

    model_config = SettingsConfigDict(
        env_file=find_dotenv(),
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
        env_file_required=False  # Don't fail if .env missing (k8s uses env vars)
    )

    # === Required Settings (NO DEFAULTS - must be provided) ===
    DATABASE_URL: str = Field(
        ...,
        description="PostgreSQL connection string (e.g., postgresql://user:pass@host:5432/db)"
    )
    RABBITMQ_URL: str = Field(
        ...,
        description="RabbitMQ connection string (e.g., amqp://user:pass@host:5672/)"
    )

    # === Common Optional Settings ===
    DB_POOL_MIN_SIZE: int = Field(
        5,
        description="Minimum database connection pool size",
        ge=1,
        le=100
    )
    DB_POOL_MAX_SIZE: int = Field(
        20,
        description="Maximum database connection pool size",
        ge=1,
        le=1000
    )

    LOG_LEVEL: str = Field(
        "INFO",
        description="Logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL)"
    )
    LOG_FORMAT: str = Field(
        "json",
        description="Log format (json or text)"
    )

    DEBUG: bool = Field(
        False,
        description="Enable debug mode"
    )
    ENVIRONMENT: str = Field(
        "development",
        description="Environment name (development, staging, production)"
    )


__all__ = ["BaseServiceSettings", "find_dotenv"]
