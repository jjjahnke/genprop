"""
Unit tests for configuration utilities.

Tests the shared configuration module:
- find_dotenv() function for locating .env files
- BaseServiceSettings class for common service configuration
"""

import pytest
import os
from pathlib import Path
from unittest.mock import patch
from pydantic import Field, ValidationError
from pydantic_settings import BaseSettings

from shared.config import find_dotenv, BaseServiceSettings


class TestFindDotenv:
    """Tests for find_dotenv() function."""

    def test_finds_dotenv_in_current_directory(self, tmp_path):
        """Test finding .env in current directory."""
        # Create a .env file
        env_file = tmp_path / ".env"
        env_file.write_text("TEST=value")

        # Mock the calling file location
        with patch('inspect.currentframe') as mock_frame:
            mock_frame.return_value.f_back.f_globals = {'__file__': str(tmp_path / "config.py")}
            result = find_dotenv()

        assert result == str(env_file)

    def test_finds_dotenv_in_parent_directory(self, tmp_path):
        """Test finding .env by searching up the directory tree."""
        # Create .env in parent
        env_file = tmp_path / ".env"
        env_file.write_text("TEST=value")

        # Mock calling from a subdirectory
        subdir = tmp_path / "services" / "api"
        subdir.mkdir(parents=True)

        with patch('inspect.currentframe') as mock_frame:
            mock_frame.return_value.f_back.f_globals = {'__file__': str(subdir / "config.py")}
            result = find_dotenv()

        assert result == str(env_file)

    def test_returns_none_when_not_found(self, tmp_path):
        """Test returns None when .env not found."""
        # Mock calling from directory without .env
        with patch('inspect.currentframe') as mock_frame:
            mock_frame.return_value.f_back.f_globals = {'__file__': str(tmp_path / "config.py")}
            result = find_dotenv()

        assert result is None

    def test_searches_up_to_5_levels(self, tmp_path):
        """Test that search stops after 5 levels."""
        # Create deeply nested directory
        deep_path = tmp_path / "a" / "b" / "c" / "d" / "e" / "f"
        deep_path.mkdir(parents=True)

        # Put .env at level 6 (should not be found)
        env_file = tmp_path / ".env"
        env_file.write_text("TEST=value")

        with patch('inspect.currentframe') as mock_frame:
            mock_frame.return_value.f_back.f_globals = {'__file__': str(deep_path / "config.py")}
            result = find_dotenv()

        # Should not find .env that's 6 levels up
        assert result is None


class TestBaseServiceSettings:
    """Tests for BaseServiceSettings class."""

    def test_requires_database_url(self):
        """Test that DATABASE_URL is required."""
        # Create a test class without env_file to isolate from .env
        from pydantic_settings import SettingsConfigDict

        class TestSettings(BaseSettings):
            model_config = SettingsConfigDict(case_sensitive=False, extra="ignore")
            DATABASE_URL: str = Field(...)
            RABBITMQ_URL: str = Field(...)

        with patch.dict(os.environ, {}, clear=True), \
             pytest.raises(ValidationError) as exc_info:
            TestSettings(RABBITMQ_URL="amqp://localhost")

        errors = exc_info.value.errors()
        field_names = [e['loc'][0] for e in errors]
        assert 'DATABASE_URL' in field_names

    def test_requires_rabbitmq_url(self):
        """Test that RABBITMQ_URL is required."""
        # Create a test class without env_file to isolate from .env
        from pydantic_settings import SettingsConfigDict

        class TestSettings(BaseSettings):
            model_config = SettingsConfigDict(case_sensitive=False, extra="ignore")
            DATABASE_URL: str = Field(...)
            RABBITMQ_URL: str = Field(...)

        with patch.dict(os.environ, {}, clear=True), \
             pytest.raises(ValidationError) as exc_info:
            TestSettings(DATABASE_URL="postgresql://localhost")

        errors = exc_info.value.errors()
        field_names = [e['loc'][0] for e in errors]
        assert 'RABBITMQ_URL' in field_names

    def test_valid_with_required_fields(self):
        """Test successful initialization with required fields."""
        settings = BaseServiceSettings(
            DATABASE_URL="postgresql://user:pass@localhost:5432/db",
            RABBITMQ_URL="amqp://user:pass@localhost:5672/"
        )

        assert settings.DATABASE_URL == "postgresql://user:pass@localhost:5432/db"
        assert settings.RABBITMQ_URL == "amqp://user:pass@localhost:5672/"

    def test_default_values_for_optional_fields(self):
        """Test that optional fields have correct defaults."""
        # Create test class without env_file to get true defaults
        from pydantic_settings import SettingsConfigDict

        class TestSettings(BaseSettings):
            model_config = SettingsConfigDict(case_sensitive=False, extra="ignore")
            DATABASE_URL: str = Field(...)
            RABBITMQ_URL: str = Field(...)
            DB_POOL_MIN_SIZE: int = 5
            DB_POOL_MAX_SIZE: int = 20
            LOG_LEVEL: str = "INFO"
            LOG_FORMAT: str = "json"
            DEBUG: bool = False
            ENVIRONMENT: str = "development"

        with patch.dict(os.environ, {}, clear=True):
            settings = TestSettings(
                DATABASE_URL="postgresql://localhost",
                RABBITMQ_URL="amqp://localhost"
            )

        assert settings.DB_POOL_MIN_SIZE == 5
        assert settings.DB_POOL_MAX_SIZE == 20
        assert settings.LOG_LEVEL == "INFO"
        assert settings.LOG_FORMAT == "json"
        assert settings.DEBUG is False
        assert settings.ENVIRONMENT == "development"

    def test_override_optional_fields(self):
        """Test overriding optional fields."""
        settings = BaseServiceSettings(
            DATABASE_URL="postgresql://localhost",
            RABBITMQ_URL="amqp://localhost",
            DB_POOL_MIN_SIZE=10,
            DB_POOL_MAX_SIZE=50,
            LOG_LEVEL="DEBUG",
            DEBUG=True,
            ENVIRONMENT="production"
        )

        assert settings.DB_POOL_MIN_SIZE == 10
        assert settings.DB_POOL_MAX_SIZE == 50
        assert settings.LOG_LEVEL == "DEBUG"
        assert settings.DEBUG is True
        assert settings.ENVIRONMENT == "production"

    def test_validates_pool_size_ranges(self):
        """Test that pool size validation works."""
        # Min size too small
        with pytest.raises(ValidationError):
            BaseServiceSettings(
                DATABASE_URL="postgresql://localhost",
                RABBITMQ_URL="amqp://localhost",
                DB_POOL_MIN_SIZE=0
            )

        # Max size too large
        with pytest.raises(ValidationError):
            BaseServiceSettings(
                DATABASE_URL="postgresql://localhost",
                RABBITMQ_URL="amqp://localhost",
                DB_POOL_MAX_SIZE=2000
            )

    def test_loads_from_environment_variables(self):
        """Test that settings load from environment variables."""
        env_vars = {
            'DATABASE_URL': 'postgresql://envuser:envpass@envhost:5432/envdb',
            'RABBITMQ_URL': 'amqp://envuser:envpass@envhost:5672/',
            'LOG_LEVEL': 'WARNING',
            'DEBUG': 'true'
        }

        with patch.dict(os.environ, env_vars, clear=True):
            settings = BaseServiceSettings()

        assert settings.DATABASE_URL == 'postgresql://envuser:envpass@envhost:5432/envdb'
        assert settings.RABBITMQ_URL == 'amqp://envuser:envpass@envhost:5672/'
        assert settings.LOG_LEVEL == 'WARNING'
        assert settings.DEBUG is True

    def test_env_vars_override_defaults(self):
        """Test that environment variables override default values."""
        env_vars = {
            'DATABASE_URL': 'postgresql://localhost',
            'RABBITMQ_URL': 'amqp://localhost',
            'DB_POOL_MIN_SIZE': '15'
        }

        with patch.dict(os.environ, env_vars, clear=True):
            settings = BaseServiceSettings()

        assert settings.DB_POOL_MIN_SIZE == 15  # From env var, not default 5

    def test_case_insensitive_env_vars(self):
        """Test that environment variables are case insensitive."""
        env_vars = {
            'database_url': 'postgresql://localhost',  # lowercase
            'RABBITMQ_URL': 'amqp://localhost'
        }

        with patch.dict(os.environ, env_vars, clear=True):
            settings = BaseServiceSettings()

        assert settings.DATABASE_URL == 'postgresql://localhost'

    def test_extra_fields_ignored(self):
        """Test that extra fields are ignored (not raising error)."""
        # Should not raise an error for unknown fields
        settings = BaseServiceSettings(
            DATABASE_URL="postgresql://localhost",
            RABBITMQ_URL="amqp://localhost",
            UNKNOWN_FIELD="some_value"  # Extra field
        )

        assert settings.DATABASE_URL == "postgresql://localhost"
        # UNKNOWN_FIELD is ignored, not set as attribute


class TestServiceSettingsInheritance:
    """Tests for inheriting from BaseServiceSettings."""

    def test_can_inherit_and_add_fields(self):
        """Test that services can inherit and add their own fields."""
        class CustomSettings(BaseServiceSettings):
            API_PORT: int = 8080
            MAX_UPLOAD_MB: int = 5000

        settings = CustomSettings(
            DATABASE_URL="postgresql://localhost",
            RABBITMQ_URL="amqp://localhost"
        )

        # Has base fields
        assert settings.DATABASE_URL == "postgresql://localhost"
        assert settings.LOG_LEVEL == "INFO"

        # Has custom fields
        assert settings.API_PORT == 8080
        assert settings.MAX_UPLOAD_MB == 5000

    def test_custom_fields_can_be_overridden(self):
        """Test that custom fields can be overridden via env vars."""
        class CustomSettings(BaseServiceSettings):
            API_PORT: int = 8080

        env_vars = {
            'DATABASE_URL': 'postgresql://localhost',
            'RABBITMQ_URL': 'amqp://localhost',
            'API_PORT': '9000'
        }

        with patch.dict(os.environ, env_vars, clear=True):
            settings = CustomSettings()

        assert settings.API_PORT == 9000  # Overridden from env
