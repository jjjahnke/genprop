"""
Pytest configuration and shared fixtures for ingestion-api tests.

Provides common test fixtures and configuration for all test modules.
"""

import pytest
import asyncio
from pathlib import Path


@pytest.fixture(scope="session")
def event_loop():
    """Create an event loop for async tests."""
    loop = asyncio.get_event_loop_policy().new_event_loop()
    yield loop
    loop.close()


@pytest.fixture
def fixtures_dir():
    """Get the fixtures directory path."""
    return Path(__file__).parent / "fixtures"


@pytest.fixture
def sample_parcel_csv_file(fixtures_dir):
    """Get path to sample parcel CSV fixture."""
    return fixtures_dir / "sample_parcel.csv"


@pytest.fixture
def sample_retr_csv_file(fixtures_dir):
    """Get path to sample RETR CSV fixture."""
    return fixtures_dir / "sample_retr.csv"
