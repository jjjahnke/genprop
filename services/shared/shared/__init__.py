"""
Shared utilities for Wisconsin Real Estate Database services.

This package provides common functionality for:
- Configuration management (Pydantic Settings)
- Data models (Pydantic)
- Database connections (asyncpg)
- Message queue clients (RabbitMQ)
"""

__version__ = "0.1.0"

__all__ = [
    "config",
    "models",
    "database",
    "rabbitmq",
]
