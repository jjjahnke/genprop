"""
Shared utilities for Wisconsin Real Estate Database services.

This package provides common functionality for:
- Data models (Pydantic)
- Database connections (asyncpg)
- Message queue clients (RabbitMQ)
- Hash utilities
- Wisconsin address normalization
"""

__version__ = "0.1.0"

__all__ = [
    "models",
    "database",
    "rabbitmq",
    "hash_utils",
    "wisconsin_normalizer",
]
