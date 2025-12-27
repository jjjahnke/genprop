"""
RabbitMQ connection and messaging utilities.

This module provides:
- RabbitMQ connection management with singleton pattern
- Message publishing with retry logic and persistence
- Queue declarations for Layer 1 processing
"""

import pika
import json
import os
from typing import Optional, Any, Dict
import logging
import time

logger = logging.getLogger(__name__)

# Global connection and channel (singleton)
_rabbitmq_connection: Optional[pika.BlockingConnection] = None
_rabbitmq_channel: Optional[pika.channel.Channel] = None


def get_rabbitmq_connection() -> pika.channel.Channel:
    """
    Get or create the RabbitMQ connection and channel.

    Uses a singleton pattern to ensure only one connection exists per process.
    The connection is automatically created on first access with configuration
    from environment variables.

    Returns:
        pika.channel.Channel: The RabbitMQ channel for publishing/consuming

    Example:
        ```python
        channel = get_rabbitmq_connection()
        channel.basic_publish(
            exchange='',
            routing_key='deduplication',
            body=json.dumps(message)
        )
        ```
    """
    global _rabbitmq_connection, _rabbitmq_channel

    if _rabbitmq_connection is None or _rabbitmq_connection.is_closed:
        rabbitmq_url = os.getenv(
            "RABBITMQ_URL",
            "amqp://realestate:devpassword@localhost:5672/"
        )

        logger.info("Creating RabbitMQ connection")

        # Parse connection parameters
        parameters = pika.URLParameters(rabbitmq_url)
        parameters.heartbeat = 600
        parameters.blocked_connection_timeout = 300

        _rabbitmq_connection = pika.BlockingConnection(parameters)
        _rabbitmq_channel = _rabbitmq_connection.channel()

        # Declare queues on connection
        _declare_queues(_rabbitmq_channel)

        logger.info("RabbitMQ connection created successfully")

    return _rabbitmq_channel


def _declare_queues(channel: pika.channel.Channel) -> None:
    """
    Declare all Layer 1 queues.

    Creates durable queues with quorum mode for high availability.
    Includes dead letter queue configuration for failed messages.

    Args:
        channel: The RabbitMQ channel to use for declarations
    """
    logger.info("Declaring RabbitMQ queues")

    # Deduplication queue (ingestion-api → deduplication-service)
    channel.queue_declare(
        queue='deduplication',
        durable=True,
        arguments={
            'x-queue-type': 'quorum',
            'x-max-length': 1000000,
            'x-message-ttl': 86400000,  # 24 hours
            'x-dead-letter-exchange': 'dlx.dead-letter',
            'x-dead-letter-routing-key': 'dlq.deduplication'
        }
    )

    # Processing queues (deduplication-service → Layer 2)
    for source_type in ['parcel', 'retr', 'dfi']:
        channel.queue_declare(
            queue=f'processing.{source_type}',
            durable=True,
            arguments={
                'x-queue-type': 'quorum',
                'x-message-ttl': 86400000,
                'x-dead-letter-exchange': 'dlx.dead-letter',
                'x-dead-letter-routing-key': f'dlq.processing.{source_type}'
            }
        )

    # Dead letter exchange
    channel.exchange_declare(
        exchange='dlx.dead-letter',
        exchange_type='direct',
        durable=True
    )

    # Dead letter queues
    channel.queue_declare(queue='dlq.deduplication', durable=True)
    channel.queue_bind(
        queue='dlq.deduplication',
        exchange='dlx.dead-letter',
        routing_key='dlq.deduplication'
    )

    for source_type in ['parcel', 'retr', 'dfi']:
        dlq_name = f'dlq.processing.{source_type}'
        channel.queue_declare(queue=dlq_name, durable=True)
        channel.queue_bind(
            queue=dlq_name,
            exchange='dlx.dead-letter',
            routing_key=dlq_name
        )

    logger.info("RabbitMQ queues declared successfully")


def publish_message(
    queue: str,
    message: Dict[str, Any],
    max_retries: int = 3,
    retry_delay: float = 1.0
) -> bool:
    """
    Publish a message to a RabbitMQ queue with retry logic.

    Messages are published with persistence enabled (delivery_mode=2) to
    ensure they survive broker restarts.

    Args:
        queue: The queue name to publish to
        message: The message dictionary to publish (will be JSON-encoded)
        max_retries: Maximum number of retry attempts
        retry_delay: Delay in seconds between retries

    Returns:
        bool: True if message was published successfully, False otherwise

    Example:
        ```python
        success = publish_message('deduplication', {
            'batch_id': 'uuid',
            'source_type': 'PARCEL',
            'raw_data': {...}
        })
        ```
    """
    for attempt in range(max_retries):
        try:
            channel = get_rabbitmq_connection()

            channel.basic_publish(
                exchange='',
                routing_key=queue,
                body=json.dumps(message),
                properties=pika.BasicProperties(
                    delivery_mode=2,  # Persistent
                    content_type='application/json'
                )
            )

            if attempt > 0:
                logger.info(f"Message published to {queue} after {attempt + 1} attempts")

            return True

        except Exception as e:
            logger.warning(f"Failed to publish message to {queue} (attempt {attempt + 1}/{max_retries}): {e}")

            if attempt < max_retries - 1:
                time.sleep(retry_delay * (attempt + 1))  # Exponential backoff
            else:
                logger.error(f"Failed to publish message to {queue} after {max_retries} attempts")
                return False

    return False


def close_rabbitmq_connection() -> None:
    """
    Close the RabbitMQ connection.

    Should be called on application shutdown to gracefully close the
    connection.

    Example:
        ```python
        # In FastAPI shutdown event
        @app.on_event("shutdown")
        def shutdown():
            close_rabbitmq_connection()
        ```
    """
    global _rabbitmq_connection, _rabbitmq_channel

    if _rabbitmq_channel is not None:
        try:
            _rabbitmq_channel.close()
        except Exception as e:
            logger.warning(f"Error closing RabbitMQ channel: {e}")
        _rabbitmq_channel = None

    if _rabbitmq_connection is not None:
        try:
            logger.info("Closing RabbitMQ connection")
            _rabbitmq_connection.close()
            logger.info("RabbitMQ connection closed")
        except Exception as e:
            logger.warning(f"Error closing RabbitMQ connection: {e}")
        _rabbitmq_connection = None


def check_rabbitmq_health() -> bool:
    """
    Check if the RabbitMQ connection is healthy.

    Attempts to get a connection to verify RabbitMQ is reachable.

    Returns:
        bool: True if RabbitMQ is healthy, False otherwise

    Example:
        ```python
        if check_rabbitmq_health():
            print("RabbitMQ is healthy")
        else:
            print("RabbitMQ is unhealthy")
        ```
    """
    try:
        channel = get_rabbitmq_connection()
        return channel is not None and channel.is_open
    except Exception as e:
        logger.error(f"RabbitMQ health check failed: {e}")
        return False


__all__ = [
    "get_rabbitmq_connection",
    "publish_message",
    "close_rabbitmq_connection",
    "check_rabbitmq_health",
]
