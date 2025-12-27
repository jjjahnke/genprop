"""
Unit tests for RabbitMQ connection utilities.

Tests RabbitMQ connection management and message publishing:
- get_rabbitmq_connection() singleton pattern
- publish_message() with retry logic
- close_rabbitmq_connection() cleanup
- check_rabbitmq_health() health checks
- Queue declarations
"""

import pytest
import pika
import json
from unittest.mock import MagicMock, patch, call

from shared.rabbitmq import (
    get_rabbitmq_connection,
    publish_message,
    close_rabbitmq_connection,
    check_rabbitmq_health
)


@pytest.fixture(autouse=True)
def reset_connection():
    """Reset global RabbitMQ connection before each test."""
    import shared.rabbitmq
    shared.rabbitmq._rabbitmq_connection = None
    shared.rabbitmq._rabbitmq_channel = None
    yield
    shared.rabbitmq._rabbitmq_connection = None
    shared.rabbitmq._rabbitmq_channel = None


class TestGetRabbitmqConnection:
    """Tests for get_rabbitmq_connection() function."""

    def test_creates_connection_on_first_call(self):
        """Test that connection is created on first call."""
        mock_connection = MagicMock(spec=pika.BlockingConnection)
        mock_connection.is_closed = False
        mock_channel = MagicMock(spec=pika.channel.Channel)
        mock_connection.channel.return_value = mock_channel

        with patch('pika.BlockingConnection', return_value=mock_connection) as mock_conn_cls, \
             patch('pika.URLParameters') as mock_params:

            channel = get_rabbitmq_connection()

            assert channel is mock_channel
            mock_conn_cls.assert_called_once()
            mock_params.assert_called_once()

    def test_returns_same_channel_on_subsequent_calls(self):
        """Test singleton pattern - same channel returned on multiple calls."""
        mock_connection = MagicMock(spec=pika.BlockingConnection)
        mock_connection.is_closed = False
        mock_channel = MagicMock(spec=pika.channel.Channel)
        mock_connection.channel.return_value = mock_channel

        with patch('pika.BlockingConnection', return_value=mock_connection) as mock_conn_cls:
            channel1 = get_rabbitmq_connection()
            channel2 = get_rabbitmq_connection()
            channel3 = get_rabbitmq_connection()

            assert channel1 is channel2
            assert channel2 is channel3
            mock_conn_cls.assert_called_once()  # Connection created only once

    def test_uses_environment_variable_for_url(self):
        """Test that RABBITMQ_URL environment variable is used."""
        mock_connection = MagicMock(spec=pika.BlockingConnection)
        mock_connection.is_closed = False
        mock_channel = MagicMock()
        mock_connection.channel.return_value = mock_channel

        env_vars = {'RABBITMQ_URL': 'amqp://testuser:testpass@testhost:5672/'}

        with patch('pika.BlockingConnection', return_value=mock_connection), \
             patch('pika.URLParameters') as mock_params, \
             patch.dict('os.environ', env_vars):

            get_rabbitmq_connection()

            # Verify URL was passed to URLParameters
            mock_params.assert_called_once_with('amqp://testuser:testpass@testhost:5672/')

    def test_uses_default_url_when_env_missing(self):
        """Test default RABBITMQ_URL when environment variable not set."""
        mock_connection = MagicMock(spec=pika.BlockingConnection)
        mock_connection.is_closed = False
        mock_channel = MagicMock()
        mock_connection.channel.return_value = mock_channel

        with patch('pika.BlockingConnection', return_value=mock_connection), \
             patch('pika.URLParameters') as mock_params, \
             patch.dict('os.environ', {}, clear=True):

            get_rabbitmq_connection()

            # Verify default URL is used
            call_args = mock_params.call_args[0][0]
            assert 'amqp://' in call_args
            assert 'localhost:5672' in call_args

    def test_sets_heartbeat_and_timeout(self):
        """Test that connection parameters include heartbeat and timeout."""
        mock_connection = MagicMock(spec=pika.BlockingConnection)
        mock_connection.is_closed = False
        mock_channel = MagicMock()
        mock_connection.channel.return_value = mock_channel
        mock_params = MagicMock()

        with patch('pika.BlockingConnection', return_value=mock_connection), \
             patch('pika.URLParameters', return_value=mock_params):

            get_rabbitmq_connection()

            # Verify heartbeat and timeout are set
            assert mock_params.heartbeat == 600
            assert mock_params.blocked_connection_timeout == 300

    def test_declares_queues_on_connection(self):
        """Test that queues are declared when connection is created."""
        mock_connection = MagicMock(spec=pika.BlockingConnection)
        mock_connection.is_closed = False
        mock_channel = MagicMock()
        mock_connection.channel.return_value = mock_channel

        with patch('pika.BlockingConnection', return_value=mock_connection):
            get_rabbitmq_connection()

            # Verify queue_declare was called multiple times
            assert mock_channel.queue_declare.call_count > 0
            # Verify deduplication queue was declared
            calls = [call.kwargs for call in mock_channel.queue_declare.call_args_list]
            queue_names = [c.get('queue') for c in calls]
            assert 'deduplication' in queue_names

    def test_recreates_connection_when_closed(self):
        """Test that connection is recreated if previous one is closed."""
        mock_connection1 = MagicMock(spec=pika.BlockingConnection)
        mock_connection1.is_closed = True  # First connection is closed
        mock_connection2 = MagicMock(spec=pika.BlockingConnection)
        mock_connection2.is_closed = False
        mock_channel = MagicMock()
        mock_connection2.channel.return_value = mock_channel

        import shared.rabbitmq
        shared.rabbitmq._rabbitmq_connection = mock_connection1

        with patch('pika.BlockingConnection', return_value=mock_connection2) as mock_conn_cls:
            channel = get_rabbitmq_connection()

            assert channel is mock_channel
            mock_conn_cls.assert_called_once()  # New connection created


class TestPublishMessage:
    """Tests for publish_message() function."""

    def test_publishes_message_successfully(self):
        """Test successful message publishing."""
        mock_channel = MagicMock()
        mock_connection = MagicMock(spec=pika.BlockingConnection)
        mock_connection.is_closed = False
        mock_connection.channel.return_value = mock_channel

        message = {'batch_id': 'test-123', 'data': {'field': 'value'}}

        with patch('pika.BlockingConnection', return_value=mock_connection):
            result = publish_message('test-queue', message)

            assert result is True
            mock_channel.basic_publish.assert_called_once()

    def test_message_serialized_to_json(self):
        """Test that message is JSON-serialized before publishing."""
        mock_channel = MagicMock()
        mock_connection = MagicMock(spec=pika.BlockingConnection)
        mock_connection.is_closed = False
        mock_connection.channel.return_value = mock_channel

        message = {'batch_id': 'test-123', 'count': 42}

        with patch('pika.BlockingConnection', return_value=mock_connection):
            publish_message('test-queue', message)

            call_args = mock_channel.basic_publish.call_args
            body = call_args.kwargs['body']
            # Body should be JSON string
            assert isinstance(body, str)
            assert json.loads(body) == message

    def test_uses_correct_routing_key(self):
        """Test that routing key matches the queue name."""
        mock_channel = MagicMock()
        mock_connection = MagicMock(spec=pika.BlockingConnection)
        mock_connection.is_closed = False
        mock_connection.channel.return_value = mock_channel

        with patch('pika.BlockingConnection', return_value=mock_connection):
            publish_message('deduplication', {'test': 'data'})

            call_args = mock_channel.basic_publish.call_args
            assert call_args.kwargs['routing_key'] == 'deduplication'
            assert call_args.kwargs['exchange'] == ''

    def test_message_is_persistent(self):
        """Test that messages are published with persistence."""
        mock_channel = MagicMock()
        mock_connection = MagicMock(spec=pika.BlockingConnection)
        mock_connection.is_closed = False
        mock_connection.channel.return_value = mock_channel

        with patch('pika.BlockingConnection', return_value=mock_connection):
            publish_message('test-queue', {'test': 'data'})

            call_args = mock_channel.basic_publish.call_args
            properties = call_args.kwargs['properties']
            assert properties.delivery_mode == 2  # Persistent
            assert properties.content_type == 'application/json'

    def test_retries_on_failure(self):
        """Test that publish retries on failure."""
        mock_channel = MagicMock()
        mock_channel.basic_publish.side_effect = [
            Exception("Connection lost"),
            Exception("Still failing"),
            None  # Success on third try
        ]

        mock_connection = MagicMock(spec=pika.BlockingConnection)
        mock_connection.is_closed = False
        mock_connection.channel.return_value = mock_channel

        with patch('pika.BlockingConnection', return_value=mock_connection), \
             patch('time.sleep'):  # Don't actually sleep in tests

            result = publish_message('test-queue', {'test': 'data'}, max_retries=3)

            assert result is True
            assert mock_channel.basic_publish.call_count == 3

    def test_returns_false_after_max_retries(self):
        """Test that publish returns False after all retries fail."""
        mock_channel = MagicMock()
        mock_channel.basic_publish.side_effect = Exception("Always fails")

        mock_connection = MagicMock(spec=pika.BlockingConnection)
        mock_connection.is_closed = False
        mock_connection.channel.return_value = mock_channel

        with patch('pika.BlockingConnection', return_value=mock_connection), \
             patch('time.sleep'):

            result = publish_message('test-queue', {'test': 'data'}, max_retries=3)

            assert result is False
            assert mock_channel.basic_publish.call_count == 3

    def test_exponential_backoff(self):
        """Test that retry delay uses exponential backoff."""
        mock_channel = MagicMock()
        mock_channel.basic_publish.side_effect = [
            Exception("Fail 1"),
            Exception("Fail 2"),
            None
        ]

        mock_connection = MagicMock(spec=pika.BlockingConnection)
        mock_connection.is_closed = False
        mock_connection.channel.return_value = mock_channel

        with patch('pika.BlockingConnection', return_value=mock_connection), \
             patch('time.sleep') as mock_sleep:

            publish_message('test-queue', {'test': 'data'}, max_retries=3, retry_delay=1.0)

            # Verify exponential backoff: 1.0, 2.0
            assert mock_sleep.call_count == 2
            assert mock_sleep.call_args_list[0][0][0] == 1.0  # First retry: 1.0 * 1
            assert mock_sleep.call_args_list[1][0][0] == 2.0  # Second retry: 1.0 * 2


class TestCloseRabbitmqConnection:
    """Tests for close_rabbitmq_connection() function."""

    def test_closes_channel_and_connection(self):
        """Test that both channel and connection are closed."""
        mock_channel = MagicMock()
        mock_connection = MagicMock()

        import shared.rabbitmq
        shared.rabbitmq._rabbitmq_channel = mock_channel
        shared.rabbitmq._rabbitmq_connection = mock_connection

        close_rabbitmq_connection()

        mock_channel.close.assert_called_once()
        mock_connection.close.assert_called_once()

    def test_handles_no_connection_gracefully(self):
        """Test that closing with no connection doesn't error."""
        # Should not raise an exception
        close_rabbitmq_connection()

    def test_handles_close_errors_gracefully(self):
        """Test that errors during close are handled."""
        mock_channel = MagicMock()
        mock_channel.close.side_effect = Exception("Close failed")
        mock_connection = MagicMock()

        import shared.rabbitmq
        shared.rabbitmq._rabbitmq_channel = mock_channel
        shared.rabbitmq._rabbitmq_connection = mock_connection

        # Should not raise exception
        close_rabbitmq_connection()

    def test_resets_globals_to_none(self):
        """Test that global connection variables are reset."""
        import shared.rabbitmq

        mock_channel = MagicMock()
        mock_connection = MagicMock()
        shared.rabbitmq._rabbitmq_channel = mock_channel
        shared.rabbitmq._rabbitmq_connection = mock_connection

        close_rabbitmq_connection()

        assert shared.rabbitmq._rabbitmq_channel is None
        assert shared.rabbitmq._rabbitmq_connection is None


class TestCheckRabbitmqHealth:
    """Tests for check_rabbitmq_health() function."""

    def test_returns_true_when_connection_healthy(self):
        """Test health check returns True when RabbitMQ is accessible."""
        mock_channel = MagicMock()
        mock_channel.is_open = True
        mock_connection = MagicMock(spec=pika.BlockingConnection)
        mock_connection.is_closed = False
        mock_connection.channel.return_value = mock_channel

        with patch('pika.BlockingConnection', return_value=mock_connection):
            result = check_rabbitmq_health()

            assert result is True

    def test_returns_false_on_connection_error(self):
        """Test health check returns False when connection fails."""
        with patch('pika.BlockingConnection', side_effect=Exception("Connection failed")):
            result = check_rabbitmq_health()

            assert result is False

    def test_returns_false_when_channel_closed(self):
        """Test health check returns False when channel is closed."""
        mock_channel = MagicMock()
        mock_channel.is_open = False
        mock_connection = MagicMock(spec=pika.BlockingConnection)
        mock_connection.is_closed = False
        mock_connection.channel.return_value = mock_channel

        with patch('pika.BlockingConnection', return_value=mock_connection):
            result = check_rabbitmq_health()

            assert result is False


class TestQueueDeclarations:
    """Tests for queue declaration logic."""

    def test_declares_deduplication_queue(self):
        """Test that deduplication queue is declared with correct settings."""
        mock_channel = MagicMock()
        mock_connection = MagicMock(spec=pika.BlockingConnection)
        mock_connection.is_closed = False
        mock_connection.channel.return_value = mock_channel

        with patch('pika.BlockingConnection', return_value=mock_connection):
            get_rabbitmq_connection()

            # Find the deduplication queue declaration
            calls = mock_channel.queue_declare.call_args_list
            dedup_call = next(c for c in calls if c.kwargs.get('queue') == 'deduplication')

            assert dedup_call.kwargs['durable'] is True
            assert dedup_call.kwargs['arguments']['x-queue-type'] == 'quorum'
            assert 'x-max-length' in dedup_call.kwargs['arguments']

    def test_declares_processing_queues(self):
        """Test that processing queues are declared for each source type."""
        mock_channel = MagicMock()
        mock_connection = MagicMock(spec=pika.BlockingConnection)
        mock_connection.is_closed = False
        mock_connection.channel.return_value = mock_channel

        with patch('pika.BlockingConnection', return_value=mock_connection):
            get_rabbitmq_connection()

            calls = mock_channel.queue_declare.call_args_list
            queue_names = [c.kwargs.get('queue') for c in calls]

            assert 'processing.parcel' in queue_names
            assert 'processing.retr' in queue_names
            assert 'processing.dfi' in queue_names

    def test_declares_dead_letter_exchange(self):
        """Test that dead letter exchange is declared."""
        mock_channel = MagicMock()
        mock_connection = MagicMock(spec=pika.BlockingConnection)
        mock_connection.is_closed = False
        mock_connection.channel.return_value = mock_channel

        with patch('pika.BlockingConnection', return_value=mock_connection):
            get_rabbitmq_connection()

            # Verify dead letter exchange declaration
            mock_channel.exchange_declare.assert_called()
            calls = mock_channel.exchange_declare.call_args_list
            dlx_call = next(c for c in calls if c.kwargs.get('exchange') == 'dlx.dead-letter')

            assert dlx_call.kwargs['exchange_type'] == 'direct'
            assert dlx_call.kwargs['durable'] is True

    def test_declares_dead_letter_queues(self):
        """Test that dead letter queues are declared and bound."""
        mock_channel = MagicMock()
        mock_connection = MagicMock(spec=pika.BlockingConnection)
        mock_connection.is_closed = False
        mock_connection.channel.return_value = mock_channel

        with patch('pika.BlockingConnection', return_value=mock_connection):
            get_rabbitmq_connection()

            # Verify DLQ declarations
            queue_calls = mock_channel.queue_declare.call_args_list
            queue_names = [c.kwargs.get('queue') for c in queue_calls]

            assert 'dlq.deduplication' in queue_names
            assert 'dlq.processing.parcel' in queue_names
            assert 'dlq.processing.retr' in queue_names
            assert 'dlq.processing.dfi' in queue_names

            # Verify queue bindings
            assert mock_channel.queue_bind.call_count > 0
