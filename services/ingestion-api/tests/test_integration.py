"""
Integration tests for ingestion API.

Tests full upload flows with real docker-compose services (PostgreSQL, RabbitMQ).

These tests require:
- Docker Compose running (make start-services)
- Database migrated (make migrate)

Run with: pytest tests/test_integration.py --integration
"""

import pytest
import asyncio
from pathlib import Path
from uuid import UUID
from httpx import AsyncClient
import asyncpg
import pika

from main import app
from config import Settings

# Mark all tests in this module as integration tests
pytestmark = pytest.mark.integration

settings = Settings()


@pytest.fixture
async def db_conn():
    """Create database connection for verification."""
    conn = await asyncpg.connect(settings.DATABASE_URL)
    yield conn
    await conn.close()


@pytest.fixture
def rabbitmq_channel():
    """Create RabbitMQ channel for verification."""
    connection = pika.BlockingConnection(
        pika.URLParameters(settings.RABBITMQ_URL)
    )
    channel = connection.channel()
    yield channel
    channel.close()
    connection.close()


@pytest.fixture(autouse=True)
async def cleanup_test_data(db_conn):
    """Clean up test data before each test."""
    # Delete test batches
    await db_conn.execute(
        "DELETE FROM import_batches WHERE source_name LIKE 'Test %'"
    )
    yield
    # Cleanup after test
    await db_conn.execute(
        "DELETE FROM import_batches WHERE source_name LIKE 'Test %'"
    )


class TestCSVUploadIntegration:
    """Integration tests for CSV upload flow."""

    @pytest.mark.asyncio
    async def test_csv_upload_full_flow(self, db_conn, rabbitmq_channel):
        """Test complete CSV upload flow with real services."""
        # Prepare test file
        csv_path = Path(__file__).parent / "fixtures" / "sample_retr.csv"
        assert csv_path.exists(), f"Test fixture not found: {csv_path}"

        # Upload CSV
        async with AsyncClient(app=app, base_url="http://test") as client:
            with open(csv_path, "rb") as f:
                response = await client.post(
                    "/api/v1/ingest/retr",
                    files={"file": ("sample_retr.csv", f, "text/csv")},
                    data={"source_name": "Test RETR Integration"}
                )

        # Verify HTTP response
        assert response.status_code == 202
        data = response.json()
        assert "batch_id" in data
        assert data["status"] == "processing"
        assert data["source_type"] == "RETR"

        batch_id = UUID(data["batch_id"])

        # Verify batch record in database
        batch = await db_conn.fetchrow(
            "SELECT * FROM import_batches WHERE batch_id = $1",
            batch_id
        )
        assert batch is not None
        assert batch["source_name"] == "Test RETR Integration"
        assert batch["source_type"] == "RETR"
        assert batch["file_format"] == "CSV"
        assert batch["status"] in ["processing", "completed"]

        # Wait for processing to complete (max 5 seconds)
        for _ in range(10):
            await asyncio.sleep(0.5)
            batch = await db_conn.fetchrow(
                "SELECT * FROM import_batches WHERE batch_id = $1",
                batch_id
            )
            if batch["status"] == "completed":
                break

        # Verify final status
        assert batch["status"] == "completed"
        assert batch["processed_records"] > 0
        assert batch["completed_at"] is not None

        # Verify messages in RabbitMQ
        queue_info = rabbitmq_channel.queue_declare(
            queue="deduplication",
            passive=True
        )
        message_count = queue_info.method.message_count
        assert message_count >= batch["processed_records"], \
            f"Expected at least {batch['processed_records']} messages, got {message_count}"

    @pytest.mark.asyncio
    async def test_csv_status_endpoint(self, db_conn):
        """Test status endpoint during CSV processing."""
        csv_path = Path(__file__).parent / "fixtures" / "sample_retr.csv"

        # Upload CSV
        async with AsyncClient(app=app, base_url="http://test") as client:
            with open(csv_path, "rb") as f:
                response = await client.post(
                    "/api/v1/ingest/retr",
                    files={"file": ("sample_retr.csv", f, "text/csv")},
                    data={"source_name": "Test RETR Status"}
                )

            batch_id = response.json()["batch_id"]

            # Query status immediately
            status_response = await client.get(f"/api/v1/ingest/status/{batch_id}")
            assert status_response.status_code == 200

            status_data = status_response.json()
            assert status_data["batch_id"] == batch_id
            assert status_data["source_name"] == "Test RETR Status"
            assert status_data["status"] in ["processing", "completed"]

            # Wait and check final status
            await asyncio.sleep(2)
            status_response = await client.get(f"/api/v1/ingest/status/{batch_id}")
            final_status = status_response.json()
            assert final_status["status"] == "completed"
            assert final_status["progress"] == 100.0


class TestGDBUploadIntegration:
    """Integration tests for GDB upload flow."""

    @pytest.mark.asyncio
    async def test_gdb_upload_full_flow(self, db_conn, rabbitmq_channel):
        """Test complete GDB upload flow with real services."""
        # Prepare test file
        gdb_path = Path(__file__).parent / "fixtures" / "test_parcels.gdb.zip"
        assert gdb_path.exists(), f"Test fixture not found: {gdb_path}"

        # Upload GDB
        async with AsyncClient(app=app, base_url="http://test") as client:
            with open(gdb_path, "rb") as f:
                response = await client.post(
                    "/api/v1/ingest/parcel/gdb",
                    files={"file": ("test_parcels.gdb.zip", f, "application/zip")},
                    data={
                        "source_name": "Test Parcels Integration",
                        "layer_name": "V11_Parcels"
                    }
                )

        # Verify HTTP response
        assert response.status_code == 202
        data = response.json()
        assert "batch_id" in data
        assert data["status"] == "processing"
        assert data["source_type"] == "PARCEL"
        assert data["total_records"] == 75

        batch_id = UUID(data["batch_id"])

        # Verify batch record in database
        batch = await db_conn.fetchrow(
            "SELECT * FROM import_batches WHERE batch_id = $1",
            batch_id
        )
        assert batch is not None
        assert batch["source_name"] == "Test Parcels Integration"
        assert batch["source_type"] == "PARCEL"
        assert batch["file_format"] == "GDB"
        assert batch["total_records"] == 75

        # Wait for processing to complete
        for _ in range(20):
            await asyncio.sleep(0.5)
            batch = await db_conn.fetchrow(
                "SELECT * FROM import_batches WHERE batch_id = $1",
                batch_id
            )
            if batch["status"] == "completed":
                break

        # Verify final status
        assert batch["status"] == "completed"
        assert batch["processed_records"] == 75
        assert batch["completed_at"] is not None

        # Verify messages in RabbitMQ
        queue_info = rabbitmq_channel.queue_declare(
            queue="deduplication",
            passive=True
        )
        message_count = queue_info.method.message_count
        assert message_count >= 75, \
            f"Expected at least 75 messages, got {message_count}"

    @pytest.mark.asyncio
    async def test_gdb_stays_responsive_during_processing(self, db_conn):
        """Test that API stays responsive while processing GDB."""
        gdb_path = Path(__file__).parent / "fixtures" / "test_parcels.gdb.zip"

        async with AsyncClient(app=app, base_url="http://test") as client:
            # Start GDB upload
            with open(gdb_path, "rb") as f:
                upload_response = await client.post(
                    "/api/v1/ingest/parcel/gdb",
                    files={"file": ("test_parcels.gdb.zip", f, "application/zip")},
                    data={"source_name": "Test Responsive"}
                )

            batch_id = upload_response.json()["batch_id"]

            # Immediately try to query status (should not block)
            status_response = await client.get(f"/api/v1/ingest/status/{batch_id}")
            assert status_response.status_code == 200

            # Verify health endpoint still works
            health_response = await client.get("/health")
            assert health_response.status_code == 200


class TestErrorHandlingIntegration:
    """Integration tests for error handling."""

    @pytest.mark.asyncio
    async def test_invalid_csv_fails_gracefully(self, db_conn):
        """Test that invalid CSV files fail gracefully."""
        # Create invalid CSV
        invalid_csv = Path("/tmp/test_invalid.csv")
        invalid_csv.write_text("not,valid,csv\nwith,missing,columns\n")

        async with AsyncClient(app=app, base_url="http://test") as client:
            with open(invalid_csv, "rb") as f:
                response = await client.post(
                    "/api/v1/ingest/retr",
                    files={"file": ("invalid.csv", f, "text/csv")},
                    data={"source_name": "Test Invalid CSV"}
                )

        # Should still return 202 (async processing)
        assert response.status_code == 202
        batch_id = response.json()["batch_id"]

        # Wait for processing
        await asyncio.sleep(2)

        # Check batch status - should be failed or completed with errors
        batch = await db_conn.fetchrow(
            "SELECT * FROM import_batches WHERE batch_id = $1",
            UUID(batch_id)
        )
        # Either failed or completed with very few processed records
        assert batch["status"] in ["failed", "completed"]

        invalid_csv.unlink()

    @pytest.mark.asyncio
    async def test_oversized_file_rejected(self):
        """Test that oversized files are rejected immediately."""
        # Create a large file (> max size)
        large_file = Path("/tmp/test_large.csv")
        large_file.write_bytes(b"x" * (settings.max_upload_size_bytes + 1))

        async with AsyncClient(app=app, base_url="http://test") as client:
            with open(large_file, "rb") as f:
                response = await client.post(
                    "/api/v1/ingest/retr",
                    files={"file": ("large.csv", f, "text/csv")},
                    data={"source_name": "Test Large"}
                )

        # Should be rejected with 413
        assert response.status_code == 413
        assert "FileTooLarge" in response.json()["detail"]["error"]

        large_file.unlink()
