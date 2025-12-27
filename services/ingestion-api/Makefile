.PHONY: help install test test-unit test-integration test-cov clean lint format run dev migrate migrate-down db-console rabbitmq-console docker-build docker-run

# Default target
help:
	@echo "📦 Ingestion API - Available Commands"
	@echo ""
	@echo "🔧 Setup & Installation:"
	@echo "  make install          Install dependencies with Poetry"
	@echo "  make install-dev      Install with dev dependencies"
	@echo ""
	@echo "🧪 Testing:"
	@echo "  make test             Run all tests (unit + integration)"
	@echo "  make test-unit        Run unit tests only (fast, no Docker)"
	@echo "  make test-integration Run integration tests only (requires Docker)"
	@echo "  make test-cov         Run tests with coverage report"
	@echo "  make test-watch       Run tests in watch mode"
	@echo ""
	@echo "🐳 Services:"
	@echo "  make services-up      Start all docker-compose services"
	@echo "  make services-down    Stop all docker-compose services"
	@echo "  make services-logs    View docker-compose logs"
	@echo "  make services-clean   Stop services and remove volumes"
	@echo ""
	@echo "🗄️  Database:"
	@echo "  make migrate          Run database migrations (upgrade)"
	@echo "  make migrate-down     Rollback last migration"
	@echo "  make migrate-create   Create new migration (MSG='description')"
	@echo "  make db-console       Open PostgreSQL console"
	@echo "  make db-reset         Reset database (drop + recreate + migrate)"
	@echo ""
	@echo "🐰 RabbitMQ:"
	@echo "  make rabbitmq-console Open RabbitMQ management console"
	@echo "  make rabbitmq-queues  List RabbitMQ queues"
	@echo "  make rabbitmq-purge   Purge all queues"
	@echo ""
	@echo "🚀 Development:"
	@echo "  make dev              Start API in development mode (hot reload)"
	@echo "  make run              Start API in production mode"
	@echo "  make lint             Check code with ruff"
	@echo "  make format           Format code with ruff"
	@echo "  make clean            Remove temporary files and caches"
	@echo ""
	@echo "🐋 Docker:"
	@echo "  make docker-build     Build Docker image"
	@echo "  make docker-run       Run Docker container"
	@echo "  make docker-push      Push image to registry"
	@echo ""

# Installation
install:
	poetry install

install-dev:
	poetry install --with dev

# Testing
test:
	poetry run pytest -v

test-unit:
	@echo "🧪 Running unit tests (no docker required)..."
	poetry run pytest -m "not integration" -v

test-integration:
	@echo "🧪 Running integration tests (requires docker-compose)..."
	@echo "⚠️  Make sure services are running: make services-up"
	poetry run pytest -m integration -v

test-cov:
	@echo "📊 Running tests with coverage..."
	poetry run pytest -m "not integration" \
		--cov=services --cov=routers --cov=models --cov=config \
		--cov-report=term-missing \
		--cov-report=html
	@echo "✅ Coverage report: open htmlcov/index.html"

test-watch:
	poetry run pytest-watch -m "not integration"

# Services
services-up:
	@echo "🐳 Starting docker-compose services..."
	cd ../../ && docker-compose up -d postgres rabbitmq redis
	@echo "⏳ Waiting for services to be ready..."
	@sleep 5
	@echo "✅ Services running. Check with: docker-compose ps"

services-down:
	@echo "🛑 Stopping docker-compose services..."
	cd ../../ && docker-compose down

services-logs:
	cd ../../ && docker-compose logs -f

services-clean:
	@echo "🧹 Stopping services and removing volumes..."
	cd ../../ && docker-compose down -v

# Database
migrate:
	@echo "🗄️  Running database migrations..."
	cd ../../ && poetry run alembic upgrade head

migrate-down:
	@echo "⏪ Rolling back last migration..."
	cd ../../ && poetry run alembic downgrade -1

migrate-create:
	@if [ -z "$(MSG)" ]; then \
		echo "❌ Error: MSG is required. Usage: make migrate-create MSG='description'"; \
		exit 1; \
	fi
	@echo "📝 Creating new migration: $(MSG)"
	cd ../../ && poetry run alembic revision --autogenerate -m "$(MSG)"

db-console:
	@echo "🗄️  Connecting to PostgreSQL..."
	docker exec -it postgres psql -U genprop -d genprop

db-reset:
	@echo "⚠️  Resetting database (all data will be lost)..."
	@read -p "Are you sure? [y/N] " -n 1 -r; \
	echo; \
	if [[ $$REPLY =~ ^[Yy]$$ ]]; then \
		docker exec -it postgres psql -U genprop -c "DROP DATABASE IF EXISTS genprop;"; \
		docker exec -it postgres psql -U genprop -c "CREATE DATABASE genprop;"; \
		cd ../../ && poetry run alembic upgrade head; \
		echo "✅ Database reset complete"; \
	fi

# RabbitMQ
rabbitmq-console:
	@echo "🐰 Opening RabbitMQ management console..."
	@echo "URL: http://localhost:15672"
	@echo "User: guest / Password: guest"
	open http://localhost:15672 || xdg-open http://localhost:15672

rabbitmq-queues:
	@echo "📋 RabbitMQ Queues:"
	docker exec -it rabbitmq rabbitmqctl list_queues name messages consumers

rabbitmq-purge:
	@echo "🧹 Purging RabbitMQ queues..."
	docker exec -it rabbitmq rabbitmqctl purge_queue deduplication
	docker exec -it rabbitmq rabbitmqctl purge_queue processing.parcel
	docker exec -it rabbitmq rabbitmqctl purge_queue processing.retr

# Development
dev:
	@echo "🚀 Starting API in development mode..."
	@echo "📍 API: http://localhost:8080"
	@echo "📚 Docs: http://localhost:8080/docs"
	poetry run fastapi dev main.py --host 0.0.0.0 --port 8080

run:
	@echo "🚀 Starting API in production mode..."
	poetry run uvicorn main:app --host 0.0.0.0 --port 8080

lint:
	@echo "🔍 Checking code with ruff..."
	poetry run ruff check .

format:
	@echo "✨ Formatting code with ruff..."
	poetry run ruff check . --fix
	poetry run ruff format .

clean:
	@echo "🧹 Cleaning temporary files..."
	find . -type d -name "__pycache__" -exec rm -rf {} + 2>/dev/null || true
	find . -type d -name ".pytest_cache" -exec rm -rf {} + 2>/dev/null || true
	find . -type d -name ".ruff_cache" -exec rm -rf {} + 2>/dev/null || true
	find . -type d -name "htmlcov" -exec rm -rf {} + 2>/dev/null || true
	find . -type f -name "*.pyc" -delete 2>/dev/null || true
	find . -type f -name ".coverage" -delete 2>/dev/null || true
	rm -rf /tmp/gdb-processing/* 2>/dev/null || true
	@echo "✅ Cleanup complete"

# Docker
docker-build:
	@echo "🐋 Building Docker image..."
	docker build -t realestate/ingestion-api:latest .

docker-run:
	@echo "🐋 Running Docker container..."
	docker run -d \
		--name ingestion-api \
		-p 8080:8080 \
		--network genprop_default \
		-e DATABASE_URL=postgresql://genprop:devpassword@postgres:5432/genprop \
		-e RABBITMQ_URL=amqp://guest:guest@rabbitmq:5672/ \
		realestate/ingestion-api:latest

docker-push:
	@echo "📤 Pushing Docker image..."
	docker push realestate/ingestion-api:latest

# Quick start workflow
quickstart: services-up migrate install test-unit
	@echo ""
	@echo "✅ Quickstart complete!"
	@echo "Next steps:"
	@echo "  1. Start API: make dev"
	@echo "  2. Open docs: http://localhost:8080/docs"
	@echo "  3. Run tests: make test"
