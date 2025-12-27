.PHONY: help install install-root install-shared install-ingestion install-all
.PHONY: docker-up docker-down docker-logs docker-clean
.PHONY: docker-build docker-push docker-build-all docker-push-all
.PHONY: migrate migrate-create migrate-rollback
.PHONY: test test-shared test-ingestion test-all test-cov
.PHONY: lint lint-fix format
.PHONY: run-ingestion run-dedup
.PHONY: clean clean-pyc clean-test clean-docker clean-all
.PHONY: setup dev-setup

# Colors for output
CYAN := \033[0;36m
GREEN := \033[0;32m
YELLOW := \033[0;33m
RED := \033[0;31m
NC := \033[0m # No Color

# Default target
.DEFAULT_GOAL := help

help: ## Show this help message
	@echo "$(CYAN)Wisconsin Real Estate Database - Development Commands$(NC)"
	@echo ""
	@echo "$(GREEN)Setup & Installation:$(NC)"
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "  $(CYAN)%-20s$(NC) %s\n", $$1, $$2}' | grep -E "setup|install"
	@echo ""
	@echo "$(GREEN)Docker Services:$(NC)"
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "  $(CYAN)%-20s$(NC) %s\n", $$1, $$2}' | grep "docker"
	@echo ""
	@echo "$(GREEN)Database:$(NC)"
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "  $(CYAN)%-20s$(NC) %s\n", $$1, $$2}' | grep "migrate"
	@echo ""
	@echo "$(GREEN)Development:$(NC)"
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "  $(CYAN)%-20s$(NC) %s\n", $$1, $$2}' | grep -E "run|test|lint|format"
	@echo ""
	@echo "$(GREEN)Cleanup:$(NC)"
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "  $(CYAN)%-20s$(NC) %s\n", $$1, $$2}' | grep "clean"

##@ Setup & Installation

setup: install-all docker-up migrate ## Complete first-time setup (install deps, start docker, migrate DB)
	@echo "$(GREEN)✓ Setup complete!$(NC)"
	@echo "$(YELLOW)Run 'make run-ingestion' to start the ingestion API$(NC)"

dev-setup: ## Quick setup for development (copy .env, install deps)
	@echo "$(CYAN)Setting up development environment...$(NC)"
	@if [ ! -f .env ]; then cp .env.example .env; echo "$(GREEN)✓ Created .env from .env.example$(NC)"; fi
	@$(MAKE) install-all
	@echo "$(GREEN)✓ Development setup complete!$(NC)"
	@echo "$(YELLOW)Run 'make docker-up' to start services, then 'make migrate'$(NC)"

install-all: install-root install-shared install-ingestion ## Install all dependencies (root, shared, ingestion-api)

install-root: ## Install root project dependencies
	@echo "$(CYAN)Installing root dependencies...$(NC)"
	@poetry install
	@echo "$(GREEN)✓ Root dependencies installed$(NC)"

install-shared: ## Install shared package dependencies
	@echo "$(CYAN)Installing shared package...$(NC)"
	@cd services/shared && poetry install
	@echo "$(GREEN)✓ Shared package installed$(NC)"

install-ingestion: install-shared ## Install ingestion-api dependencies
	@echo "$(CYAN)Installing ingestion-api...$(NC)"
	@cd services/ingestion-api && poetry install
	@echo "$(GREEN)✓ Ingestion API installed$(NC)"

install-dedup: install-shared ## Install deduplication-service dependencies
	@echo "$(CYAN)Installing deduplication-service...$(NC)"
	@cd services/deduplication-service && poetry install
	@echo "$(GREEN)✓ Deduplication service installed$(NC)"

##@ Docker Services

docker-up: ## Start all Docker services (TimescaleDB, RabbitMQ, Redis, Qdrant)
	@echo "$(CYAN)Starting Docker services...$(NC)"
	@docker-compose up -d
	@echo "$(GREEN)✓ Docker services started$(NC)"
	@echo "$(YELLOW)TimescaleDB: localhost:5432$(NC)"
	@echo "$(YELLOW)RabbitMQ Management: http://localhost:15672$(NC)"
	@echo "$(YELLOW)Redis: localhost:6379$(NC)"
	@echo "$(YELLOW)Qdrant: http://localhost:6333$(NC)"

docker-down: ## Stop all Docker services
	@echo "$(CYAN)Stopping Docker services...$(NC)"
	@docker-compose down
	@echo "$(GREEN)✓ Docker services stopped$(NC)"

docker-logs: ## Show Docker service logs (tail -f)
	@docker-compose logs -f

docker-logs-db: ## Show TimescaleDB logs
	@docker-compose logs -f timescaledb

docker-logs-mq: ## Show RabbitMQ logs
	@docker-compose logs -f rabbitmq

docker-clean: ## Stop and remove all Docker containers, volumes, and networks
	@echo "$(RED)WARNING: This will delete all data in Docker volumes!$(NC)"
	@read -p "Are you sure? [y/N] " -n 1 -r; \
	echo; \
	if [[ $$REPLY =~ ^[Yy]$$ ]]; then \
		docker-compose down -v; \
		echo "$(GREEN)✓ Docker cleaned$(NC)"; \
	fi

docker-restart: docker-down docker-up ## Restart all Docker services

docker-build-ingestion: ## Build ingestion-api Docker image (local arch)
	@echo "$(CYAN)Building ingestion-api image (local architecture)...$(NC)"
	@if [ -f .env ]; then \
		export $$(cat .env | grep -v '^#' | grep -v '^$$' | xargs); \
		docker build -t $${DOCKER_REGISTRY}/$${DOCKER_NAMESPACE}/ingestion-api:latest services/ingestion-api; \
	else \
		docker build -t registry.lan:30500/realestate/ingestion-api:latest services/ingestion-api; \
	fi
	@echo "$(GREEN)✓ Image built for local architecture$(NC)"

docker-build-ingestion-amd64: ## Build ingestion-api for AMD64 (for cluster deployment)
	@echo "$(CYAN)Building ingestion-api image for linux/amd64...$(NC)"
	@if [ -f .env ]; then \
		export $$(cat .env | grep -v '^#' | grep -v '^$' | xargs); \
		docker buildx build --platform linux/amd64 -t $${DOCKER_REGISTRY}/$${DOCKER_NAMESPACE}/ingestion-api:latest services/ingestion-api --load; \
	else \
		docker buildx build --platform linux/amd64 -t registry.lan:30500/realestate/ingestion-api:latest services/ingestion-api --load; \
	fi
	@echo "$(GREEN)✓ Image built for AMD64$(NC)"

docker-build-dedup: ## Build deduplication-service Docker image (local arch)
	@echo "$(CYAN)Building deduplication-service image (local architecture)...$(NC)"
	@if [ -f .env ]; then \
		export $$(cat .env | grep -v '^#' | grep -v '^$' | xargs); \
		docker build -t $${DOCKER_REGISTRY}/$${DOCKER_NAMESPACE}/deduplication-service:latest services/deduplication-service; \
	else \
		docker build -t registry.lan:30500/realestate/deduplication-service:latest services/deduplication-service; \
	fi
	@echo "$(GREEN)✓ Image built for local architecture$(NC)"

docker-build-dedup-amd64: ## Build deduplication-service for AMD64 (for cluster deployment)
	@echo "$(CYAN)Building deduplication-service image for linux/amd64...$(NC)"
	@if [ -f .env ]; then \
		export $$(cat .env | grep -v '^#' | grep -v '^$' | xargs); \
		docker buildx build --platform linux/amd64 -t $${DOCKER_REGISTRY}/$${DOCKER_NAMESPACE}/deduplication-service:latest services/deduplication-service --load; \
	else \
		docker buildx build --platform linux/amd64 -t registry.lan:30500/realestate/deduplication-service:latest services/deduplication-service --load; \
	fi
	@echo "$(GREEN)✓ Image built for AMD64$(NC)"

docker-build-all: docker-build-ingestion docker-build-dedup ## Build all service Docker images (local arch)

docker-build-all-amd64: docker-build-ingestion-amd64 docker-build-dedup-amd64 ## Build all service images for AMD64 (cluster deployment)

docker-push-ingestion: docker-build-ingestion-amd64 ## Build (AMD64) and push ingestion-api to registry
	@echo "$(CYAN)Pushing ingestion-api to registry...$(NC)"
	@if [ -f .env ]; then \
		export $$(cat .env | grep -v '^#' | grep -v '^$' | xargs); \
		docker push $${DOCKER_REGISTRY}/$${DOCKER_NAMESPACE}/ingestion-api:latest; \
	else \
		docker push registry.lan:30500/realestate/ingestion-api:latest; \
	fi
	@echo "$(GREEN)✓ Image pushed to registry$(NC)"

docker-push-dedup: docker-build-dedup-amd64 ## Build (AMD64) and push deduplication-service to registry
	@echo "$(CYAN)Pushing deduplication-service to registry...$(NC)"
	@if [ -f .env ]; then \
		export $$(cat .env | grep -v '^#' | grep -v '^$' | xargs); \
		docker push $${DOCKER_REGISTRY}/$${DOCKER_NAMESPACE}/deduplication-service:latest; \
	else \
		docker push registry.lan:30500/realestate/deduplication-service:latest; \
	fi
	@echo "$(GREEN)✓ Image pushed to registry$(NC)"

docker-push-all: docker-build-all-amd64 ## Build (AMD64) and push all service images to registry
	@echo "$(CYAN)Pushing all images to registry...$(NC)"
	@$(MAKE) docker-push-ingestion
	@$(MAKE) docker-push-dedup
	@echo "$(GREEN)✓ All images pushed$(NC)"

##@ Database

migrate: ## Run database migrations (alembic upgrade head)
	@echo "$(CYAN)Running database migrations...$(NC)"
	@poetry run alembic upgrade head
	@echo "$(GREEN)✓ Migrations applied$(NC)"

migrate-create: ## Create a new migration (usage: make migrate-create MSG="description")
	@if [ -z "$(MSG)" ]; then \
		echo "$(RED)Error: MSG is required$(NC)"; \
		echo "$(YELLOW)Usage: make migrate-create MSG='add new table'$(NC)"; \
		exit 1; \
	fi
	@echo "$(CYAN)Creating migration: $(MSG)$(NC)"
	@poetry run alembic revision -m "$(MSG)"
	@echo "$(GREEN)✓ Migration created$(NC)"

migrate-rollback: ## Rollback last migration
	@echo "$(YELLOW)Rolling back last migration...$(NC)"
	@poetry run alembic downgrade -1
	@echo "$(GREEN)✓ Migration rolled back$(NC)"

migrate-history: ## Show migration history
	@poetry run alembic history

db-shell: ## Open psql shell to TimescaleDB
	@docker-compose exec timescaledb psql -U realestate -d realestate

db-reset: docker-down ## Reset database (WARNING: deletes all data)
	@echo "$(RED)WARNING: This will delete all database data!$(NC)"
	@read -p "Are you sure? [y/N] " -n 1 -r; \
	echo; \
	if [[ $$REPLY =~ ^[Yy]$$ ]]; then \
		docker-compose down -v; \
		docker-compose up -d timescaledb; \
		sleep 5; \
		$(MAKE) migrate; \
		echo "$(GREEN)✓ Database reset complete$(NC)"; \
	fi

##@ Development

run-ingestion: ## Run ingestion-api service (FastAPI with reload)
	@echo "$(CYAN)Starting Ingestion API on http://localhost:8080$(NC)"
	@cd services/ingestion-api && poetry run uvicorn main:app --reload --host 0.0.0.0 --port 8080

run-dedup: ## Run deduplication-service
	@echo "$(CYAN)Starting Deduplication Service$(NC)"
	@cd services/deduplication-service && poetry run python main.py

##@ Testing

test: test-all ## Run all tests

test-all: test-shared test-ingestion ## Run tests for all services

test-shared: ## Run shared package tests
	@echo "$(CYAN)Running shared package tests...$(NC)"
	@cd services/shared && poetry run pytest

test-ingestion: ## Run ingestion-api tests
	@echo "$(CYAN)Running ingestion-api tests...$(NC)"
	@cd services/ingestion-api && poetry run pytest

test-dedup: ## Run deduplication-service tests
	@echo "$(CYAN)Running deduplication-service tests...$(NC)"
	@cd services/deduplication-service && poetry run pytest

test-cov: ## Run tests with coverage report
	@echo "$(CYAN)Running tests with coverage...$(NC)"
	@cd services/ingestion-api && poetry run pytest --cov --cov-report=html --cov-report=term
	@echo "$(GREEN)✓ Coverage report generated at services/ingestion-api/htmlcov/index.html$(NC)"

test-watch: ## Run tests in watch mode (requires pytest-watch)
	@cd services/ingestion-api && poetry run ptw

##@ Code Quality

lint: ## Run ruff linter on all code
	@echo "$(CYAN)Running linter...$(NC)"
	@poetry run ruff check .
	@cd services/shared && poetry run ruff check .
	@cd services/ingestion-api && poetry run ruff check .

lint-fix: ## Run ruff linter and auto-fix issues
	@echo "$(CYAN)Running linter with auto-fix...$(NC)"
	@poetry run ruff check . --fix
	@cd services/shared && poetry run ruff check . --fix
	@cd services/ingestion-api && poetry run ruff check . --fix
	@echo "$(GREEN)✓ Linting complete$(NC)"

format: lint-fix ## Format code (alias for lint-fix)

typecheck: ## Run mypy type checker
	@echo "$(CYAN)Running type checker...$(NC)"
	@poetry run mypy services/

##@ Cleanup

clean: clean-pyc clean-test ## Remove Python and test artifacts

clean-all: clean clean-docker ## Remove all build, test, and Docker artifacts

clean-pyc: ## Remove Python file artifacts
	@echo "$(CYAN)Cleaning Python artifacts...$(NC)"
	@find . -type f -name '*.py[co]' -delete
	@find . -type d -name '__pycache__' -delete
	@find . -type d -name '*.egg-info' -exec rm -rf {} +
	@find . -type d -name '*.egg' -exec rm -rf {} +
	@echo "$(GREEN)✓ Python artifacts cleaned$(NC)"

clean-test: ## Remove test and coverage artifacts
	@echo "$(CYAN)Cleaning test artifacts...$(NC)"
	@find . -type d -name '.pytest_cache' -exec rm -rf {} +
	@find . -type d -name 'htmlcov' -exec rm -rf {} +
	@find . -type f -name '.coverage' -delete
	@find . -type f -name 'coverage.xml' -delete
	@echo "$(GREEN)✓ Test artifacts cleaned$(NC)"

clean-docker: ## Remove Docker artifacts (same as docker-clean)
	@$(MAKE) docker-clean

##@ Utilities

check-env: ## Check if .env file exists
	@if [ ! -f .env ]; then \
		echo "$(RED)✗ .env file not found$(NC)"; \
		echo "$(YELLOW)Run: cp .env.example .env$(NC)"; \
		exit 1; \
	else \
		echo "$(GREEN)✓ .env file exists$(NC)"; \
	fi

health: ## Check health of all services
	@echo "$(CYAN)Checking service health...$(NC)"
	@echo -n "TimescaleDB: "
	@docker-compose exec -T timescaledb pg_isready -U realestate > /dev/null 2>&1 && echo "$(GREEN)✓$(NC)" || echo "$(RED)✗$(NC)"
	@echo -n "RabbitMQ: "
	@curl -s -u realestate:devpassword http://localhost:15672/api/overview > /dev/null 2>&1 && echo "$(GREEN)✓$(NC)" || echo "$(RED)✗$(NC)"
	@echo -n "Redis: "
	@docker-compose exec -T redis redis-cli ping > /dev/null 2>&1 && echo "$(GREEN)✓$(NC)" || echo "$(RED)✗$(NC)"
	@echo -n "Qdrant: "
	@curl -s http://localhost:6333/health > /dev/null 2>&1 && echo "$(GREEN)✓$(NC)" || echo "$(RED)✗$(NC)"

status: health ## Alias for health check

rabbitmq-ui: ## Open RabbitMQ management UI in browser
	@open http://localhost:15672 || xdg-open http://localhost:15672 || echo "Open http://localhost:15672 in your browser (user: realestate, pass: devpassword)"

qdrant-ui: ## Open Qdrant UI in browser
	@open http://localhost:6333/dashboard || xdg-open http://localhost:6333/dashboard || echo "Open http://localhost:6333/dashboard in your browser"

api-docs: ## Open API docs in browser
	@open http://localhost:8080/docs || xdg-open http://localhost:8080/docs || echo "Open http://localhost:8080/docs in your browser"
