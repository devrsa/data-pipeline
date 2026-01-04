.PHONY: help install test lint format clean build run docker-up docker-down

# Default target
help:
	@echo "Available commands:"
	@echo "  install     Install dependencies"
	@echo "  test        Run tests"
	@echo "  lint        Run linting"
	@echo "  format      Format code"
	@echo "  clean       Clean cache files"
	@echo "  build       Build Docker image"
	@echo "  run         Run pipeline"
	@echo "  docker-up   Start services with Docker Compose"
	@echo "  docker-down Stop services with Docker Compose"
	@echo "  sample-data Generate sample data"

# Install dependencies
install:
	pip install -r requirements.txt
	pip install -e .

# Run tests
test:
	pytest

# Run tests with coverage
test-cov:
	pytest --cov=src --cov-report=html

# Run linting
lint:
	flake8 src/ tests/
	black --check src/ tests/
	isort --check-only src/ tests/

# Format code
format:
	black src/ tests/
	isort src/ tests/

# Clean cache files
clean:
	find . -type f -name "*.pyc" -delete
	find . -type d -name "__pycache__" -delete
	find . -type d -name "*.egg-info" -exec rm -rf {} +
	rm -rf .coverage htmlcov/ .pytest_cache/

# Build Docker image
build:
	docker build -t data-engineering-pipeline .

# Run pipeline locally
run:
	python -m src.etl_pipeline

# Run streaming pipeline
run-streaming:
	python -m src.streaming_pipeline

# Run monitoring
run-monitoring:
	python -m src.monitoring

# Start services with Docker Compose
docker-up:
	docker-compose up -d

# Stop services with Docker Compose
docker-down:
	docker-compose down

# View Docker logs
docker-logs:
	docker-compose logs -f

# Generate sample data
sample-data:
	python create_sample_data.py

# Run data validation
validate:
	python -m src.data_validator

# Setup development environment
setup-dev:
	cp .env.example .env
	pip install -r requirements.txt
	pip install pytest pytest-cov black flake8 isort pre-commit
	pre-commit install

# Run all quality checks
quality: lint test

# Deploy to production (placeholder)
deploy:
	@echo "Deployment command not implemented yet"

# Backup database
backup-db:
	@echo "Database backup command not implemented yet"

# Restore database
restore-db:
	@echo "Database restore command not implemented yet"