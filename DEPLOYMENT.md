# Deployment Guide

## Overview

This guide provides comprehensive instructions for deploying the Data Engineering Pipelines project across different environments. The project supports multiple deployment strategies from local development to production-grade cloud deployments.

## Prerequisites

### System Requirements

**Minimum Requirements:**
- CPU: 4 cores
- Memory: 8GB RAM
- Storage: 50GB available space
- OS: Linux (Ubuntu 20.04+), macOS, or Windows 10+ with WSL2

**Recommended Requirements:**
- CPU: 8+ cores
- Memory: 16GB+ RAM
- Storage: 100GB+ SSD
- Network: 1Gbps connection

### Software Dependencies

**Required Software:**
- Docker 20.10+ and Docker Compose 2.0+
- Python 3.11+
- Git 2.30+
- Make (for Makefile commands)

**Optional Tools:**
- Kubernetes (for cloud deployment)
- kubectl and helm
- AWS CLI/GCP gcloud (for cloud deployments)
- Node.js 16+ (for monitoring dashboards)

## Quick Start

### 1. Clone Repository

```bash
git clone https://github.com/your-org/data-engineering-pipelines.git
cd data-engineering-pipelines
```

### 2. Environment Setup

```bash
# Copy environment template
cp .env.example .env

# Edit environment variables
nano .env  # or your preferred editor
```

### 3. Development Deployment

```bash
# Using Docker Compose (recommended)
make docker-up

# Or using local Python
make install
make run
```

### 4. Verify Deployment

```bash
# Check service health
make docker-logs

# Run tests
make test

# Access services
# - Airflow: http://localhost:8080
# - PostgreSQL: localhost:5432
# - Redis: localhost:6379
# - Kafka: localhost:9092
```

## Deployment Environments

### Development Environment

**Purpose**: Local development and testing
**Infrastructure**: Docker Compose on local machine

**Setup Steps:**

1. **Initialize Environment**
```bash
# Create project directory structure
mkdir -p logs data/backups data/tmp

# Set up Python virtual environment
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate

# Install dependencies
make install
```

2. **Configure Services**
```bash
# Edit .env with development settings
DATABASE_URL=postgresql://postgres:password@localhost:5432/data_pipeline
REDIS_URL=redis://localhost:6379/0
DEBUG=True
LOG_LEVEL=DEBUG
```

3. **Start Services**
```bash
# Start all services
docker-compose up -d

# Initialize database
docker-compose exec postgres psql -U postgres -d data_pipeline -c "
CREATE TABLE IF NOT EXISTS pipeline_runs (
    id SERIAL PRIMARY KEY,
    pipeline_name VARCHAR(255),
    status VARCHAR(50),
    start_time TIMESTAMP,
    end_time TIMESTAMP,
    records_processed INTEGER
);
"
```

4. **Run Sample Data**
```bash
# Generate sample data
make sample-data

# Run pipeline
make run
```

### Staging Environment

**Purpose**: Production-like testing environment
**Infrastructure**: Cloud VM or Kubernetes cluster

**Setup Steps:**

1. **Infrastructure Provisioning**
```bash
# Using Terraform (example)
cd infrastructure/staging
terraform init
terraform plan
terraform apply
```

2. **Configuration Management**
```bash
# Create staging configuration
cp config/etl_config.json config/etl_config_staging.json

# Update staging-specific settings
# - Database connections
# - API endpoints
# - Monitoring endpoints
```

3. **Deployment**
```bash
# Build and push Docker images
docker build -t your-registry/data-pipelines:staging .
docker push your-registry/data-pipelines:staging

# Deploy to staging
kubectl apply -f k8s/staging/
```

4. **Smoke Tests**
```bash
# Run automated smoke tests
pytest tests/smoke/ --environment=staging

# Manual verification
curl -f http://staging.your-domain.com/health
```

### Production Environment

**Purpose**: Live production workloads
**Infrastructure**: Kubernetes cluster with high availability

**Setup Steps:**

1. **Infrastructure Setup**
```bash
# Production network configuration
# - VPC setup
# - Security groups
# - Load balancers
# - CDN configuration
```

2. **Security Configuration**
```bash
# SSL/TLS certificates
certbot certonly --webroot -w /var/www/html -d your-domain.com

# Secrets management
kubectl create secret generic db-credentials \
  --from-literal=username=prod_user \
  --from-literal=password=secure_password

# Network policies
kubectl apply -f security/network-policies.yaml
```

3. **High Availability Setup**
```bash
# Multi-zone deployment
kubectl apply -f k8s/production/
kubectl apply -f k8s/monitoring/

# Auto-scaling configuration
kubectl autoscale deployment data-pipeline \
  --cpu-percent=70 \
  --min=3 \
  --max=10
```

4. **Production Deployment**
```bash
# Blue-green deployment strategy
kubectl apply -f k8s/production/blue-green.yaml

# Health checks and monitoring
kubectl get pods -w
```

## Configuration Management

### Environment Variables

**Required Variables:**
```bash
# Database Configuration
DATABASE_URL=postgresql://user:pass@host:port/db
DB_HOST=localhost
DB_PORT=5432
DB_NAME=data_pipeline
DB_USER=your_username
DB_PASSWORD=your_password

# Redis Configuration
REDIS_URL=redis://localhost:6379/0
REDIS_HOST=localhost
REDIS_PORT=6379

# Kafka Configuration
KAFKA_BOOTSTRAP_SERVERS=localhost:9092
KAFKA_TOPIC_INPUT=data_pipeline_input
KAFKA_TOPIC_OUTPUT=data_pipeline_output

# Security
SECRET_KEY=your-secret-key-here
ENCRYPTION_KEY=your-encryption-key-here

# Monitoring
PROMETHEUS_PORT=9090
GRAFANA_PORT=3000
```

**Optional Variables:**
```bash
# Performance Tuning
MAX_WORKERS=4
BATCH_SIZE=1000
MEMORY_LIMIT=2GB

# Logging
LOG_LEVEL=INFO
LOG_FILE_PATH=logs/pipeline.log
LOG_ROTATION=daily

# Notifications
SLACK_WEBHOOK_URL=https://hooks.slack.com/...
EMAIL_TO=admin@company.com
```

### Configuration Files

**ETL Configuration** (`config/etl_config.json`):
```json
{
  "database": {
    "connection_string": "${DATABASE_URL}",
    "pool_size": 10,
    "max_overflow": 20
  },
  "sources": {
    "csv": {
      "file_path": "data/input.csv",
      "encoding": "utf-8"
    },
    "api": {
      "url": "https://api.example.com/data",
      "timeout": 30,
      "retry_count": 3
    }
  },
  "transformations": {
    "remove_duplicates": true,
    "handle_missing_values": "drop",
    "date_columns": ["created_at", "updated_at"]
  }
}
```

**Monitoring Configuration** (`config/monitoring_config.json`):
```json
{
  "metrics": {
    "collection_interval": 60,
    "retention_days": 30
  },
  "alerts": {
    "cpu_threshold": 80.0,
    "memory_threshold": 85.0,
    "disk_threshold": 90.0,
    "error_rate_threshold": 5.0
  },
  "notifications": {
    "slack_webhook": "${SLACK_WEBHOOK_URL}",
    "email_recipients": "${EMAIL_TO}"
  }
}
```

## Docker Deployment

### Building Images

```bash
# Build base image
docker build -t data-engineering-pipelines:latest .

# Build for specific environment
docker build --build-arg ENV=production -t data-engineering-pipelines:prod .

# Multi-platform builds
docker buildx build --platform linux/amd64,linux/arm64 -t your-registry/data-pipelines:latest .
```

### Docker Compose Services

**Development Stack**:
```yaml
version: '3.8'
services:
  app:
    build: .
    environment:
      - DATABASE_URL=postgresql://postgres:password@postgres:5432/data_pipeline
    depends_on:
      - postgres
      - redis
      - kafka

  postgres:
    image: postgres:15
    environment:
      - POSTGRES_DB=data_pipeline
      - POSTGRES_USER=postgres
      - POSTGRES_PASSWORD=password
    volumes:
      - postgres_data:/var/lib/postgresql/data

  redis:
    image: redis:7-alpine
    volumes:
      - redis_data:/data

  kafka:
    image: confluentinc/cp-kafka:7.4.0
    depends_on:
      - zookeeper
    environment:
      KAFKA_ZOOKEEPER_CONNECT: zookeeper:2181
      KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://localhost:9092

volumes:
  postgres_data:
  redis_data:
```

## Kubernetes Deployment

### Namespace Setup

```yaml
# namespace.yaml
apiVersion: v1
kind: Namespace
metadata:
  name: data-pipelines
  labels:
    name: data-pipelines
```

### Application Deployment

```yaml
# deployment.yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: data-pipeline
  namespace: data-pipelines
spec:
  replicas: 3
  selector:
    matchLabels:
      app: data-pipeline
  template:
    metadata:
      labels:
        app: data-pipeline
    spec:
      containers:
      - name: data-pipeline
        image: your-registry/data-pipelines:latest
        ports:
        - containerPort: 8080
        env:
        - name: DATABASE_URL
          valueFrom:
            secretKeyRef:
              name: db-credentials
              key: url
        resources:
          requests:
            memory: "512Mi"
            cpu: "250m"
          limits:
            memory: "1Gi"
            cpu: "500m"
        livenessProbe:
          httpGet:
            path: /health
            port: 8080
          initialDelaySeconds: 30
          periodSeconds: 10
        readinessProbe:
          httpGet:
            path: /ready
            port: 8080
          initialDelaySeconds: 5
          periodSeconds: 5
```

### Service Configuration

```yaml
# service.yaml
apiVersion: v1
kind: Service
metadata:
  name: data-pipeline-service
  namespace: data-pipelines
spec:
  selector:
    app: data-pipeline
  ports:
  - protocol: TCP
    port: 80
    targetPort: 8080
  type: LoadBalancer
```

### Horizontal Pod Autoscaler

```yaml
# hpa.yaml
apiVersion: autoscaling/v2
kind: HorizontalPodAutoscaler
metadata:
  name: data-pipeline-hpa
  namespace: data-pipelines
spec:
  scaleTargetRef:
    apiVersion: apps/v1
    kind: Deployment
    name: data-pipeline
  minReplicas: 3
  maxReplicas: 10
  metrics:
  - type: Resource
    resource:
      name: cpu
      target:
        type: Utilization
        averageUtilization: 70
  - type: Resource
    resource:
      name: memory
      target:
        type: Utilization
        averageUtilization: 80
```

## Monitoring & Observability

### Prometheus Configuration

```yaml
# prometheus-config.yaml
global:
  scrape_interval: 15s
  evaluation_interval: 15s

rule_files:
  - "alert_rules.yml"

scrape_configs:
  - job_name: 'data-pipeline'
    static_configs:
      - targets: ['data-pipeline-service:80']
    metrics_path: /metrics
    scrape_interval: 5s

alerting:
  alertmanagers:
    - static_configs:
        - targets:
          - alertmanager:9093
```

### Grafana Dashboard

```json
{
  "dashboard": {
    "title": "Data Pipeline Monitoring",
    "panels": [
      {
        "title": "Pipeline Throughput",
        "type": "graph",
        "targets": [
          {
            "expr": "rate(pipeline_records_processed_total[5m])",
            "legendFormat": "Records/sec"
          }
        ]
      },
      {
        "title": "Error Rate",
        "type": "singlestat",
        "targets": [
          {
            "expr": "rate(pipeline_errors_total[5m])",
            "legendFormat": "Errors/sec"
          }
        ]
      }
    ]
  }
}
```

## Security Configuration

### SSL/TLS Setup

```bash
# Generate self-signed certificates (development)
openssl req -x509 -newkey rsa:4096 -keyout key.pem -out cert.pem -days 365 -nodes

# Use Let's Encrypt (production)
certbot certonly --standalone -d your-domain.com
```

### Network Policies

```yaml
# network-policy.yaml
apiVersion: networking.k8s.io/v1
kind: NetworkPolicy
metadata:
  name: data-pipeline-netpol
  namespace: data-pipelines
spec:
  podSelector:
    matchLabels:
      app: data-pipeline
  policyTypes:
  - Ingress
  - Egress
  ingress:
  - from:
    - namespaceSelector:
        matchLabels:
          name: monitoring
    ports:
    - protocol: TCP
      port: 8080
  egress:
  - to:
    - namespaceSelector:
        matchLabels:
          name: database
    ports:
    - protocol: TCP
      port: 5432
```

## Backup & Recovery

### Database Backup

```bash
# Automated backup script
#!/bin/bash
DATE=$(date +%Y%m%d_%H%M%S)
BACKUP_FILE="backup_${DATE}.sql"

# Create backup
kubectl exec -n data-pipelines deployment/postgres -- pg_dump -U postgres data_pipeline > $BACKUP_FILE

# Upload to cloud storage
aws s3 cp $BACKUP_FILE s3://your-backup-bucket/postgres/

# Cleanup old backups (keep 30 days)
find . -name "backup_*.sql" -mtime +30 -delete
```

### Disaster Recovery

```bash
# Restore from backup
#!/bin/bash
BACKUP_FILE=$1

# Download backup from cloud storage
aws s3 cp s3://your-backup-bucket/postgres/$BACKUP_FILE .

# Restore database
kubectl exec -i -n data-pipelines deployment/postgres -- psql -U postgres data_pipeline < $BACKUP_FILE

# Verify restoration
kubectl exec -n data-pipelines deployment/postgres -- psql -U postgres -d data_pipeline -c "SELECT COUNT(*) FROM processed_data;"
```

## Troubleshooting

### Common Issues

**1. Database Connection Errors**
```bash
# Check database connectivity
kubectl exec -it -n data-pipelines deployment/data-pipeline -- nc -z postgres 5432

# Check database logs
kubectl logs -n data-pipelines deployment/postgres

# Test connection manually
kubectl exec -it -n data-pipelines deployment/data-pipeline -- python -c "
import sqlalchemy
engine = sqlalchemy.create_engine('postgresql://postgres:password@postgres:5432/data_pipeline')
print(engine.execute('SELECT 1').scalar())
"
```

**2. Memory Issues**
```bash
# Check memory usage
kubectl top pods -n data-pipelines

# Adjust resource limits
kubectl patch deployment data-pipeline -n data-pipelines -p '
{
  "spec": {
    "template": {
      "spec": {
        "containers": [{
          "name": "data-pipeline",
          "resources": {
            "limits": {
              "memory": "2Gi"
            }
          }
        }]
      }
    }
  }
}'
```

**3. Performance Issues**
```bash
# Check pod metrics
kubectl exec -n data-pipelines deployment/data-pipeline -- ps aux

# Profile application
kubectl exec -n data-pipelines deployment/data-pipeline -- python -m cProfile -o profile.stats src/etl_pipeline.py

# Check database performance
kubectl exec -n data-pipelines deployment/postgres -- psql -U postgres -d data_pipeline -c "
SELECT query, calls, total_time, mean_time 
FROM pg_stat_statements 
ORDER BY total_time DESC 
LIMIT 10;"
```

### Log Analysis

```bash
# View application logs
kubectl logs -f -n data-pipelines deployment/data-pipeline

# Search for errors
kubectl logs -n data-pipelines deployment/data-pipeline | grep ERROR

# Aggregate error counts
kubectl logs -n data-pipelines deployment/data-pipeline | grep ERROR | awk '{print $1}' | sort | uniq -c
```

## Performance Optimization

### Database Optimization

```sql
-- Create indexes for better query performance
CREATE INDEX CONCURRENTLY idx_processed_data_timestamp ON processed_data(created_at);
CREATE INDEX CONCURRENTLY idx_processed_data_status ON processed_data(status);

-- Update statistics
ANALYZE processed_data;

-- Check slow queries
SELECT query, mean_time, calls 
FROM pg_stat_statements 
WHERE mean_time > 1000 
ORDER BY mean_time DESC;
```

### Application Optimization

```python
# Connection pooling configuration
from sqlalchemy import create_engine
from sqlalchemy.pool import QueuePool

engine = create_engine(
    DATABASE_URL,
    poolclass=QueuePool,
    pool_size=20,
    max_overflow=30,
    pool_pre_ping=True,
    pool_recycle=3600
)

# Batch processing optimization
def process_in_batches(df, batch_size=1000):
    for i in range(0, len(df), batch_size):
        batch = df[i:i + batch_size]
        yield batch
```

This deployment guide provides a comprehensive framework for deploying the Data Engineering Pipelines project across different environments with proper configuration, monitoring, security, and operational procedures.