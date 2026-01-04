# Data Engineering Pipelines

A comprehensive data engineering pipeline framework with ETL, streaming, validation, and monitoring capabilities.

## 🚀 Features

- **ETL Pipeline**: Extract, Transform, Load with multiple data sources
- **Data Validation**: Schema validation, data quality checks, business rules
- **Streaming Pipeline**: Real-time data processing with Kafka
- **Orchestration**: Airflow DAGs for pipeline scheduling
- **Monitoring**: Performance metrics, alerts, and health checks
- **Testing**: Comprehensive test suite

## 📁 Project Structure

```
data-engineering-pipelines/
├── src/                    # Source code
│   ├── etl_pipeline.py    # Main ETL pipeline
│   ├── data_validator.py  # Data validation framework
│   ├── streaming_pipeline.py # Kafka streaming
│   └── monitoring.py       # Monitoring & alerting
├── dags/                   # Airflow DAGs
│   └── data_pipeline_dag.py
├── config/                 # Configuration files
│   ├── etl_config.json
│   ├── validation_rules.json
│   ├── kafka_config.json
│   └── monitoring_config.json
├── tests/                  # Test files
├── data/                   # Data storage
├── logs/                   # Log files
└── requirements.txt        # Dependencies
```

## 🛠️ Installation

1. **Clone the repository**:
   ```bash
   cd "C:\Users\Student\Documents\data-engineering-pipelines"
   ```

2. **Install dependencies**:
   ```bash
   pip install -r requirements.txt
   ```

3. **Set up environment variables**:
   ```bash
   # Create .env file
   DATABASE_URL=postgresql://user:password@localhost:5432/datawarehouse
   KAFKA_BOOTSTRAP_SERVERS=localhost:9092
   REDIS_URL=redis://localhost:6379
   ```

## 🏃‍♂️ Quick Start

### 1. Run ETL Pipeline

```python
from src.etl_pipeline import ETLPipeline

# Initialize pipeline
pipeline = ETLPipeline()

# Run with CSV data
result = pipeline.run_pipeline(
    source_type="csv",
    source_path="data/sample_data.csv",
    target_table="customer_data"
)

print(f"Pipeline completed: {result}")
```

### 2. Validate Data

```python
from src.data_validator import DataValidator
import pandas as pd

# Create validator
validator = DataValidator()

# Load your data
df = pd.read_csv("data/your_data.csv")

# Run validation
results = validator.run_full_validation(df)
print(f"Validation passed: {results['overall_passed']}")
```

### 3. Start Streaming Pipeline

```python
from src.streaming_pipeline import StreamDataPipeline

# Initialize streaming pipeline
stream_pipeline = StreamDataPipeline()

# Setup and run
stream_pipeline.setup_pipeline()
stream_pipeline.run_pipeline()
```

### 4. Monitor Pipelines

```python
from src.monitoring import PipelineMonitor

# Start monitoring
monitor = PipelineMonitor()
monitor.start_monitoring()

# Record metrics
monitor.record_pipeline_metrics(
    pipeline_name="customer_etl",
    status="success",
    rows_processed=10000,
    processing_time=45.5
)

# Check health
health = monitor.get_pipeline_health("customer_etl")
print(f"Pipeline health: {health}")
```

## 📊 Airflow Integration

1. **Copy DAG to Airflow**:
   ```bash
   cp dags/data_pipeline_dag.py /path/to/airflow/dags/
   ```

2. **Configure Airflow connections**:
   - PostgreSQL connection
   - Slack webhook (for notifications)

3. **Enable DAG** in Airflow UI

## 🔧 Configuration

### ETL Configuration (`config/etl_config.json`)
```json
{
    "database": {
        "url": "postgresql://user:password@localhost:5432/datawarehouse"
    },
    "sources": {
        "csv_path": "data/source_data.csv",
        "api_endpoint": "https://api.example.com/data"
    },
    "transformations": {
        "remove_duplicates": true,
        "fill_missing": "forward_fill"
    }
}
```

### Validation Rules (`config/validation_rules.json`)
```json
{
    "schema_rules": {
        "required_columns": ["id", "name", "email"],
        "column_types": {
            "id": "int64",
            "name": "object"
        }
    },
    "data_quality_rules": {
        "max_null_percentage": 20,
        "min_rows": 100
    }
}
```

## 🧪 Testing

Run the test suite:

```bash
# Run all tests
python -m pytest tests/ -v

# Run with coverage
python -m pytest tests/ --cov=src --cov-report=html
```

## 📈 Monitoring Features

- **Real-time metrics**: CPU, memory, throughput
- **Alert system**: Slack/email notifications
- **Performance analysis**: Trend detection, bottleneck identification
- **Health scoring**: Automated pipeline health assessment

## 🔄 Streaming Features

- **Kafka integration**: Producer/consumer setup
- **Stream processing**: Real-time transformation
- **Message enrichment**: Add computed fields
- **Stream filtering**: Conditional message routing

## 🚨 Alert Types

- High error rate
- Low throughput
- High memory/CPU usage
- Pipeline failures
- Data quality issues

## 📝 Logging

All components use structured logging with `loguru`:
- Automatic log rotation
- JSON format for parsing
- Different log levels
- File and console output

## 🔒 Security Best Practices

- Environment variables for secrets
- Database connection pooling
- Input validation
- Error handling without data exposure

## 📚 Dependencies

Key libraries:
- `pandas` - Data manipulation
- `sqlalchemy` - Database ORM
- `kafka-python` - Kafka client
- `apache-airflow` - Orchestration
- `great-expectations` - Data validation
- `loguru` - Logging
- `psutil` - System monitoring

## 🤝 Contributing

1. Fork the repository
2. Create feature branch
3. Add tests for new functionality
4. Ensure all tests pass
5. Submit pull request

## 📄 License

This project is licensed under the MIT License.

## 🆘 Support

For issues and questions:
1. Check the logs in `logs/` directory
2. Review configuration files
3. Run validation checks
4. Check monitoring dashboard

---

**Project Path**: `C:\Users\Student\Documents\data-engineering-pipelines\`