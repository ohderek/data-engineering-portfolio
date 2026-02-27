# Data Pipeline Framework

A production-grade, modular ETL framework built on Apache Airflow and Snowflake. Designed for extensibility, observability, and idempotent execution.

## Architecture

```
data-pipeline-framework/
├── dags/                          # Airflow DAG definitions
│   ├── ecommerce_ingestion_dag.py # E-commerce orders & events pipeline
│   └── data_quality_dag.py        # Automated quality checks
├── src/
│   ├── extractors/                # Source connectors
│   │   ├── base_extractor.py      # Abstract base with retry & backoff
│   │   ├── api_extractor.py       # REST API extractor (paginated)
│   │   └── database_extractor.py  # JDBC/SQLAlchemy source extractor
│   ├── transformers/              # Data transformation logic
│   │   ├── base_transformer.py
│   │   └── data_transformer.py    # Schema normalization, type coercion
│   ├── loaders/
│   │   └── snowflake_loader.py    # Bulk COPY INTO + merge patterns
│   └── utils/
│       ├── logger.py              # Structured JSON logging
│       └── data_quality.py        # Row count, null, freshness checks
└── tests/
    ├── test_extractors.py
    ├── test_transformers.py
    └── test_loaders.py
```

## Key Features

- **Idempotent execution** — safe to re-run with no duplicates
- **Incremental + full-refresh modes** — configurable per pipeline
- **Dead-letter queue** — failed records routed to error tables
- **Observability** — structured logs + Airflow metrics + Snowflake query history
- **Schema evolution** — automatic column addition, type widening alerts
- **Data quality gates** — pipelines fail fast on quality violations

## Quick Start

```bash
# Clone and set up
git clone https://github.com/dereko/data-pipeline-framework.git
cd data-pipeline-framework
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt

# Configure environment (copy and fill in values)
cp .env.example .env

# Run tests
pytest tests/ -v

# Start Airflow locally (Docker)
docker-compose up -d
```

## Environment Variables

```bash
# .env.example — never commit actual values
SNOWFLAKE_ACCOUNT=<your_account>
SNOWFLAKE_USER=<your_user>
SNOWFLAKE_ROLE=TRANSFORMER
SNOWFLAKE_WAREHOUSE=COMPUTE_WH
SNOWFLAKE_DATABASE=RAW
SNOWFLAKE_SCHEMA=PUBLIC
SOURCE_API_BASE_URL=https://api.example.com
```

## Pipeline Patterns

| Pattern | Use Case | Implementation |
|---------|----------|----------------|
| Full Refresh | Small dimension tables | Truncate → Insert |
| Incremental | Large fact tables | Watermark → Merge |
| CDC | Near-real-time sources | Debezium → Kafka → Stage |
| SCD Type 2 | Slowly changing dimensions | dbt snapshot |

## Tech Stack

- **Orchestration:** Apache Airflow 2.x
- **Processing:** Python 3.11, Pandas, PySpark
- **Destination:** Snowflake (COPY INTO, Merge)
- **Quality:** Great Expectations
- **CI/CD:** GitHub Actions
- **Infrastructure:** Docker, Terraform (AWS)
