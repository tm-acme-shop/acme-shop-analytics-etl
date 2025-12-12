# AcmeShop Analytics ETL

Python-based ETL batch jobs for the AcmeShop e-commerce platform. This service processes data from all AcmeShop services (users, orders, payments, notifications) and generates analytics for reporting and business intelligence.

## Architecture

```
┌─────────────────┐     ┌─────────────────┐     ┌─────────────────┐
│  Users Service  │     │ Orders Service  │     │Payments Service │
└────────┬────────┘     └────────┬────────┘     └────────┬────────┘
         │                       │                       │
         └───────────────────────┼───────────────────────┘
                                 │
                    ┌────────────▼────────────┐
                    │     Analytics ETL       │
                    │  (Airflow Orchestrated) │
                    └────────────┬────────────┘
                                 │
                    ┌────────────▼────────────┐
                    │   Analytics Warehouse   │
                    └─────────────────────────┘
```

## ETL Jobs

- **User Analytics**: Processes user registration, activity, and churn metrics
- **Order Analytics**: Aggregates order volumes, revenue, and product trends
- **Payment Analytics**: Tracks payment success rates, fraud detection metrics
- **Notification Analytics**: Measures delivery rates, engagement metrics

## Project Structure

```
acme-shop-analytics-etl/
├── dags/                           # Airflow DAG definitions
│   ├── user_analytics_dag.py
│   ├── order_analytics_dag.py
│   ├── payment_analytics_dag.py
│   └── notification_analytics_dag.py
├── sql/                            # SQL query files
│   ├── user_analytics_legacy.sql   # Legacy schema queries
│   └── user_analytics_v2.sql       # V2 schema queries
├── src/acme_shop_analytics_etl/    # Main package
│   ├── config/                     # Configuration & feature flags
│   ├── db/                         # Database access layer
│   ├── etl/                        # ETL job implementations
│   ├── logging/                    # Logging utilities
│   ├── models/                     # Data models (v1 & v2)
│   └── pii/                        # PII handling utilities
├── scripts/                        # Utility scripts
└── tests/                          # Unit tests
```

## Setup

### Prerequisites

- Python 3.11+
- PostgreSQL 14+
- Apache Airflow 2.x (for orchestration)

### Installation

```bash
# Create virtual environment
python -m venv .venv
source .venv/bin/activate

# Install package in development mode
pip install -e ".[dev]"
```

### Configuration

Copy the example environment file and configure:

```bash
cp .env.example .env
# Edit .env with your database credentials and feature flags
```

### Feature Flags

| Flag | Description | Default |
|------|-------------|---------|
| `ENABLE_LEGACY_ETL` | Enable legacy ETL pipelines | `true` |
| `ENABLE_V1_SCHEMA` | Use v1 data models | `true` |

## Running ETL Jobs

### Via Airflow

```bash
# Start Airflow scheduler and webserver
airflow scheduler &
airflow webserver
```

### Via CLI (local development)

```bash
# Run all ETL jobs
python scripts/run_all_etl_locally.py

# Run specific job
python -m acme_shop_analytics_etl.cli --job user_analytics
```

## Development

### Running Tests

```bash
pytest tests/
```

### Code Style

This project uses `ruff` for linting and `black` for formatting.

```bash
ruff check .
black .
```

## Migration Notes

### V1 → V2 Schema Migration

The codebase contains both v1 (legacy) and v2 (modern) patterns:

- **V1 Models**: Direct PII storage, denormalized schemas
- **V2 Models**: Tokenized PII, normalized schemas, better typing

Feature flags control which pipeline runs. See `src/acme_shop_analytics_etl/config/feature_flags.py`.

### Security Notes

> ⚠️ **TODO(TEAM-SEC)**: Several legacy patterns exist for migration purposes:
> - MD5 hashing for deduplication (should migrate to SHA-256)
> - Raw SQL string interpolation (should use parameterized queries)
> - Direct PII storage in v1 models (should use tokenization)

## License

Copyright © 2024 AcmeShop Inc. All rights reserved.
