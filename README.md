# Djinni Job Scraper

ETL pipeline for collecting and analyzing job postings from djinni.co using Apache Airflow.

## Tech Stack

- **Apache Airflow** — ETL pipeline orchestration
- **PostgreSQL** — data storage
- **Redis** — Celery broker
- **Python** — data parsing and processing
- **BeautifulSoup4 & lxml** — HTML data extraction
- **Docker** — containerization

## Features

- Automatic job posting collection from djinni.co
- Detailed information parsing (salary, skills, companies)
- External Crawler-API integration for bypass blocking
- ETL pipeline with Extract, Transform, Load separation
- Report generation for collected data
- Batch processing and retry mechanism support

## Project Structure

```
airflow/
└── dags/
    ├── djinni_dag.py         # Main DAG
    ├── task_loader_dag.py    # Additional DAG
    ├── tasks/                # ETL tasks
    │   ├── extract_tasks.py
    │   ├── transform_tasks.py
    │   ├── load_tasks.py
    │   └── report_tasks.py
    └── utils/                # Utilities
        ├── crawler_client.py
        ├── job_parser.py
        └── database.py

database/
└── init.sql              # Database schema
```

## Database Schema

- **companies** — company information
- **job_catalog** — job posting URL catalog
- **jobs** — detailed job posting information
- **job_skills** — skills for job postings

## Quick Start

### 1. Clone the repository

```bash
git clone https://github.com/softK1T/djinni.git
cd djinni
```

### 2. Start infrastructure

```bash
docker-compose up -d
```

### 3. Access Airflow

```
URL: http://localhost:8080
Login: admin
Password: admin
```

### 4. Enable DAG

In Airflow web interface, enable DAG `djinni_etl_pipeline`

## ETL Pipeline

### Extract
- Extract job posting URLs from djinni.co catalog
- Save to `job_catalog` table

### Transform
- Download HTML pages via Crawler-API
- Parse data (JSON-LD + XPath)
- Save to JSON files

### Load
- Load data from JSON to PostgreSQL
- Normalize and link tables
- Handle duplicates

### Report
- Generate statistics for collected data

## Configuration

### DAG Parameters

In `djinni_dag.py`:

```python
extract_tasks = ExtractTasks(max_pages=700, max_jobs=11000)
```

### Database Connection

Configuration in `docker-compose.yml`:

```yaml
POSTGRES_DB: djinni_analytics
POSTGRES_USER: djinni_user
POSTGRES_PASSWORD: djinni_pass
```

## Crawler-API Integration

External [Crawler-API](https://github.com/softK1T/crawler-api) is used to bypass djinni.co blocking.

Configuration in `utils/crawler_client.py`:

```python
CRAWLER_API_URL = "http://crawler-api:8000"
```

## Data Parsing

`job_parser.py` extracts:
- Basic information from JSON-LD
- Additional fields via XPath
- Skills, view statistics
- Company information

## Requirements

- Docker & Docker Compose
- 4GB RAM for Airflow
- Working Crawler-API instance (optional)
