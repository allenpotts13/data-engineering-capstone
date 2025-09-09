![Python Version](https://img.shields.io/badge/python-3.11%2B-blue)
![License](https://img.shields.io/badge/license-MIT-green)
# Potts Capstone Project

## Overview
This repository contains a data engineering capstone project focused on ingesting, transforming, and analyzing motorcycle accident data using modern tools and cloud-native pipelines. The project leverages Airflow, DuckDB, MinIO, and Python for scalable, reproducible workflows.

## Features
- Automated data ingestion from FTP, APIs, and web scraping
- Data transformation and analysis using DuckDB
- Storage and retrieval via MinIO object storage
- Orchestrated pipelines with Airflow
- Modular codebase for extensibility

## Folder Structure
```
airflow/           # Airflow DAGs and plugins
data/              # Data scripts, DuckDB files, SQL queries
metabase/          # Metabase Docker setup
src/               # Source code for ingestion, scraping, utilities
tests/             # Unit tests
docker-compose.yml # Container orchestration
pyproject.toml     # Poetry project config
requirements.txt   # Python dependencies
README.md          # Project documentation
```

## Setup Instructions
1. Clone the repository:
	```sh
	git clone https://github.com/NSS-Data-Engineering-May2025/potts-capstone.git
	cd potts-capstone
	```
2. Install dependencies:
	```sh
	poetry install
	```
3. Configure environment variables:

4. Start services:
	```sh
	docker-compose up
	```

5. Next run the duckdb_setup.py to get your database setup:
    ```sh
    poetry run python data/duckdb_setup.py
    ```

## Usage
- Run Airflow pipelines to ingest and process data
- Use DuckDB for SQL-based analysis
- Access results via MinIO or Metabase

## Data Sources
- FARS (Fatality Analysis Reporting System) FTP
- Web scraping for supplemental datasets


## Pipeline Overview
1. Ingestion: Download raw data from FTP, API, or web sources
2. Transformation: Clean and normalize data using DuckDB
3. Storage: Upload processed files to MinIO
4. Analysis: Query data with SQL and visualize in Metabase

## License
This project is licensed under the MIT License. See the [LICENSE](LICENSE) file for details.

## Contact
For questions or collaboration, contact the project owner or open an issue on GitHub.
