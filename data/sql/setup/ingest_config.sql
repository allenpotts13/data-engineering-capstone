CREATE TABLE IF NOT EXISTS config.ingest_config (
            file_path TEXT PRIMARY KEY,
            ingested_at TIMESTAMP
        );