import io
import os
import re
import tempfile

import duckdb
import pandas as pd
from dotenv import load_dotenv

from src.utils.logger import setup_logger
from src.utils.minio_client import get_minio_client

logger = setup_logger(__name__, log_file="src/logs/duckdb_ingestion.log")
load_dotenv()


def fars_data_to_duckdb():
    duckdb_folder = os.path.join(os.getcwd(), "data", "duckdb")
    os.makedirs(duckdb_folder, exist_ok=True)
    duckdb_path = os.path.join(duckdb_folder, "motorcycle_capstone.duckdb")

    schema_name = "bronze"
    bucket_name = os.getenv("MINIO_BUCKET_NAME")
    minio_client = get_minio_client()

    con = duckdb.connect(duckdb_path)
    con.execute(f"CREATE SCHEMA IF NOT EXISTS {schema_name}")

    # Create ingest_config table if it doesn't exist
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS ingest_config (
            file_path TEXT PRIMARY KEY,
            ingested_at TIMESTAMP
        )
    """
    )

    objects = minio_client.list_objects(
        bucket_name, prefix="parquet/fars/", recursive=True
    )
    file_type_to_files = {}
    for obj in objects:
        parts = obj.object_name.split("/")
        if len(parts) >= 5 and parts[-1].endswith(".parquet"):
            file_type = parts[-1].replace(".parquet", "").lower()
            file_type_to_files.setdefault(file_type, []).append(obj.object_name)

    for file_type, matched_files in file_type_to_files.items():
        logger.info(
            f"File type '{file_type}': found {len(matched_files)} matching files."
        )
        if not matched_files:
            logger.warning(f"No files found for file type '{file_type}'.")
            continue

        matched_files_sorted = sorted(
            matched_files, key=lambda x: re.search(r"/([0-9]{4})/", x).group(1)
        )
        table_name = f"{schema_name}.bronze_{file_type.lower()}"

        schema_columns = None
        first_file = True
        for file_path in matched_files_sorted:
            result = con.execute(
                "SELECT 1 FROM ingest_config WHERE file_path = ?", [file_path]
            ).fetchone()
            if result:
                logger.info(f"Skipping already ingested file: {file_path}")
                continue
            try:
                logger.info(f"Downloading {file_path} from MinIO for DuckDB import")
                data = minio_client.get_object(bucket_name, file_path).read()
                imported_data = pd.read_parquet(io.BytesIO(data))
                if schema_columns is None:
                    schema_columns = imported_data.columns.tolist()
                missing_columns = [
                    col for col in schema_columns if col not in imported_data.columns
                ]
                if missing_columns:
                    missing_df = pd.DataFrame(
                        {col: [pd.NA] * len(imported_data) for col in missing_columns}
                    )
                    imported_data = pd.concat([imported_data, missing_df], axis=1)
                extra_cols = [
                    col for col in imported_data.columns if col not in schema_columns
                ]
                if extra_cols:
                    imported_data = imported_data.drop(columns=extra_cols)
                imported_data = imported_data[schema_columns]
                for col in imported_data.columns:
                    if pd.api.types.is_integer_dtype(imported_data[col]) or (
                        len(imported_data[col].dropna()) > 0
                        and pd.api.types.is_integer_dtype(
                            type(imported_data[col].dropna().iloc[0])
                        )
                    ):
                        imported_data[col] = pd.to_numeric(
                            imported_data[col], errors="coerce"
                        ).astype("Int64")
                con.register("df", imported_data)
                if first_file:
                    con.execute(f"DROP TABLE IF EXISTS {table_name}")
                    con.execute(f"CREATE TABLE {table_name} AS SELECT * FROM df")
                    logger.info(f"Created DuckDB table {table_name} from {file_path}")
                    first_file = False
                else:
                    con.execute(f"INSERT INTO {table_name} SELECT * FROM df")
                    logger.info(f"Appended {file_path} to DuckDB table {table_name}")
                # Record ingestion in ingest_config
                con.execute(
                    "INSERT INTO ingest_config (file_path, ingested_at) VALUES (?, CURRENT_TIMESTAMP)",
                    [file_path],
                )
            except Exception as e:
                logger.error(f"Error loading {file_path} into DuckDB: {e}")
    con.close()


def helmet_laws_to_duckdb():
    duckdb_folder = os.path.join(os.getcwd(), "data", "duckdb")
    os.makedirs(duckdb_folder, exist_ok=True)
    duckdb_path = os.path.join(duckdb_folder, "motorcycle_capstone.duckdb")

    schema_name = "bronze"
    bucket_name = os.getenv("MINIO_BUCKET_NAME")
    minio_client = get_minio_client()

    con = duckdb.connect(duckdb_path)
    con.execute(f"CREATE SCHEMA IF NOT EXISTS {schema_name}")

    matching_files = [
        obj
        for obj in minio_client.list_objects(
            bucket_name, prefix="parquet/", recursive=True
        )
        if obj.object_name.endswith(".parquet")
        and "motorcycle_helmet_laws" in obj.object_name
    ]

    if matching_files:
        most_recent = max(matching_files, key=lambda o: o.last_modified)
        table_name = "bronze_motorcycle_helmet_laws"
        full_table_name = f"{schema_name}.{table_name}"

        logger.info(
            f"Downloading {most_recent.object_name} from MinIO for DuckDB import"
        )
        response = minio_client.get_object(bucket_name, most_recent.object_name)
        local_parquet = os.path.join(
            tempfile.gettempdir(), os.path.basename(most_recent.object_name)
        )
        with open(local_parquet, "wb") as lp:
            lp.write(response.read())
        # Truncate table before loading new data
        con.execute(f"DROP TABLE IF EXISTS {full_table_name}")
        con.execute(
            f"CREATE TABLE {full_table_name} AS SELECT * FROM '{local_parquet}'"
        )
        os.remove(local_parquet)
        logger.info(
            f"Loaded {most_recent.object_name} into DuckDB table {full_table_name}"
        )
    con.close()


def main():
    try:
        fars_data_to_duckdb()
        helmet_laws_to_duckdb()
        logger.info("Data ingestion to DuckDB completed successfully.")
        return True
    except Exception as e:
        logger.error(f"Error during DuckDB ingestion: {e}")
        return False


if __name__ == "__main__":
    main()
