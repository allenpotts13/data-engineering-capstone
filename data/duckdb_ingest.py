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

    file_types = [
        "accident",
        "CEvent",
        "damage",
        "distract",
        "drimpair",
        "drugs",
        "factor",
        "maneuver",
        "miacc",
        "midrvacc",
        "miper",
        "nmcrash",
        "nmdistract",
        "nmimpair",
        "nmprior",
        "parkwork",
        "pbtype",
        "person",
        "race",
        "safetyEq",
        "vehicle",
        "VEvent",
        "Violatn",
        "Vision",
        "VPICdecode",
        "VPICTrailerdecode",
        "VSOE",
    ]

    con = duckdb.connect(duckdb_path)
    con.execute(f"CREATE SCHEMA IF NOT EXISTS {schema_name}")

    for file_type in file_types:
        pattern = re.compile(
            rf"parquet/fars/.+/national/{file_type}\.parquet$", re.IGNORECASE
        )
        objects = minio_client.list_objects(
            bucket_name, prefix="parquet/fars/", recursive=True
        )
        matched_files = [
            obj.object_name for obj in objects if pattern.search(obj.object_name)
        ]

        logger.info(
            f"File type '{file_type}': found {len(matched_files)} matching files."
        )
        if not matched_files:
            logger.warning(f"No files found for file type '{file_type}'.")
            continue

        matched_files_sorted = sorted(
            matched_files,
            key=lambda x: re.search(r"/([0-9]{4})/", x).group(1),
            reverse=True,
        )
        latest_file = matched_files_sorted[0]
        table_name = f"{schema_name}.bronze_{file_type.lower()}"

        logger.info(f"Using {latest_file} as schema standard for {file_type}")
        data = minio_client.get_object(bucket_name, latest_file).read()
        schema_df = pd.read_parquet(io.BytesIO(data))
        schema_columns = schema_df.columns.tolist()

        con.register("df", schema_df)
        con.execute(f"CREATE OR REPLACE TABLE {table_name} AS SELECT * FROM df")
        logger.info(f"Loaded {latest_file} into DuckDB table {table_name}")

        for file_path in matched_files_sorted[1:]:
            try:
                logger.info(f"Downloading {file_path} from MinIO for DuckDB import")
                data = minio_client.get_object(bucket_name, file_path).read()
                imported_data = pd.read_parquet(io.BytesIO(data))
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
                con.execute(f"INSERT INTO {table_name} SELECT * FROM df")
                logger.info(f"Loaded {file_path} into DuckDB table {table_name}")
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
        con.execute(
            f"CREATE OR REPLACE TABLE {full_table_name} AS SELECT * FROM '{local_parquet}'"
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
