import datetime
import os
import tempfile
import zipfile
from io import BufferedReader, BytesIO

import pandas as pd
from dotenv import load_dotenv
from minio import S3Error

from src.utils.http_utils import guess_content_type, requests_session_with_retries
from src.utils.logger import setup_logger
from src.utils.minio_client import get_minio_client

load_dotenv()
logger = setup_logger(__name__, log_file="src/logs/ftp_ingestion.log")


def _default_fars_zip_url(year: int, scope: str = "National") -> str:
    year = str(year)
    scope_clean = scope.strip().title()
    return (
        f"https://static.nhtsa.gov/nhtsa/downloads/FARS/{year}/"
        f"{scope_clean}/FARS{year}{scope_clean}CSV.zip"
    )


def _download_zip(zip_url, timeout):
    print(f"[FARS] Downloading ZIP: {zip_url}")
    logger.info(f"Downloading ZIP from {zip_url}")
    with requests_session_with_retries() as session:
        with session.get(zip_url, timeout=timeout, stream=True) as resp:
            resp.raise_for_status()
            total = int(resp.headers.get("Content-Length") or 0)
            downloaded = 0
            chunk_size = 1024 * 1024
            with tempfile.NamedTemporaryFile(delete=False, suffix=".zip") as tmp:
                tmp_path = tmp.name
                for chunk in resp.iter_content(chunk_size=chunk_size):
                    if not chunk:
                        continue
                    tmp.write(chunk)
                    downloaded += len(chunk)
                    if total:
                        pct = downloaded / total * 100
                        print(
                            f"  ... {downloaded/1_048_576:.1f} MB ({pct:.1f}%)",
                            end="\r",
                        )
                print()
            return tmp_path


def _extract_zip_files(tmp_path, only_csv=True):
    print("[FARS] ZIP downloaded. Unzipping & filtering files…")
    logger.info("ZIP downloaded; starting unzip phase")
    with zipfile.ZipFile(tmp_path) as zip_file:
        zip_file_list = [z for z in zip_file.infolist() if not z.is_dir()]
        if not zip_file_list:
            msg = "ZIP contained no files"
            print(msg)
            logger.error(msg)
            try:
                os.remove(tmp_path)
            except Exception as e:
                logger.error(f"Error removing temp file {tmp_path}: {e}")
            return []
        filtered = []
        for file in zip_file_list:
            name_in_zip = file.filename
            size = file.file_size
            if size == 0:
                logger.warning(f"Skipping empty file: {name_in_zip}")
                continue
            if only_csv and not name_in_zip.lower().endswith(".csv"):
                logger.info(f"Skipping non-CSV: {name_in_zip}")
                continue
            filtered.append(file)
    try:
        os.remove(tmp_path)
    except Exception as e:
        logger.error(f"Error removing temp file {tmp_path}: {e}")
    return filtered


def _upload_file_to_minio(minio_client, bucket, prefix, file, zip_file):
    name_in_zip = file.filename
    size = file.file_size
    object_key = prefix + os.path.basename(name_in_zip)
    ctype = guess_content_type(name_in_zip)
    print(f"  → {name_in_zip}  ->  {object_key}  ({size/1_048_576:.2f} MB)")
    logger.info(f"Uploading {name_in_zip} to {object_key} (size={size})")
    with zip_file.open(file, "r") as f:
        data_stream = BufferedReader(f)
        minio_client.put_object(
            bucket_name=bucket,
            object_name=object_key,
            data=data_stream,
            length=size,
            content_type=ctype,
        )
    return object_key


def upload_to_minio_parquet(csv_object_name: str, bucket_name: str) -> str | None:
    try:
        minio_client = get_minio_client()
        if not minio_client.bucket_exists(bucket_name):
            minio_client.make_bucket(bucket_name)
        minio_object_response = minio_client.get_object(bucket_name, csv_object_name)
        data = minio_object_response.read()
        if not data:
            logger.warning(f"{csv_object_name} is empty; skipping parquet conversion.")
            return None

        ftp_call = pd.read_csv(
            BytesIO(data), encoding="latin1", low_memory=False, dtype=str
        )
        parquet_buf = BytesIO()
        ftp_call.to_parquet(parquet_buf, index=False, engine="pyarrow")
        parquet_buf.seek(0)
        # Ensure both .csv and .CSV extensions are replaced with .parquet
        base_key = csv_object_name
        if base_key.lower().endswith(".csv"):
            base_key = base_key[: -(len(".csv"))] + ".parquet"
        parquet_key = base_key.replace("raw/", "parquet/")

        minio_client.put_object(
            bucket_name,
            parquet_key,
            parquet_buf,
            length=parquet_buf.getbuffer().nbytes,
            content_type="application/octet-stream",
        )
        logger.info(
            f"Uploaded Parquet file as {parquet_key} to MinIO bucket {bucket_name}."
        )
        print(f"Uploaded Parquet file as {parquet_key} to MinIO.")
        return parquet_key

    except S3Error as e:
        logger.error(f"Error uploading Parquet to MinIO: {e}")
        print(f"Error uploading Parquet to MinIO: {e}")
    except Exception as e:
        logger.error(f"Parquet conversion failed for {csv_object_name}: {e}")
        print(f"Parquet conversion failed: {e}")
    finally:
        try:
            minio_object_response.close()
        except Exception as e:
            logger.error(f"Error closing MinIO object response: {e}")

    return None


def get_existing_years(minio_client, bucket_name, prefix="parquet/fars/"):
    existing_years = set()
    objects = minio_client.list_objects(bucket_name, prefix=prefix, recursive=True)
    for obj in objects:
        parts = obj.object_name.split("/")
        if len(parts) > 2 and parts[2].isdigit():
            existing_years.add(int(parts[2]))
    return existing_years


def process_year(year, bucket_name):

    print(f"\nProcessing new year: {year}")
    uploaded_csvs = ingest_fars_zip_to_minio(year=year, scope="National")
    if not uploaded_csvs:
        logger.warning(f"No CSV files uploaded to MinIO for year {year}.")
        print(f"No CSV files uploaded to MinIO for year {year}.")
        return
    for csv_object_name in uploaded_csvs:
        upload_to_minio_parquet(csv_object_name, bucket_name)


def ingest_fars_zip_to_minio(
    year: int,
    scope: str = "National",
    bucket_name: str = None,
    prefix: str = "raw/fars/",
):
    timeout = 60
    zip_url = _default_fars_zip_url(year, scope)
    tmp_zip_path = _download_zip(zip_url, timeout)
    extracted_files = _extract_zip_files(tmp_zip_path, only_csv=True)
    if not extracted_files:
        logger.warning(f"No CSV files found in ZIP for year {year}.")
        return []
    if bucket_name is None:
        bucket_name = os.getenv("MINIO_BUCKET_NAME", "capstone")
    minio_client = get_minio_client()
    if not minio_client.bucket_exists(bucket_name):
        minio_client.make_bucket(bucket_name)
    uploaded_object_names = []
    with zipfile.ZipFile(tmp_zip_path) as zip_file:
        for file in extracted_files:
            object_key = prefix + f"{year}/" + os.path.basename(file.filename)
            try:
                _upload_file_to_minio(
                    minio_client, bucket_name, prefix + f"{year}/", file, zip_file
                )
                uploaded_object_names.append(object_key)
            except Exception as e:
                logger.error(f"Failed to upload {file.filename} for year {year}: {e}")
    return uploaded_object_names


def main():
    print("Starting API ingestion...")
    logger.info(f"API_BASE_URL: {os.getenv('API_BASE_URL')}")
    logger.info(f"MINIO_BUCKET_NAME: {os.getenv('MINIO_BUCKET_NAME')}")

    bucket_name = os.getenv("MINIO_BUCKET_NAME", "capstone")
    minio_client = get_minio_client()

    current_year = datetime.datetime.now().year
    available_years = list(range(2024, current_year + 1))
    existing_years = get_existing_years(minio_client, bucket_name)
    new_years = [year for year in available_years if year not in existing_years]

    if not new_years:
        print("No new year data to ingest. Exiting.")
        logger.info("No new year data to ingest. Exiting.")
        return True

    for year in new_years:
        process_year(year, bucket_name)

    print("API ingestion and Parquet upload complete.")
    return True


if __name__ == "__main__":
    main()
