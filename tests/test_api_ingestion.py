import os
import sys
from io import BytesIO
from unittest.mock import MagicMock

from src.ftp_ingestion import (
    _default_fars_zip_url,
    ingest_fars_zip_to_minio,
    upload_to_minio_parquet,
)

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))


def test_default_fars_zip_url_national():
    url = _default_fars_zip_url(2022, "National")
    assert (
        url
        == "https://static.nhtsa.gov/nhtsa/downloads/FARS/2022/National/FARS2022NationalCSV.zip"
    )


def test_default_fars_zip_url_state():
    url = _default_fars_zip_url(2022, "Texas")
    assert (
        url
        == "https://static.nhtsa.gov/nhtsa/downloads/FARS/2022/Texas/FARS2022TexasCSV.zip"
    )


def test_ingest_fars_zip_to_minio_success(monkeypatch):
    mock_session = MagicMock()
    mock_response = MagicMock()
    mock_response.iter_content.return_value = [b"data"]
    mock_response.headers = {"Content-Length": "4"}
    mock_response.status_code = 200
    mock_response.raise_for_status.return_value = None
    mock_session.get.return_value.__enter__.return_value = mock_response
    monkeypatch.setattr(
        "src.api_ingestion.requests_session_with_retries", lambda: mock_session
    )

    class MockZipInfo:
        def __init__(self, filename, file_size):
            self.filename = filename
            self.file_size = file_size

        def is_dir(self):
            return False

    class MockZipFile:
        def __init__(self, *a, **kw):
            self._files = [MockZipInfo("test.csv", 4)]

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc_val, exc_tb):
            pass

        def infolist(self):
            return self._files

        def open(self, file, mode):
            return BytesIO(b"data")

    monkeypatch.setattr("src.api_ingestion.zipfile.ZipFile", MockZipFile)

    mock_minio_client = MagicMock()
    monkeypatch.setattr("src.api_ingestion.get_minio_client", lambda: mock_minio_client)

    uploaded = ingest_fars_zip_to_minio(2022, "National")
    assert uploaded == ["raw/fars/2022/National/test.csv"]


def test_upload_to_minio_parquet_success(monkeypatch):
    mock_minio_client = MagicMock()
    mock_minio_object_response = MagicMock()
    mock_minio_object_response.read.return_value = b"col1,col2\n1,2\n3,4"
    mock_minio_client.get_object.return_value = mock_minio_object_response
    monkeypatch.setattr("src.api_ingestion.get_minio_client", lambda: mock_minio_client)

    mock_df = MagicMock()
    monkeypatch.setattr("pandas.read_csv", lambda *a, **kw: mock_df)
    mock_buf = BytesIO()
    mock_df.to_parquet.side_effect = lambda buf, index, engine: buf.write(b"PARQUET")
    monkeypatch.setattr("src.api_ingestion.BytesIO", lambda *a, **kw: mock_buf)

    result = upload_to_minio_parquet("raw/fars/2022/National/test.csv", "bucket")
    assert result == "parquet/fars/2022/National/test.parquet"
