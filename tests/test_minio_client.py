from src.utils.minio_client import get_minio_client


def test_get_minio_client_returns_minio(monkeypatch):
    monkeypatch.setenv("MINIO_ENDPOINT", "localhost:9000")
    monkeypatch.setenv("MINIO_ACCESS_KEY", "minioadmin")
    monkeypatch.setenv("MINIO_SECRET_KEY", "minioadmin")
    client = get_minio_client()
    assert client is not None
