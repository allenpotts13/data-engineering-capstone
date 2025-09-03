from src.utils.minio_client import get_minio_client


def test_get_minio_client_returns_minio():
    client = get_minio_client()
    # The actual Minio client requires env vars, so just check type
    from minio import Minio

    assert isinstance(client, Minio)
