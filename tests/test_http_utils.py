import requests

from src.utils.http_utils import guess_content_type, requests_session_with_retries


def test_requests_session_with_retries_returns_session():
    session = requests_session_with_retries()
    assert isinstance(session, requests.Session)


def test_guess_content_type_csv():
    assert guess_content_type("file.csv") in ("text/csv", "application/vnd.ms-excel")


def test_guess_content_type_txt():
    assert guess_content_type("file.txt") == "text/plain"
