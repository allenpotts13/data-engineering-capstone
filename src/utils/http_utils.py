import mimetypes
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

def requests_session_with_retries(
    total=5, backoff_factor=1.5, status_forcelist=(429, 500, 502, 503, 504)
) -> requests.Session:
    retry = Retry(
        total=total,
        read=total,
        connect=total,
        backoff_factor=backoff_factor,
        status_forcelist=status_forcelist,
        allowed_methods=frozenset(["GET"]),
        raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retry, pool_connections=10, pool_maxsize=10)
    s = requests.Session()
    s.mount("https://", adapter)
    s.mount("http://", adapter)
    s.headers.update({
        "User-Agent": "AllenCapstone/1.0 (+https://example.com)",
        "Accept": "application/zip,application/octet-stream,*/*;q=0.8",
    })
    s.trust_env = False 
    return s

def guess_content_type(name: str) -> str:
    ctype, _ = mimetypes.guess_type(name)
    return ctype or "application/octet-stream"
