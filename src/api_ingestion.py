import os
from dotenv import load_dotenv
from utils.logger import setup_logger
from utils.minio_client import get_minio_client
import requests
import pandas as pd
from io import BytesIO
from minio.error import S3Error

load_dotenv()
logger = setup_logger(__name__)
minio_client = get_minio_client()
