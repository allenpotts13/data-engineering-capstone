import io
import os

import pandas as pd
from dotenv import load_dotenv

from src.utils.minio_client import get_minio_client

load_dotenv()

minio_client = get_minio_client()
bucket_name = os.getenv("MINIO_BUCKET_NAME")

# Example for 2021
file_path_2021 = "parquet/fars/2021/National/accident.parquet"
data_2021 = minio_client.get_object(bucket_name, file_path_2021).read()
df_2021 = pd.read_parquet(io.BytesIO(data_2021))
print("2021 columns:", df_2021.columns)
print(df_2021[["state"]].head())  # or print(df_2021.head()) to see all columns

# Repeat for 2022
file_path_2022 = "parquet/fars/2022/National/accident.parquet"
data_2022 = minio_client.get_object(bucket_name, file_path_2022).read()
df_2022 = pd.read_parquet(io.BytesIO(data_2022))
print("2022 columns:", df_2022.columns)
print(df_2022[["state"]].head())
