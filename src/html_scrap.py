import requests
from bs4 import BeautifulSoup
from urllib.robotparser import RobotFileParser
import csv
import os
from datetime import datetime
from utils.minio_client import get_minio_client
from dotenv import load_dotenv
import re
import io
import tempfile
import pandas as pd

load_dotenv()

def can_scrape(url):
    from urllib.parse import urlparse
    parsed = urlparse(url)
    robots_url = f"{parsed.scheme}://{parsed.netloc}/robots.txt"
    print(f"Checking robots.txt at: {robots_url}")
    try:
        resp = requests.get(robots_url)
        if resp.status_code == 200:
            lines = resp.text.splitlines()
            disallowed = []
            for line in lines:
                line = line.strip()
                if line.lower().startswith('disallow:'):
                    path = line.split(':', 1)[1].strip()
                    if path:
                        disallowed.append(path)
            for path in disallowed:
                if url.startswith(f"{parsed.scheme}://{parsed.netloc}{path}"):
                    print(f"Explicitly disallowed by robots.txt: {path}")
                    return False
            print("Not explicitly disallowed by robots.txt. Scraping allowed.")
            return True
        else:
            print(f"Could not fetch robots.txt, status code: {resp.status_code}")
            return True
    except Exception as e:
        print(f"Could not read robots.txt: {e}")
        return True

def scrape_helmet_laws():
    url = os.getenv("HELMET_LAWS_URL")
    if not can_scrape(url):
        print("Scraping not allowed by robots.txt.")
        return None
    response = requests.get(url)
    if response.status_code != 200:
        print(f"Failed to fetch page: {response.status_code}")
        return None
    soup = BeautifulSoup(response.text, "html.parser")
    table = soup.find("table")
    if not table:
        print("Helmet laws table not found.")
        return None
    
    state_names = [
        "Alabama", "Alaska", "Arizona", "Arkansas", "California", "Colorado", "Connecticut", "Delaware", 
        "District of Columbia", "Florida", "Georgia", "Hawaii", "Idaho", "Illinois", "Indiana", "Iowa", 
        "Kansas", "Kentucky", "Louisiana", "Maine", "Maryland", "Massachusetts", "Michigan", "Minnesota", 
        "Mississippi", "Missouri", "Montana", "Nebraska", "Nevada", "New Hampshire", "New Jersey", 
        "New Mexico", "New York", "North Carolina", "North Dakota", "Ohio", "Oklahoma", "Oregon", 
        "Pennsylvania", "Rhode Island", "South Carolina", "South Dakota", "Tennessee", "Texas", "Utah", 
        "Vermont", "Virginia", "Washington", "West Virginia", "Wisconsin", "Wyoming"
    ]
    all_rows = table.find_all("tr")[1:]
    print(f"Found {len(all_rows)} data rows in table.")
    for row_index, row in enumerate(all_rows[:5]):
        cells = [table_data.get_text(strip=True) for table_data in row.find_all("td")]
        print(f"Row {row_index} cells: {cells}")

    data = []
    footnote_pattern = re.compile(r"Footnote(\d+)")
    for row_index, row in enumerate(all_rows):
        cells = [td.get_text(strip=True) for td in row.find_all("td")]
        footnotes = []
        helmet_law = cells[0] if len(cells) > 0 else ""
        vehicle_law = cells[1] if len(cells) > 1 else ""
        for text in [helmet_law, vehicle_law]:
            matches = footnote_pattern.findall(text)
            footnotes.extend(matches)
        helmet_law_clean = footnote_pattern.sub("", helmet_law).strip()
        vehicle_law_clean = footnote_pattern.sub("", vehicle_law).strip()
        if len(cells) == 2 and row_index < len(state_names):
            data.append({
                "State": state_names[row_index],
                "Required to wear helmet": helmet_law_clean,
                "Motorcycle-type vehicles not covered": vehicle_law_clean,
                "Footnotes": ", ".join(footnotes) if footnotes else ""
            })
    return ["State", "Required to wear helmet", "Motorcycle-type vehicles not covered", "Footnotes"], data

def upload_csv_to_minio(headers, helmet_laws, minio_client, bucket_name, csv_filename):
    with tempfile.NamedTemporaryFile(mode='w', newline='', encoding='utf-8', delete=False) as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=headers)
        writer.writeheader()
        writer.writerows(helmet_laws)
        temp_csv_path = csvfile.name
    minio_csv_path = f"raw/{csv_filename}"
    found = minio_client.bucket_exists(bucket_name)
    if not found:
        minio_client.make_bucket(bucket_name)
    minio_client.fput_object(bucket_name, minio_csv_path, temp_csv_path)
    print(f"Uploaded CSV to MinIO: {bucket_name}/{minio_csv_path}")
    return minio_csv_path

def csv_to_parquet_and_upload(minio_client, bucket_name, minio_csv_path, parquet_filename):
            response = minio_client.get_object(bucket_name, minio_csv_path)
            csv_bytes = response.read()
            helmet_law_csv = pd.read_csv(io.BytesIO(csv_bytes))
            with tempfile.NamedTemporaryFile(suffix='.parquet', delete=False) as pqfile:
                helmet_law_csv.to_parquet(pqfile.name, index=False)
                temp_parquet_path = pqfile.name
            minio_parquet_path = f"parquet/{parquet_filename}"
            minio_client.fput_object(bucket_name, minio_parquet_path, temp_parquet_path)
            print(f"Uploaded Parquet to MinIO: {bucket_name}/{minio_parquet_path}")

        

def main():
    result = scrape_helmet_laws()
    if not result:
        return
    headers, helmet_laws = result
    date_str = datetime.now().strftime('%Y%m%d')
    csv_filename = f"motorcycle_helmet_laws_{date_str}.csv"
    parquet_filename = f"motorcycle_helmet_laws_{date_str}.parquet"
    minio_client = get_minio_client()
    bucket_name = os.getenv("MINIO_BUCKET_NAME", "default-bucket")
    minio_csv_path = upload_csv_to_minio(headers, helmet_laws, minio_client, bucket_name, csv_filename)
    csv_to_parquet_and_upload(minio_client, bucket_name, minio_csv_path, parquet_filename)


if __name__ == "__main__":
    main()
