import requests
import time
import sys
from datetime import datetime, timedelta, timezone
import zipfile
import os
from pathlib import Path
from Credentials import CLIENT_ID, CLIENT_SECRET, engine_DEV_Final as engine
from sqlalchemy import text
import pandas as pd
from concurrent.futures import ThreadPoolExecutor, as_completed

Zip_path = Path(r"C:\BCDA\Data")

Zip_path.mkdir(parents=True, exist_ok=True)

os.chdir(Zip_path)

onedrive = next(p for p in Path(os.environ["USERPROFILE"]).iterdir()
                if p.name.startswith("OneDrive - "))

query = """
    Select top 1 watermark
    from BCDA_data.dbo.watermark
    order by id desc
"""

timestamp = pd.read_sql(query,
    engine
)
timestamp = timestamp['watermark'].iloc[0]
timestamp = pd.to_datetime(timestamp).isoformat()

print(timestamp)
# ============================================================
# Configuration
# ============================================================

TOKEN_URL = "https://api.bcda.cms.gov/auth/token"
EXPORT_URL = f"https://api.bcda.cms.gov/api/v2/Patient/$export"

params = {
    "_since": timestamp
}

TIMEOUT_SECONDS = 36000  # 1 hour max for polling

# ============================================================
# Authentication
# ============================================================

def get_access_token():
    print("Authenticating...")
    response = requests.post(
        TOKEN_URL,
        auth=(CLIENT_ID, CLIENT_SECRET),
        headers={"Accept": "application/json"},
        data={"grant_type": "client_credentials"},
        timeout=30,
    )

    print("Token HTTP Status:", response.status_code)
    response.raise_for_status()

    token_json = response.json()
    access_token = token_json.get("access_token")
    if not access_token:
        print("No access_token found in token response:", token_json)
        sys.exit(1)

    print("Access token acquired (prefix):", access_token[:20], "...")
    return access_token

# ============================================================
# Start Export Job
# ============================================================

def start_export_job(access_token):
    headers = {
        "Accept": "application/fhir+json",
        "Prefer": "respond-async",
        "Authorization": f"Bearer {access_token}",
    }

    print("Starting export job...")
    response = requests.get(EXPORT_URL, headers=headers, params=params, timeout=60)

    print("Export Start HTTP Status:", response.status_code)
    print("Export Start Response Preview:", response.text[:200])

    if response.status_code != 202:
        print("Failed to start export job. Status:", response.status_code)
        print("Body:", response.text)
        sys.exit(1)

    job_url = response.headers.get("Content-Location")
    if not job_url:
        print("No Content-Location header found in export response.")
        sys.exit(1)

    print("Job Tracking URL:", job_url)
    return job_url

# ============================================================
# Poll Job Status
# ============================================================

def poll_job(job_url, headers):
    print("Polling job status...")
    start_time = time.time()
    backoff = 5  # initial backoff in seconds

    while True:

        if time.time() - start_time > TIMEOUT_SECONDS:
            raise TimeoutError(f"Polling timed out after {TIMEOUT_SECONDS} seconds")

        try:
            response = requests.get(job_url, headers=headers, timeout=60)
        except requests.exceptions.RequestException as e:
            print("Network error while polling:", e)
            print(f"Retrying in {backoff} seconds...")
            time.sleep(backoff)
            backoff = min(backoff * 2, 60)  # exponential backoff
            continue

        print("HTTP Status:", response.status_code)
        print("Response Text Preview:", response.text[:200])

        if response.status_code == 429:
            retry_after = int(response.headers.get("Retry-After", backoff))
            print(f"Rate limited. Waiting {retry_after} seconds before retrying...")
            time.sleep(retry_after)
            backoff = min(backoff * 2, 60)
            continue

        if response.status_code == 202:
            print("Job is still processing...")
            time.sleep(backoff)
            backoff = min(backoff * 2, 30)
            continue

        if response.status_code == 200:
            try:
                job_data = response.json()
            except Exception as e:
                print("Error parsing JSON:", e)
                print("Raw body:", response.text)
                time.sleep(backoff)
                continue

            if "output" in job_data:
                print("Job completed, output found.")
                return job_data

            status = job_data.get("status")
            print("Parsed Job Status:", status)

            if status in ["completed", "failed"]:
                return job_data

            time.sleep(backoff)
            backoff = min(backoff * 2, 30)
            continue

        print("Unexpected status:", response.status_code)
        time.sleep(backoff)
        backoff = min(backoff * 2, 30)

def download_file(output_file, headers, ts, idx):
    file_type = output_file.get("type", "UnknownType")
    file_url = output_file.get("url")

    if not file_url:
        print("Missing URL:", output_file)
        return

    print(f"Downloading ({file_type}): {file_url}")

    max_retries = 5
    retry_count = 0
    backoff = 5  # initial backoff in seconds

    while retry_count < max_retries:
        try:
            response = requests.get(file_url, headers=headers, timeout=300)
        except requests.exceptions.RequestException as e:
            print("Download error:", e)
            print(f"Retrying in {backoff} seconds...")
            time.sleep(backoff)
            retry_count += 1
            backoff = min(backoff * 2, 60)
            continue

        if response.status_code == 200:
            break  # success

        if response.status_code == 429:
            retry_after = int(response.headers.get("Retry-After", backoff))
            print(f"Rate limited on download. Waiting {retry_after} seconds...")
            time.sleep(retry_after)
            retry_count += 1
            backoff = min(backoff * 2, 60)
            continue

        print("Download failed:", response.status_code)
        return

    safe_type = "".join(c if c.isalnum() else "_" for c in file_type)
    filename = f"{safe_type}_{idx:0>4}_{ts}.ndjson"

    with open(filename, "wb") as f:
        f.write(response.content)

    print("Saved:", filename)
# ============================================================
# Parallel Download Controller
# ============================================================

def download_outputs(job_data, headers, ts):

    outputs = job_data.get("output", [])

    if not outputs:
        print("No output files found.")
        return

    with ThreadPoolExecutor(max_workers=6) as executor:

        futures = []

        for idx, output_file in enumerate(outputs, start=1):

            futures.append(
                executor.submit(
                    download_file,
                    output_file,
                    headers,
                    ts,
                    idx
                )
            )

        for future in as_completed(futures):
            future.result()

# ============================================================
# Zipping Files
# ============================================================

def zip_files(Zip_path, ts):

    storage_folder = Path(
        onedrive
        / "MedicareRawData_Files"
        / "BCDA_Data"
        / str(datetime.now().year)
        / datetime.now().strftime("%B")
    )

    storage_folder.mkdir(parents=True, exist_ok=True)

    zip_name = storage_folder / f"Patient_{ts}.zip"
    with zipfile.ZipFile(zip_name, 'w', compression=zipfile.ZIP_DEFLATED) as zipf:
        for file in Path(Zip_path).glob('Patient*.ndjson'):
            zipf.write(file, os.path.basename(file))

    zip_name = storage_folder / f"ExplanationOfBenefit_{ts}.zip"
    with zipfile.ZipFile(zip_name, 'w', compression=zipfile.ZIP_DEFLATED) as zipf:
        for file in Path(Zip_path).glob('ExplanationOfBenefit*.ndjson'):
            zipf.write(file, os.path.basename(file))

    zip_name = storage_folder / f"Coverage_{ts}.zip"
    with zipfile.ZipFile(zip_name, 'w', compression=zipfile.ZIP_DEFLATED) as zipf:
        for file in Path(Zip_path).glob('Coverage*.ndjson'):
            zipf.write(file, os.path.basename(file))

# ============================================================
# Watermark
# ============================================================

def update_watermark(engine):

    Watermark = datetime.now(timezone.utc).isoformat(timespec='milliseconds')
    date = pd.Timestamp.now()

    query = text("""
        insert into BCDA_Data.dbo.WaterMark
        (
            watermark,
            date
        )
        values
        (
            :watermark,
            :date
        )
    """)

    with engine.begin() as conn:

        conn.execute(query, {
            "watermark": Watermark,
            "date": date
        })

# ============================================================
# Main Pipeline
# ============================================================

def main():

    access_token = get_access_token()

    poll_headers = {
        "Accept": "application/fhir+json",
        "Authorization": f"Bearer {access_token}",
    }

    job_url = start_export_job(access_token)

    job_data = poll_job(job_url, poll_headers)

    print("Refreshing token before downloading outputs...")

    download_token = get_access_token()

    download_headers = {
        "Accept": "application/fhir+json",
        "Authorization": f"Bearer {download_token}",
    }

    ts = datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S")

    download_outputs(job_data, download_headers, ts)

    update_watermark(engine)

    zip_files(Zip_path, ts)

    print("Pipeline finished.")

# ============================================================

if __name__ == "__main__":
    main()