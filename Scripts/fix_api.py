from BCDA_API import get_access_token
import requests

access_token = get_access_token()
# Example: job_url from start_export_job() Content-Location header
job_url = "https://api.bcda.cms.gov/api/v2/jobs"

response = requests.get(
    job_url,
    headers={
        "Accept": "application/fhir+json",
        "Prefer": "respond-async",
        "Authorization": f"Bearer {access_token}"
    },
)

print("HTTP Status:", response.status_code)
try:
    print(response.json())  # actual job status
except Exception:
    print(response.text)    # fallback if not JSON