import requests
import json
import uuid
from datetime import datetime
from dagster import asset


API_URL = "https://static-ectreport69.ect.go.th/data/data/refs/info_mp_candidate.json"
BUCKET_NAME = "thailand-election2026"


@asset(required_resource_keys={"s3"},
       group_name="bronze",
)
def raw_mp_candidate(context):
    """
    Bronze layer - MP Candidate (Raw JSON)
    """

    # Call API
    response = requests.get(API_URL, timeout=30)
    response.raise_for_status()
    payload = response.json()

    # Metadata
    now = datetime.utcnow()
    ingestion_date = now.strftime("%Y-%m-%d")
    timestamp_str = now.strftime("%Y%m%dT%H%M%S")
    short_uuid = str(uuid.uuid4())[:8]

    filename = f"{timestamp_str}_{short_uuid}.json"

    record = {
        "metadata": {
            "dataset": "mp_candidate",
            "ingestion_timestamp": now.isoformat() + "Z",
            "source": API_URL,
            "status_code": response.status_code,
            "batch_id": short_uuid,
        },
        "payload": payload,
    }

    # S3 Path
    file_key = (
        f"bronze/election_api/mp_candidate/"
        f"ingestion_date={ingestion_date}/"
        f"{filename}"
    )

    # Upload
    s3 = context.resources.s3

    s3.put_object(
        Bucket=BUCKET_NAME,
        Key=file_key,
        Body=json.dumps(record),
        ContentType="application/json",
    )

    context.log.info(f"Saved to s3://{BUCKET_NAME}/{file_key}")

    return file_key
