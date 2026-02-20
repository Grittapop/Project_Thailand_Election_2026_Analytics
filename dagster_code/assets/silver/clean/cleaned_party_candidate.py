import json
import pandas as pd
from dagster import asset
from datetime import datetime

BUCKET = "thailand-election2026"
BRONZE_PREFIX = "bronze/election_api/party_candidate/"


def safe_int(value):
    try:
        return int(value) if value not in (None, "") else None
    except (ValueError, TypeError):
        return None


@asset(
    required_resource_keys={"s3"},
    group_name="silver",
)
def cleaned_party_candidate(context, raw_party_candidate):

    s3 = context.resources.s3

    # -------------------------------------------------
    # 1️⃣ Read latest bronze JSON
    # -------------------------------------------------
    paginator = s3.get_paginator("list_objects_v2")
    pages = paginator.paginate(Bucket=BUCKET, Prefix=BRONZE_PREFIX)

    keys = [
        obj["Key"]
        for page in pages
        for obj in page.get("Contents", [])
        if obj["Key"].endswith(".json")
    ]

    if not keys:
        raise Exception("No bronze party_candidate files found")

    latest_key = sorted(keys)[-1]
    context.log.info(f"Reading bronze file: {latest_key}")

    obj = s3.get_object(Bucket=BUCKET, Key=latest_key)
    data = json.loads(obj["Body"].read())
    payload = data.get("payload", [])

    # -------------------------------------------------
    # 2️⃣ Flatten
    # -------------------------------------------------
    rows = []

    for party in payload:
        party_no = safe_int(party.get("party_no"))

        for candidate in party.get("party_list_candidates", []):
            rows.append({
                "party_no": party_no,
                "list_no": safe_int(candidate.get("list_no")),
                "candidate_name": candidate.get("name"),
                "image_url": candidate.get("image_url"),
            })

    if not rows:
        context.log.info("No rows found after flatten")
        return pd.DataFrame()

    df = pd.DataFrame(rows)

    ingestion_date = datetime.utcnow().date()
    df["ingestion_date"] = ingestion_date

    df = df[
        [
            "party_no",
            "list_no",
            "candidate_name",
            "image_url",
            "ingestion_date",
        ]
    ]

    df = df.where(pd.notnull(df), None)

    context.log.info(f"Cleaned {len(df)} rows")

    return df