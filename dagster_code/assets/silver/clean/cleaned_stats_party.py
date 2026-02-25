import json
import pandas as pd
from dagster import asset
from datetime import datetime

BUCKET = "thailand-election2026"
BRONZE_PREFIX = "bronze/election_api/stats_party/"


def safe_int(value):
    try:
        return int(value) if value not in (None, "") else None
    except (ValueError, TypeError):
        return None


def safe_float(value):
    try:
        return float(value) if value not in (None, "") else None
    except (ValueError, TypeError):
        return None


@asset(
    required_resource_keys={"s3"},
    group_name="silver",
)
def cleaned_stats_party(context, raw_stats_party):

    s3 = context.resources.s3

    # Read latest bronze file
    paginator = s3.get_paginator("list_objects_v2")
    pages = paginator.paginate(Bucket=BUCKET, Prefix=BRONZE_PREFIX)

    keys = [
        obj["Key"]
        for page in pages
        for obj in page.get("Contents", [])
        if obj["Key"].endswith(".json")
    ]

    if not keys:
        raise Exception("No bronze stats_party files found")

    latest_key = sorted(keys)[-1]
    context.log.info(f"Reading bronze file: {latest_key}")

    obj = s3.get_object(Bucket=BUCKET, Key=latest_key)
    data = json.loads(obj["Body"].read())

    payload = data.get("payload", {})

    counted_vote_stations = safe_int(payload.get("counted_vote_stations"))
    percent_count = safe_float(payload.get("percent_count"))
    result_party = payload.get("result_party", [])

    # Flatten
    rows = []

    for party in result_party:
        rows.append({
            "party_id": safe_int(party.get("party_id")),
            "party_vote": safe_int(party.get("party_vote")),
            "party_vote_percent": safe_float(party.get("party_vote_percent")),
            "mp_app_vote": safe_int(party.get("mp_app_vote")),
            "mp_app_vote_percent": safe_float(party.get("mp_app_vote_percent")),
            "first_mp_app_count": safe_int(party.get("first_mp_app_count")),
            "counted_vote_stations": counted_vote_stations,
            "percent_count": percent_count,
        })

    if not rows:
        context.log.info("No rows after flatten")
        return pd.DataFrame()

    df = pd.DataFrame(rows)

    ingestion_date = datetime.utcnow().date()
    df["ingestion_date"] = ingestion_date

    df = df.where(pd.notnull(df), None)

    context.log.info(f"Cleaned {len(df)} rows")

    return df