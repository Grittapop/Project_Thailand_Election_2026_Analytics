import json
import pandas as pd
from dagster import asset
from datetime import datetime

BUCKET = "thailand-election2026"
BRONZE_PREFIX = "bronze/election_api/mp_candidate/"


@asset(
    required_resource_keys={"s3"},
    group_name="silver",
)
def cleaned_mp_candidate(context, raw_mp_candidate):

    s3 = context.resources.s3

    # -------------------------------------------------
    # 1️⃣ Load Bronze
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
        raise Exception("No bronze mp_candidate files found")

    latest_key = sorted(keys)[-1]
    context.log.info(f"Reading bronze file: {latest_key}")

    obj = s3.get_object(Bucket=BUCKET, Key=latest_key)
    data = json.loads(obj["Body"].read())

    df = pd.DataFrame(data.get("payload", []))
    if df.empty:
        context.log.info("No rows found in payload")
        return pd.DataFrame()

    # -------------------------------------------------
    # 2️⃣ Transform
    # -------------------------------------------------
    df = df.rename(columns={
        "mp_app_id": "mp_candidate_id",
        "mp_app_no": "candidate_no",
        "mp_app_party_id": "party_id",
        "mp_app_name": "candidate_name",
    })

    df["candidate_no"] = (
        pd.to_numeric(df.get("candidate_no"), errors="coerce")
        .fillna(0)
        .astype(int)
    )

    df["party_id"] = (
        pd.to_numeric(df.get("party_id"), errors="coerce")
        .fillna(0)
        .astype(int)
    )

    ingestion_date = datetime.utcnow().date()
    df["ingestion_date"] = ingestion_date

    df = df[
        [
            "mp_candidate_id",
            "candidate_no",
            "party_id",
            "candidate_name",
            "image_url",
            "ingestion_date",
        ]
    ]

    df = df.where(pd.notnull(df), None)

    context.log.info(f"Cleaned {len(df)} rows")

    return df