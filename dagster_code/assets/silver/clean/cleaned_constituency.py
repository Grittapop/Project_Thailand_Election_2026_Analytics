import json
import pandas as pd
from dagster import asset
from datetime import datetime

BUCKET = "thailand-election2026"
BRONZE_PREFIX = "bronze/election_api/constituency/"


@asset(
    required_resource_keys={"s3"},
    group_name="silver",
)
def cleaned_constituency(context, raw_constituency):

    s3 = context.resources.s3

    # -------------------------------------------------
    # 1️⃣ Read Latest Bronze JSON
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
        raise Exception("No bronze constituency files found")

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
        "cons_id": "constituency_id",
        "cons_no": "constituency_no",
        "prov_id": "province_id",
    })

    df["constituency_no"] = (
        pd.to_numeric(df["constituency_no"], errors="coerce")
        .fillna(0)
        .astype(int)
    )

    df["total_vote_stations"] = (
        pd.to_numeric(df.get("total_vote_stations"), errors="coerce")
        .fillna(0)
        .astype(int)
    )

    df["registered_vote"] = (
        pd.to_numeric(df.get("registered_vote"), errors="coerce")
        .fillna(0)
        .astype(int)
    )

    if "zone" in df.columns:
        df["zone"] = df["zone"].apply(
            lambda x: ", ".join(x) if isinstance(x, list) else None
        )
    else:
        df["zone"] = None

    ingestion_date = datetime.utcnow().date()
    df["ingestion_date"] = ingestion_date

    df = df[
        [
            "constituency_id",
            "constituency_no",
            "province_id",
            "zone",
            "total_vote_stations",
            "registered_vote",
            "ingestion_date",
        ]
    ]

    df = df.where(pd.notnull(df), None)

    context.log.info(f"Cleaned {len(df)} rows")

    return df