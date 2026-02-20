import json
import pandas as pd
from dagster import asset
from datetime import datetime

BUCKET = "thailand-election2026"
BRONZE_PREFIX = "bronze/election_api/province/"


@asset(
    required_resource_keys={"s3"},
    group_name="silver",
)
def cleaned_province(context, raw_province):

    s3 = context.resources.s3

    # -------------------------------------------------
    # 1️⃣ Read latest bronze file
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
        raise Exception("No bronze province files found")

    latest_key = sorted(keys)[-1]
    context.log.info(f"Reading bronze file: {latest_key}")

    obj = s3.get_object(Bucket=BUCKET, Key=latest_key)
    data = json.loads(obj["Body"].read())

    provinces = data.get("payload", {}).get("province", [])

    if not provinces:
        context.log.info("No province rows found")
        return pd.DataFrame()

    df = pd.DataFrame(provinces)

    # -------------------------------------------------
    # 2️⃣ Transform
    # -------------------------------------------------
    df = df[
        [
            "province_id",
            "prov_id",
            "province",
            "abbre_thai",
            "eng",
        ]
    ].copy()

    df["province_id"] = pd.to_numeric(df["province_id"], errors="coerce")
    df["prov_id"] = df["prov_id"].astype(str)

    ingestion_date = datetime.utcnow().date()
    df["ingestion_date"] = ingestion_date

    df = df[
        [
            "province_id",
            "prov_id",
            "province",
            "abbre_thai",
            "eng",
            "ingestion_date",
        ]
    ]

    df = df.where(pd.notnull(df), None)

    context.log.info(f"Cleaned {len(df)} rows")

    return df