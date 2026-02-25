import json
import pandas as pd
from dagster import asset
from datetime import datetime

BUCKET = "thailand-election2026"
BRONZE_PREFIX = "bronze/election_api/stats_cons/"


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
def cleaned_stats_cons(context, raw_stats_cons):

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
        raise Exception("No bronze stats_cons files found")

    latest_key = sorted(keys)[-1]
    context.log.info(f"Reading bronze file: {latest_key}")

    obj = s3.get_object(Bucket=BUCKET, Key=latest_key)
    data = json.loads(obj["Body"].read())

    provinces = data.get("payload", {}).get("result_province", [])

    rows = []

    # Flatten
    for province in provinces:

        prov_raw = province.get("prov_id")
        province_id = str(prov_raw) if prov_raw not in (None, "") else None

        for cons in province.get("constituencies", []):

            cons_raw = cons.get("cons_id")
            constituency_id = str(cons_raw) if cons_raw not in (None, "") else None

            turn_out = safe_int(cons.get("turn_out"))
            percent_turn_out = safe_float(cons.get("percent_turn_out"))
            valid_votes = safe_int(cons.get("valid_votes"))
            invalid_votes = safe_int(cons.get("invalid_votes"))
            blank_votes = safe_int(cons.get("blank_votes"))

            for candidate in cons.get("candidates", []):

                rows.append({
                    "constituency_id": constituency_id,
                    "province_id": province_id,
                    "party_id": safe_int(candidate.get("party_id")),
                    "vote": safe_int(candidate.get("mp_app_vote")),
                    "vote_percent": safe_float(candidate.get("mp_app_vote_percent")),
                    "rank": safe_int(candidate.get("mp_app_rank")),
                    "turn_out": turn_out,
                    "percent_turn_out": percent_turn_out,
                    "valid_votes": valid_votes,
                    "invalid_votes": invalid_votes,
                    "blank_votes": blank_votes,
                })

    if not rows:
        context.log.info("No rows after flatten")
        return pd.DataFrame()

    df = pd.DataFrame(rows)

    # add ingestion_date
    ingestion_date = datetime.utcnow().date()
    df["ingestion_date"] = ingestion_date

    # enforce column order (กัน order เพี้ยนตอน insert)
    df = df[
        [
            "constituency_id",
            "province_id",
            "party_id",
            "vote",
            "vote_percent",
            "rank",
            "turn_out",
            "percent_turn_out",
            "valid_votes",
            "invalid_votes",
            "blank_votes",
            "ingestion_date",
        ]
    ]

    # The NaN → None format prevents Trino errors
    df = df.where(pd.notnull(df), None)

    context.log.info(f"Cleaned {len(df)} rows")

    return df