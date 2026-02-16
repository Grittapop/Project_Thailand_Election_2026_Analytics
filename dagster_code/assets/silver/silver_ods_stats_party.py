import json
import pandas as pd
from dagster import asset
import s3fs
from datetime import datetime


BUCKET = "thailand-election2026"


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
    deps=["bronze_stats_party"],
    required_resource_keys={"s3"},
    group_name="silver",
)
def ods_stats_party(context):

    s3_client = context.resources.s3

    fs = s3fs.S3FileSystem(
        key=s3_client._request_signer._credentials.access_key,
        secret=s3_client._request_signer._credentials.secret_key,
        client_kwargs={"endpoint_url": s3_client.meta.endpoint_url},
    )

    # ---------- 1️⃣ หา bronze ล่าสุด ----------
    bronze_prefix = f"{BUCKET}/bronze/election_api/stats_party/"
    files = fs.glob(f"{bronze_prefix}**/*.json")

    if not files:
        raise Exception("No bronze stats_party files found")

    latest_file = sorted(files)[-1]
    context.log.info(f"Reading bronze file: {latest_file}")

    # ---------- 2️⃣ อ่าน JSON ----------
    with fs.open(latest_file, "r") as f:
        data = json.load(f)

    payload = data.get("payload", {})

    counted_vote_stations = safe_int(payload.get("counted_vote_stations"))
    percent_count = safe_float(payload.get("percent_count"))

    result_party = payload.get("result_party", [])

    rows = []

    # ---------- 3️⃣ Flatten ----------
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
        raise Exception("No party stats rows created")

    df = pd.DataFrame(rows)

    # ---------- 4️⃣ Clean types ----------
    int_cols = [
        "party_id",
        "party_vote",
        "mp_app_vote",
        "first_mp_app_count",
        "counted_vote_stations",
    ]

    for col in int_cols:
        df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")

    float_cols = [
        "party_vote_percent",
        "mp_app_vote_percent",
        "percent_count",
    ]

    for col in float_cols:
        df[col] = pd.to_numeric(df[col], errors="coerce").astype("Float64")

    # ---------- 5️⃣ ingestion_date ----------
    ingestion_date = datetime.utcnow().strftime("%Y-%m-%d")
    df["ingestion_date"] = ingestion_date

    # ---------- 6️⃣ Write Parquet ----------
    silver_path = (
        f"{BUCKET}/silver/ods_stats_party/"
        f"ingestion_date={ingestion_date}/"
        f"part-{datetime.utcnow().strftime('%H%M%S')}.parquet"
    )

    context.log.info(f"Writing structured data to: {silver_path}")

    with fs.open(silver_path, "wb") as f:
        df.to_parquet(f, index=False, engine="pyarrow")

    return silver_path
