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
    deps=["bronze_stats_cons"],
    required_resource_keys={"s3"},
    group_name="silver",
)
def fact_vote_constituency_candidate(context):

    s3_client = context.resources.s3

    fs = s3fs.S3FileSystem(
        key=s3_client._request_signer._credentials.access_key,
        secret=s3_client._request_signer._credentials.secret_key,
        client_kwargs={"endpoint_url": s3_client.meta.endpoint_url},
    )

    # ---------- 1️⃣ หา bronze ล่าสุด ----------
    bronze_prefix = f"{BUCKET}/bronze/election_api/stats_cons/"
    files = fs.glob(f"{bronze_prefix}**/*.json")

    if not files:
        raise Exception("No bronze stats_cons files found")

    latest_file = sorted(files)[-1]
    context.log.info(f"Reading bronze file: {latest_file}")

    # ---------- 2️⃣ อ่าน JSON ----------
    with fs.open(latest_file, "r") as f:
        data = json.load(f)

    provinces = data.get("payload", {}).get("result_province", [])

    rows = []

    # ---------- 3️⃣ Flatten ----------
    for province in provinces:
        province_id = province.get("prov_id")

        for cons in province.get("constituencies", []):

            constituency_id = cons.get("cons_id")

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
        raise Exception("No rows created")

    df = pd.DataFrame(rows)

    # ---------- 4️⃣ Clean types ----------
    df["province_id"] = df["province_id"].astype(str)

    int_cols = [
        "party_id",
        "vote",
        "rank",
        "turn_out",
        "valid_votes",
        "invalid_votes",
        "blank_votes",
    ]

    for col in int_cols:
        df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")

    df["vote_percent"] = pd.to_numeric(
        df["vote_percent"], errors="coerce"
    ).astype("Float64")

    df["percent_turn_out"] = pd.to_numeric(
        df["percent_turn_out"], errors="coerce"
    ).astype("Float64")

    context.log.info(f"Total rows created: {len(df)}")

    # ---------- 5️⃣ ingestion_date ----------
    ingestion_date = datetime.utcnow().strftime("%Y-%m-%d")
    df["ingestion_date"] = ingestion_date

    # ---------- 6️⃣ Write Parquet ----------
    silver_path = (
        f"{BUCKET}/silver/ods_stats_cons/"
        f"ingestion_date={ingestion_date}/"
        f"part-{datetime.utcnow().strftime('%H%M%S')}.parquet"
    )

    context.log.info(f"Writing structured data to: {silver_path}")

    with fs.open(silver_path, "wb") as f:
        df.to_parquet(f, index=False, engine="pyarrow")

    return silver_path


