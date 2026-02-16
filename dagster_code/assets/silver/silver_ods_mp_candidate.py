import json
import pandas as pd
from dagster import asset
import s3fs
from datetime import datetime


BUCKET = "thailand-election2026"


@asset(
    deps=["bronze_mp_candidate"],
    required_resource_keys={"s3"},
    group_name="silver",
)
def ods_mp_candidate(context):

    s3_client = context.resources.s3

    fs = s3fs.S3FileSystem(
        key=s3_client._request_signer._credentials.access_key,
        secret=s3_client._request_signer._credentials.secret_key,
        client_kwargs={"endpoint_url": s3_client.meta.endpoint_url},
    )

    # ---------- 1️⃣ หา bronze ล่าสุด ----------
    bronze_prefix = f"{BUCKET}/bronze/election_api/mp_candidate/"
    files = fs.glob(f"{bronze_prefix}**/*.json")

    if not files:
        raise Exception("No bronze mp_candidate files found")

    latest_file = sorted(files)[-1]
    context.log.info(f"Reading bronze file: {latest_file}")

    # ---------- 2️⃣ อ่าน JSON ----------
    with fs.open(latest_file, "r") as f:
        data = json.load(f)

    payload = data.get("payload", [])
    df = pd.DataFrame(payload)

    if df.empty:
        raise ValueError("Payload is empty")

    # ---------- 3️⃣ Transform ----------
    df = df.rename(columns={
        "mp_app_id": "mp_candidate_id",
        "mp_app_no": "candidate_no",
        "mp_app_party_id": "party_id",
        "mp_app_name": "candidate_name",
    })

    # safe numeric casting
    df["candidate_no"] = (
        pd.to_numeric(df["candidate_no"], errors="coerce")
        .astype("Int64")
    )

    df["party_id"] = (
        pd.to_numeric(df["party_id"], errors="coerce")
        .astype("Int64")
    )

    # data quality log
    context.log.info(f"Null candidate_no: {df['candidate_no'].isna().sum()}")
    context.log.info(f"Null party_id: {df['party_id'].isna().sum()}")

    # ---------- 4️⃣ ingestion_date ----------
    ingestion_date = datetime.utcnow().strftime("%Y-%m-%d")
    df["ingestion_date"] = ingestion_date

    # ---------- 5️⃣ Select columns ----------
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

    # ---------- 6️⃣ Write Parquet (partitioned) ----------
    silver_path = (
        f"{BUCKET}/silver/ods_mp_candidate/"
        f"ingestion_date={ingestion_date}/"
        f"part-{datetime.utcnow().strftime('%H%M%S')}.parquet"
    )

    context.log.info(f"Writing structured data to: {silver_path}")

    with fs.open(silver_path, "wb") as f:
        df.to_parquet(f, index=False, engine="pyarrow")

    return silver_path
