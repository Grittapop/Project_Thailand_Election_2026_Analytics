import json
import pandas as pd
from dagster import asset
import s3fs
from datetime import datetime

BUCKET = "thailand-election2026"


@asset(
    deps=["bronze_province"],
    required_resource_keys={"s3"},
    group_name="silver",
)
def ods_province(context):

    s3_client = context.resources.s3

    fs = s3fs.S3FileSystem(
        key=s3_client._request_signer._credentials.access_key,
        secret=s3_client._request_signer._credentials.secret_key,
        client_kwargs={"endpoint_url": s3_client.meta.endpoint_url},
    )

    # ---------- 1️⃣ หา bronze ล่าสุด ----------
    bronze_prefix = f"{BUCKET}/bronze/election_api/province/"
    files = fs.glob(f"{bronze_prefix}**/*.json")

    if not files:
        raise Exception("No bronze province files found")

    latest_file = sorted(files)[-1]
    context.log.info(f"Reading bronze file: {latest_file}")

    # ---------- 2️⃣ อ่าน JSON ----------
    with fs.open(latest_file, "r") as f:
        data = json.load(f)

    provinces = data.get("payload", {}).get("province", [])

    if not provinces:
        raise ValueError("Province payload empty")

    df = pd.DataFrame(provinces)

    # ---------- 3️⃣ Transform ----------
    df = df[
        [
            "province_id",
            "prov_id",
            "province",
            "abbre_thai",
            "eng",
        ]
    ].copy()

    df["province_id"] = pd.to_numeric(
        df["province_id"], errors="coerce"
    ).astype("Int64")

    df["prov_id"] = df["prov_id"].astype(str)

    # ---------- 4️⃣ ingestion_date ----------
    ingestion_date = datetime.utcnow().strftime("%Y-%m-%d")
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

    # ---------- 5️⃣ Write Parquet ----------
    silver_path = (
        f"{BUCKET}/silver/ods_province/"
        f"ingestion_date={ingestion_date}/"
        f"part-{datetime.utcnow().strftime('%H%M%S')}.parquet"
    )

    context.log.info(f"Writing structured data to: {silver_path}")

    with fs.open(silver_path, "wb") as f:
        df.to_parquet(f, index=False, engine="pyarrow")

    return silver_path






