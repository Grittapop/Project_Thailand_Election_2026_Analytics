import json
import pandas as pd
from dagster import asset
import s3fs
from datetime import datetime


BUCKET = "thailand-election2026"


@asset(
    deps=["bronze_constituency"],
    required_resource_keys={"s3"},
    group_name="silver",
)
def ods_constituency(context):

    s3_client = context.resources.s3

    fs = s3fs.S3FileSystem(
        key=s3_client._request_signer._credentials.access_key,
        secret=s3_client._request_signer._credentials.secret_key,
        client_kwargs={"endpoint_url": s3_client.meta.endpoint_url},
    )

    # ---------- 1️⃣ หา bronze ล่าสุด ----------
    bronze_prefix = f"{BUCKET}/bronze/election_api/constituency/"
    files = fs.glob(f"{bronze_prefix}**/*.json")

    if not files:
        raise Exception("No bronze constituency files found")

    latest_file = sorted(files)[-1]
    context.log.info(f"Reading bronze file: {latest_file}")

    # ---------- 2️⃣ อ่าน JSON ----------
    with fs.open(latest_file, "r") as f:
        data = json.load(f)

    payload = data.get("payload", [])

    df = pd.DataFrame(payload)

    # ---------- 3️⃣ Transform ----------
    df = df.rename(columns={
        "cons_id": "constituency_id",
        "cons_no": "constituency_no",
        "prov_id": "province_id",
    })

    df["constituency_no"] = pd.to_numeric(
        df["constituency_no"], errors="coerce"
    ).fillna(0).astype(int)

    df["total_vote_stations"] = pd.to_numeric(
        df.get("total_vote_stations"), errors="coerce"
    ).fillna(0).astype(int)

    df["registered_vote"] = pd.to_numeric(
        df.get("registered_vote"), errors="coerce"
    ).fillna(0).astype(int)

    # zone เป็น list → convert เป็น string
    if "zone" in df.columns:
        df["zone"] = df["zone"].apply(
            lambda x: ", ".join(x) if isinstance(x, list) else None
        )
    else:
        df["zone"] = None

    # ---------- 4️⃣ เพิ่ม ingestion_date ----------
    ingestion_date = datetime.utcnow().strftime("%Y-%m-%d")
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

    # ---------- 5️⃣ เขียน Parquet แบบ partition ----------
    silver_path = (
        f"{BUCKET}/silver/ods_constituency/"
        f"ingestion_date={ingestion_date}/"
        f"part-{datetime.utcnow().strftime('%H%M%S')}.parquet"
    )

    context.log.info(f"Writing structured data to: {silver_path}")

    with fs.open(silver_path, "wb") as f:
        df.to_parquet(f, index=False, engine="pyarrow")

    return silver_path

