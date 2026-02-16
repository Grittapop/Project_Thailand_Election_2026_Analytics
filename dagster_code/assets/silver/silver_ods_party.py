import json
import pandas as pd
from dagster import asset
import s3fs


BUCKET = "thailand-election2026"


def safe_int(value):
    try:
        return int(value) if value not in (None, "") else None
    except:
        return None


@asset(
    deps=["bronze_party"],
    required_resource_keys={"s3"},
    group_name="silver",
)
def ods_party(context):

    s3_client = context.resources.s3

    fs = s3fs.S3FileSystem(
        key=s3_client._request_signer._credentials.access_key,
        secret=s3_client._request_signer._credentials.secret_key,
        client_kwargs={"endpoint_url": s3_client.meta.endpoint_url},
    )

    # ---------- 1️⃣ หา bronze ล่าสุด ----------
    bronze_prefix = f"{BUCKET}/bronze/election_api/party/"
    files = fs.glob(f"{bronze_prefix}**/*.json")

    if not files:
        raise Exception("No bronze party files found")

    latest_file = sorted(files)[-1]
    context.log.info(f"Reading bronze file: {latest_file}")

    # ---------- 2️⃣ อ่าน JSON ----------
    with fs.open(latest_file, "r") as f:
        data = json.load(f)

    payload = data.get("payload", [])

    df = pd.DataFrame(payload)

    # ---------- 3️⃣ Transform ----------
    df = df.rename(columns={
        "id": "party_id",
        "name": "party_name",
        "abbr": "party_abbr",
        "color": "party_color",
    })

    df["party_id"] = df["party_id"].apply(safe_int)
    df["party_no"] = df["party_no"].apply(safe_int)

    df = df[
        [
            "party_id",
            "party_no",
            "party_name",
            "party_abbr",
            "party_color",
            "logo_url",
        ]
    ]

    # ---------- 4️⃣ เขียน Parquet ----------
    silver_path = (
        f"{BUCKET}/silver/ods_party/part-000.parquet"
    )

    context.log.info(f"Writing structured data to: {silver_path}")

    with fs.open(silver_path, "wb") as f:
        df.to_parquet(f, index=False, engine="pyarrow")

    return silver_path

import json
import pandas as pd
from dagster import asset
import s3fs
from datetime import datetime


BUCKET = "thailand-election2026"


@asset(
    deps=["bronze_party"],
    required_resource_keys={"s3"},
    group_name="silver",
)
def ods_party(context):

    s3_client = context.resources.s3

    fs = s3fs.S3FileSystem(
        key=s3_client._request_signer._credentials.access_key,
        secret=s3_client._request_signer._credentials.secret_key,
        client_kwargs={"endpoint_url": s3_client.meta.endpoint_url},
    )

    # ---------- 1️⃣ หา bronze ล่าสุด ----------
    bronze_prefix = f"{BUCKET}/bronze/election_api/party/"
    files = fs.glob(f"{bronze_prefix}**/*.json")

    if not files:
        raise Exception("No bronze party files found")

    latest_file = sorted(files)[-1]
    context.log.info(f"Reading bronze file: {latest_file}")

    # ---------- 2️⃣ อ่าน JSON ----------
    with fs.open(latest_file, "r") as f:
        data = json.load(f)

    payload = data.get("payload", [])
    df = pd.DataFrame(payload)

    if df.empty:
        raise ValueError("Party payload is empty")

    # ---------- 3️⃣ Transform ----------
    df = df.rename(columns={
        "id": "party_id",
        "name": "party_name",
        "abbr": "party_abbr",
        "color": "party_color",
    })

    # safe numeric casting
    df["party_id"] = pd.to_numeric(df["party_id"], errors="coerce").astype("Int64")
    df["party_no"] = pd.to_numeric(df["party_no"], errors="coerce").astype("Int64")

    # ---------- 4️⃣ ingestion_date ----------
    ingestion_date = datetime.utcnow().strftime("%Y-%m-%d")
    df["ingestion_date"] = ingestion_date

    df = df[
        [
            "party_id",
            "party_no",
            "party_name",
            "party_abbr",
            "party_color",
            "logo_url",
            "ingestion_date",
        ]
    ]

    # ---------- 5️⃣ Write Parquet ----------
    silver_path = (
        f"{BUCKET}/silver/ods_party/"
        f"ingestion_date={ingestion_date}/"
        f"part-{datetime.utcnow().strftime('%H%M%S')}.parquet"
    )

    context.log.info(f"Writing structured data to: {silver_path}")

    with fs.open(silver_path, "wb") as f:
        df.to_parquet(f, index=False, engine="pyarrow")

    return silver_path
