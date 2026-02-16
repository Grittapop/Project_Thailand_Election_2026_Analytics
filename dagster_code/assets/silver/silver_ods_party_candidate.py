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


@asset(
    deps=["bronze_party_candidate"],
    required_resource_keys={"s3"},
    group_name="silver",
)
def ods_party_candidate(context):

    s3_client = context.resources.s3

    fs = s3fs.S3FileSystem(
        key=s3_client._request_signer._credentials.access_key,
        secret=s3_client._request_signer._credentials.secret_key,
        client_kwargs={"endpoint_url": s3_client.meta.endpoint_url},
    )

    # ---------- 1️⃣ หา bronze ล่าสุด ----------
    bronze_prefix = f"{BUCKET}/bronze/election_api/party_candidate/"
    files = fs.glob(f"{bronze_prefix}**/*.json")

    if not files:
        raise Exception("No bronze party_candidate files found")

    latest_file = sorted(files)[-1]
    context.log.info(f"Reading bronze file: {latest_file}")

    # ---------- 2️⃣ อ่าน JSON ----------
    with fs.open(latest_file, "r") as f:
        data = json.load(f)

    payload = data.get("payload", [])

    rows = []

    # ---------- 3️⃣ Flatten ----------
    for party in payload:

        party_no = safe_int(party.get("party_no"))

        for candidate in party.get("party_list_candidates", []):
            rows.append({
                "party_no": party_no,
                "list_no": safe_int(candidate.get("list_no")),
                "candidate_name": candidate.get("name"),
                "image_url": candidate.get("image_url"),
            })

    if not rows:
        raise ValueError("No party candidate rows found")

    df = pd.DataFrame(rows)

    # ใช้ nullable integer
    df["party_no"] = pd.to_numeric(df["party_no"], errors="coerce").astype("Int64")
    df["list_no"] = pd.to_numeric(df["list_no"], errors="coerce").astype("Int64")

    # ---------- 4️⃣ ingestion_date ----------
    ingestion_date = datetime.utcnow().strftime("%Y-%m-%d")
    df["ingestion_date"] = ingestion_date

    df = df[
        [
            "party_no",
            "list_no",
            "candidate_name",
            "image_url",
            "ingestion_date",
        ]
    ]

    # ---------- 5️⃣ Write Parquet (partitioned) ----------
    silver_path = (
        f"{BUCKET}/silver/ods_party_candidate/"
        f"ingestion_date={ingestion_date}/"
        f"part-{datetime.utcnow().strftime('%H%M%S')}.parquet"
    )

    context.log.info(f"Writing structured data to: {silver_path}")

    with fs.open(silver_path, "wb") as f:
        df.to_parquet(f, index=False, engine="pyarrow")

    return silver_path
