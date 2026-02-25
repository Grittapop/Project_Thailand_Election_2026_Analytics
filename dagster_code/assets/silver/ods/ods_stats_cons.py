from dagster import asset
import math

BATCH_SIZE = 1000


def sql_val(v):
    if v is None:
        return "NULL"
    if isinstance(v, float) and math.isnan(v):
        return "NULL"
    if isinstance(v, str):
        return "'" + v.replace("'", "''") + "'"
    return str(v)


@asset(
    required_resource_keys={"trino"},
    group_name="silver",
)
def ods_stats_cons(context, cleaned_stats_cons):

    df = cleaned_stats_cons

    if df.empty:
        return "0 rows inserted"

    # Double safety protection against NaN (double safety)
    df = df.where(df.notnull(), None)

    trino = context.resources.trino
    cursor = trino.cursor()

    rows = list(df.itertuples(index=False, name=None))
    ingestion_date = df["ingestion_date"].iloc[0]

    try:
        # Idempotent delete
        cursor.execute(f"""
            DELETE FROM iceberg.silver.ods_stats_cons
            WHERE ingestion_date = DATE '{ingestion_date}'
        """)

        # Batch insert
        for i in range(0, len(rows), BATCH_SIZE):
            batch = rows[i:i+BATCH_SIZE]

            values_sql = ", ".join(
                [
                    "(" +
                    ", ".join(
                        sql_val(v) if idx != 11 else f"DATE '{v}'"
                        for idx, v in enumerate(row)
                    ) +
                    ")"
                    for row in batch
                ]
            )

            sql = f"""
                INSERT INTO iceberg.silver.ods_stats_cons
                VALUES {values_sql}
            """

            cursor.execute(sql)

        trino.commit()
        context.log.info(f"Inserted {len(rows)} rows")

    finally:
        cursor.close()

    return f"{len(rows)} rows inserted"