import os
import boto3
import trino
from dagster import resource
from pathlib import Path
from dagster_dbt import DbtCliResource



@resource
def s3():
    return boto3.client(
        "s3",
        endpoint_url=os.getenv("S3_ENDPOINT"),
        aws_access_key_id=os.getenv("S3_ACCESS_KEY"),
        aws_secret_access_key=os.getenv("S3_SECRET_KEY"),
    )


@resource
def trino_resource(_):
    return trino.dbapi.connect(
        host=os.getenv("TRINO_HOST", "trino"),
        port=int(os.getenv("TRINO_PORT", 8080)),
        user="admin",
        catalog="iceberg",
        schema="silver",
    )

@resource
def dbt():
    return DbtCliResource(
        project_dir="/opt/dagster/app/dbt_election",
    )