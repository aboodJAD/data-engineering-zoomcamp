from typing import List
from pathlib import Path
from datetime import timedelta

import pandas as pd
from prefect import flow, task
from prefect_gcp import GcpCredentials
from prefect.tasks import task_input_hash
from prefect_gcp.cloud_storage import GcsBucket


@task(retries=3, cache_key_fn=task_input_hash, cache_expiration=timedelta(days=1))
def extract_from_gcs(color: str, year: int, month: int) -> Path:
    """Download trip data from GCS"""
    gcs_path = f"data/{color}/{color}_tripdata_{year}-{month:02}.parquet"
    gcs_block = GcsBucket.load("zoom-gcs")
    gcs_block.get_directory(from_path=gcs_path, local_path=f"./data/")
    return Path(gcs_path)


@task()
def write_bq(df: pd.DataFrame) -> None:
    """Write DataFrame to BiqQuery"""

    gcp_credentials_block = GcpCredentials.load("zoom-gcp-creds")

    df.to_gbq(
        destination_table="dezoomcamp.rides",
        project_id="agile-polymer-376104",
        credentials=gcp_credentials_block.get_credentials_from_service_account(),
        chunksize=500_000,
        if_exists="append",
    )


@flow(log_prints=True)
def etl_gcs_to_bq(color: str, year: int, months: List[int]):
    """Main ETL flow to load data into Big Query"""
    for month in months:
        path = extract_from_gcs(color, year, month)
        df = pd.read_parquet(path)
        print(f"{path} rows:", len(df))
        write_bq(df)


if __name__ == "__main__":
    color = "yellow"
    year = 2019
    months = [2, 3]
    etl_gcs_to_bq(color, year, months)