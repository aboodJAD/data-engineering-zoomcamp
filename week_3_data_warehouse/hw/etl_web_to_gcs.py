from pathlib import Path
import pandas as pd
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket
from subprocess import run


@task()
def fetch(dataset_url: str, color:str) -> pd.DataFrame:
    run(f"wget -P ./data/{color} {dataset_url}", shell=True)


@task()
def write_gcs(path: Path) -> None:
    """Upload local parquet file to GCS"""
    gcs_block = GcsBucket.load("zoom-gcs")
    gcs_block.upload_from_path(from_path=path, to_path=path)
    return


@flow()
def etl_web_to_gcs(color: str, year:int) -> None:
    """The main ETL function"""
    for month in range(1, 13):
        dataset_file = f"{color}_tripdata_{year}-{month:02}"
        dataset_url = f"https://github.com/DataTalksClub/nyc-tlc-data/releases/download/{color}/{dataset_file}.csv.gz"
        fetch(dataset_url, color)
        write_gcs(f"data/{color}/{dataset_file}.csv.gz")


if __name__ == "__main__":
    color = "fhv"
    year = 2019
    etl_web_to_gcs(color, year)
