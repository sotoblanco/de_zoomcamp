# deploy this script:

# prefect deployment build q3.py:etl_parent_flow_gcs_bq -n "etl_q3"


from pathlib import Path
import pandas as pd
from prefect import task, flow
from prefect_gcp.cloud_storage import GcsBucket
from prefect_gcp import GcpCredentials

@task(retries=3)
def extract_from_gcs(color: str, year: int, month: int) -> Path:
    """Download the dataset from GCS"""
    gcs_path = f'data/{color}/{color}_tripdata_{year}-{month:02}.parquet'
    gcs_block = GcsBucket.load("zoom-gcs-2")
    gcs_block.get_directory(from_path=gcs_path, local_path=f"../data/")
    return Path(f"../data/{gcs_path}")

@task()
def write_bq(path: Path) -> pd.DataFrame:
    """Write the dataset to BigQuery"""
    df = pd.read_parquet(path)
    print(f"Total Rows: {df.shape[0]}")
    gcp_credentials_block = GcpCredentials.load("zoom-gcp-creds")
    df.to_gbq(
        destination_table="zoomcamp_yellow.rides_yellow",
        project_id="dtc-de-course-374821",
        credentials =gcp_credentials_block.get_credentials_from_service_account(),
        chunksize=500_000,
        if_exists="append"
    )

@flow()
def etl_gcs_to_bq(year: int, month: int, color: str) -> None:
    """The main ETL Function to load the data into BigQuery"""
    path = extract_from_gcs(color, year, month)
    write_bq(path)

@flow(log_prints=True)
def etl_parent_flow_gcs_bq(
    months: list[int] = [1, 2], year: int = 2020, color: str = "green"
):
    for month in months:
        etl_gcs_to_bq(year, month, color)

if __name__=="__main__":
    # parametrize the flow
    color = "yellow"
    months = [2, 3]
    year = 2019
    etl_parent_flow_gcs_bq(months, year, color)
