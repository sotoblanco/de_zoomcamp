# deploy this script:

# prefect deployment build q3.py:etl_parent_flow_gcs_bq -n "etl_q3"


from pathlib import Path
import pandas as pd
from prefect import task, flow
from prefect_gcp.cloud_storage import GcsBucket
from prefect_gcp import GcpCredentials

@task(retries=3)
def extract_from_gcs( year: int, month: str) -> Path:
    """Download the dataset from GCS"""
    gcs_path = f'data/fhv/fhv_tripdata_{year}-{month}.csv.gz'
    gcs_block = GcsBucket.load("zoom-gcs-2")
    gcs_block.get_directory(from_path=gcs_path)
    return Path(gcs_path)

@task()
def write_bq(path: Path) -> pd.DataFrame:
    """Write the dataset to BigQuery"""
    df = pd.read_csv(path, engine='pyarrow').astype({
            'PUlocationID': 'Int64',
            'DOlocationID': 'Int64',
            'SR_Flag': 'Int64'
    })

    print(f"Total Rows: {df.shape[0]}")
    gcp_credentials_block = GcpCredentials.load("zoom-gcp-creds")
    df.to_gbq(
        destination_table="fhv.tripdata_csv",
        project_id="dtc-de-course-374821",
        credentials =gcp_credentials_block.get_credentials_from_service_account(),
        chunksize=500_000,
        if_exists="append"
    )

@flow()
def etl_gcs_to_bq(year: int, month: str) -> None:
    """The main ETL Function to load the data into BigQuery"""
    path = extract_from_gcs(year, month)
    write_bq(path)

@flow(log_prints=True)
def etl_parent_flow_gcs_bq(year: int = 2020, months: list[str] = ["01", "02"]
):
    for month in months:
        etl_gcs_to_bq(year, month)

if __name__=="__main__":
    # parametrize the flow
    months = ["01", "02", "03", "04", "05", "06", "07", "08", "09", "10", "11", "12"]
    year = 2019
    etl_parent_flow_gcs_bq(year, months)
