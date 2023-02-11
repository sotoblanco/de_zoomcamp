# First part of q3 requires to download the data from the web and upload it to GCS

from pathlib import Path
import pandas as pd
from prefect import task, flow
from prefect_gcp.cloud_storage import GcsBucket
import pyarrow

@task(retries=3)
def fetch(dataset_url: str) -> pd.DataFrame:
    """Fetch the dataset from the web"""
    return pd.read_csv(dataset_url, engine='pyarrow').astype({
            'PUlocationID': 'Int64',
            'DOlocationID': 'Int64',
            'SR_Flag': 'Int64'
    })

@task()
def write_local(df: pd.DataFrame, dataset_file:str) -> Path:
    """Write the dataset to a local file as parquet file"""
    path = Path(f"data/fhv/{dataset_file}")
    df.to_csv(path, compression="gzip", index=False)
    return path

@task()
def write_gcs(path: Path) -> None:
    """Upload local parquet file to GCS"""
    gcs_block = GcsBucket.load("zoom-gcs-2")
    gcs_block.upload_from_path(from_path=f"{path}", to_path=path)
    return

@flow()
def etl_web_to_gcs(year, month) -> None:
    """The main ETL Function"""
    dataset_file = f"fhv_tripdata_{year}-{month}.csv.gz"
    dataset_prefix = "https://github.com/DataTalksClub/nyc-tlc-data/releases/download/fhv/"
    dataset_url = dataset_prefix + dataset_file

    df = fetch(dataset_url)
    path = write_local(df, dataset_file)
    write_gcs(path)

@flow(log_prints=True)
def etl_flow_fhv(year: int = 2019, months: list[str] = ["01", "02"]):
    for month in months:
        etl_web_to_gcs(year, month)

if __name__=="__main__":
    months = ["01", "02", "03", "04", "05", "06", "07", "08", "09", "10", "11", "12"]
    year = 2019
    etl_flow_fhv(year, months)