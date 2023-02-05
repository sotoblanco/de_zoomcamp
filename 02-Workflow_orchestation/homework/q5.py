# First part of q3 requires to download the data from the web and upload it to GCS

from pathlib import Path
import pandas as pd
from prefect import task, flow
from prefect_gcp.cloud_storage import GcsBucket

@task(retries=3)
def fetch(dataset_url: str) -> pd.DataFrame:
    """Fetch the dataset from the web"""
    df = pd.read_csv(dataset_url)
    return df

@task(log_prints=True)
def clean(df = pd.DataFrame) -> pd.DataFrame:
    """fix dtype issues"""
    df['lpep_pickup_datetime'] = pd.to_datetime(df['lpep_pickup_datetime'])
    df['lpep_dropoff_datetime'] = pd.to_datetime(df['lpep_dropoff_datetime'])
    print(df.head(2))
    print(f"columns: {df.dtypes}")
    print("--------------------")
    print(f"rows: {df.shape[0]}")
    return df

@task()
def write_local(df: pd.DataFrame, color:str, dataset_file:str) -> Path:
    """Write the dataset to a local file as parquet file"""
    path = Path(f"data/{color}/{dataset_file}.parquet")
    df.to_parquet(path, compression="gzip")
    return path

@task()
def write_gcs(path: Path) -> None:
    """Upload local parquet file to GCS"""
    gcs_block = GcsBucket.load("zoom-gcs-2")
    gcs_block.upload_from_path(from_path=f"{path}", to_path=path)
    return

@flow()
def etl_web_to_gcs(year, month, color) -> None:
    """The main ETL Function"""
    dataset_file = f'{color}_tripdata_{year}-{month:02}'
    dataset_url = f'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/{color}/{dataset_file}.csv.gz'

    df = fetch(dataset_url)
    df_clean = clean(df)
    path = write_local(df_clean, color, dataset_file)
    write_gcs(path)

@flow(log_prints=True)
def etl_parent_flow_web_to_gcs_q4(
    months: list[int] = [1, 2], year: int = 2020, color: str = "green"
):
    for month in months:
        etl_web_to_gcs(year, month, color)

if __name__=="__main__":
    color = "green"
    months = [4]
    year = 2019
    etl_parent_flow_web_to_gcs_q4(months, year, color)