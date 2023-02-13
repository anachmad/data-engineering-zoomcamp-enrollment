# For uploading parquet file needed to solve homework

from pathlib import Path
import pandas as pd
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket

@task(retries=3)
def fetch(dataset_url: str) -> pd.DataFrame:
    """Read taxi data from web into pandas Dataframe"""

    df = pd.read_csv(dataset_url)
    return df

@task(log_prints=True)
def read_df(df: pd.DataFrame) -> None:
    print(df.head(5))
    print(f"columns: {df.dtypes}")

@task(log_prints=True)
def clean(df = pd.DataFrame) -> pd.DataFrame:
    """Fix some dtype issues"""
    
    df['pickup_datetime'] = pd.to_datetime(df['pickup_datetime'])
    df['dropOff_datetime'] = pd.to_datetime(df['dropOff_datetime'])
    print(df.head(2))
    print(f"columns: {df.dtypes}")
    print(f"rows: {len(df)}")

    return df

@task()
def write_local(df: pd.DataFrame, color: str, dataset_file: str) -> Path:
    """Write Dataframe out as csv file"""

    path = Path(f"data/{color}/{dataset_file}.csv.gz")
    df.to_csv(path,compression="gzip")

    return path

@task()
def write_gcs(path: Path) -> None:
    """Upload the local parquet file to GCS"""

    gcs_block = GcsBucket.load("gcs-bucket")
    gcs_block.upload_from_path(from_path=f"{path}", to_path=path)

    return

@flow()
def etl_web_to_gcs(month: int, year: int, color: str) -> None:
    """The main ETL function"""

    dataset_file = f"{color}_tripdata_{year}-{month:02}"
    dataset_url = f"https://github.com/DataTalksClub/nyc-tlc-data/releases/download/{color}/{dataset_file}.csv.gz"

    df = fetch(dataset_url)
    # read_df(df)
    df_cleaned = clean(df)
    path = write_local(df_cleaned, color, dataset_file)
    write_gcs(path)

@flow()
def main_flow(months: list[int] = [2,3], year: int = 2019, color: str = "yellow") -> None:
    for month in months:
        etl_web_to_gcs(month, year, color)

if __name__ == '__main__':
    main_flow([2,3,4,5,6,7,8,9,10,11,12], 2019, "fhv")
