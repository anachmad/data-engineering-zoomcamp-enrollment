from pathlib import Path
import pandas as pd
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket
from prefect_gcp.credentials import GcpCredentials

@task(retries=3)
def extract_from_gcs(color: str, year: int, month: int) -> Path:
    """Downlaod trip data from GCS to Local"""

    gcs_path = f"data\{color}\{color}_tripdata_{year}-{month:02}.parquet"
    gcs_block = GcsBucket.load("gcs-bucket")
    gcs_block.get_directory(from_path=gcs_path, local_path=f"")
    
    return Path(f"{gcs_path}")

@task(log_prints=True)
def clean_data(path = Path) -> pd.DataFrame:
    """Clean invalid data"""

    df = pd.read_parquet(path)
    print(f"pre: missing passenger count: {df['passenger_count'].isna().sum()}")
    df['passenger_count'].fillna(0, inplace=True)
    print(f"post: missing passenger count: {df['passenger_count'].isna().sum()}")

    return df

@task()
def write_bq(df: pd.DataFrame) -> None:
    """Upload the local parquet file to Big Query"""

    gcp_credentials_block = GcpCredentials.load("de-course-gcp-cred")

    df.to_gbq(
        destination_table= "de_course_dataset.taxi_trip",
        project_id= "de-course-376622",
        credentials= gcp_credentials_block.get_credentials_from_service_account(), 
        chunksize= 500000,
        if_exists= "append"
    )

    return

@flow()
def etl_gcs_to_bq() -> None:
    """The main ETL function to load data from GCS to Big Query"""
    color = "yellow"
    year = 2021
    month = 1
    
    path = extract_from_gcs(color, year, month)
    df = clean_data(path)
    write_bq(df)

if __name__ == '__main__':
    etl_gcs_to_bq()
