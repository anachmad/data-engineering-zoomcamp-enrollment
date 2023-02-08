# For loading data to BQ to answer question no. 3

from pathlib import Path
import pandas as pd
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket
from prefect_gcp.credentials import GcpCredentials

@task(retries=3)
def extract_from_gcs(color: str, year: int, month: int) -> pd.DataFrame:
    """Downlaod trip data from GCS to Local"""

    gcs_path = f"data/{color}/{color}_tripdata_{year}-{month:02}.parquet"
    gcs_block = GcsBucket.load("gcs-bucket")
    gcs_block.get_directory(from_path=gcs_path, local_path=f"")

    path = Path(f"{gcs_path}")

    df = pd.read_parquet(path)
    
    return df

@task(log_prints=True, retries=3)
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

    print(f"rows processed: {len(df)}")

    return

@flow()
def etl_gcs_to_bq(month: int, year: int, color: str) -> None:
    """The EL function to load data from GCS to local and then Big Query"""
    
    df = extract_from_gcs(color, year, month)
    write_bq(df)

@flow()
def main_el_flow(months: list[int] = [2,3], year: int = 2019, color: str = "yellow") -> None:
    """The main flow for loading data from GCS to local and then Big Query"""
    for month in months:
        etl_gcs_to_bq(month, year, color)

if __name__ == '__main__':
    main_el_flow([2,3], 2019, "yellow")
