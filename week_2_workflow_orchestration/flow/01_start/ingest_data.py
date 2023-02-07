import os 
import pandas as pd
from time import time
from sqlalchemy import create_engine
from prefect import flow, task
from prefect.tasks import task_input_hash
from datetime import timedelta


@task(log_prints=True, retries=3, cache_key_fn=task_input_hash, cache_expiration=timedelta(days=1))
def extract_data(url : str) :
    
    if url.endswith('.csv.gz'):
        csv_name = 'output.csv.gz'
    else:
        csv_name = 'output.csv'

    os.system(f"wget {url} -O {csv_name}")

    df_iter = pd.read_csv(csv_name, iterator=True, chunksize=100000)

    df = next(df_iter)

    df.lpep_pickup_datetime = pd.to_datetime(df.lpep_pickup_datetime)
    df.lpep_dropoff_datetime = pd.to_datetime(df.lpep_dropoff_datetime)
    
    return df

@task(log_prints=True)
def clean_data(df) :
    print(f"pre : missing passenger count : {df['passenger_count'].isin([0]).sum()}")
    df = df[df['passenger_count'] != 0]
    print(f"post : missing passenger count : {df['passenger_count'].isin([0]).sum()}")    
    return df

@task(log_prints=True, retries=3)
def ingest_data(user,password,host,port,db,table_name,df):
        
    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')

    df.head(n=0).to_sql(name=table_name, con=engine, if_exists='replace')

    df.to_sql(name=table_name, con=engine, if_exists='append')

@flow(name='Ingest Flow')
def main_flow():
    user = 'root'
    password = 'root'
    host = 'localhost'
    port = '5432'
    db = 'ny_taxi'
    table_name = 'green_tripdata'
    url = 'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/green/green_tripdata_2019-01.csv.gz'

    df = extract_data(url)
    df_cleaned = clean_data(df)
    ingest_data(user,password,host,port,db,table_name,df_cleaned)

if __name__ == '__main__':
    main_flow()