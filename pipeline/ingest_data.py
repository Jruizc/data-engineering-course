#!/usr/bin/env python
# coding: utf-8

# In[10]:


import pandas as pd
from sqlalchemy import create_engine
from tqdm.auto import tqdm
import click

prefix = 'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow/'

url = prefix + 'yellow_tripdata_2021-01.csv.gz'

dtype = {
    "VendorID": "Int64",
    "passenger_count": "Int64",
    "trip_distance": "float64",
    "RatecodeID": "Int64",
    "store_and_fwd_flag": "string",
    "PULocationID": "Int64",
    "DOLocationID": "Int64",
    "payment_type": "Int64",
    "fare_amount": "float64",
    "extra": "float64",
    "mta_tax": "float64",
    "tip_amount": "float64",
    "tolls_amount": "float64",
    "improvement_surcharge": "float64",
    "total_amount": "float64",
    "congestion_surcharge": "float64"
}

parse_dates = [
    "tpep_pickup_datetime",
    "tpep_dropoff_datetime"
]

@click.command()
@click.option('--pg-user', default='root', help='PostgreSQL username')
@click.option('--pg-password', default='root', help='PostgreSQL password')
@click.option('--pg-host', default='localhost', help='PostgreSQL host')
@click.option('--pg-port', default='5432', help='PostgreSQL port')
@click.option('--pg-db', default='ny_taxi', help='PostgreSQL database name')
@click.option('--target-table', default='yellow_taxi_data', help='Table name for data ingestion')
@click.option('--chunksize', default=100000, help='Chunk size for reading CSV')
def run(pg_user, pg_password, pg_host, pg_port, pg_db, target_table, chunksize):

    df = pd.read_csv(url, dtype=dtype, parse_dates=parse_dates)

    engine = create_engine(f'postgresql://{pg_user}:{pg_password}@{pg_host}:{pg_port}/{pg_db}')

    df_iter = pd.read_csv(
        url,
        dtype=dtype,
        parse_dates=parse_dates,
        iterator=True,
        chunksize=chunksize
    )

    first_chunk = True

    for df_chunk in tqdm(df_iter):
        if first_chunk:
            df.head(n=0).to_sql(
                name=target_table, 
                con=engine, 
                if_exists='replace')
            first_chunk = False

        df_chunk.to_sql(
            name=target_table, 
            con=engine, 
            if_exists='append')




