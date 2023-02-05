import os
import argparse

from time import time

import pandas as pd
from sqlalchemy import create_engine

def main(params):
    user = params.user
    password = params.password
    host = params.host
    port = params.port
    database = params.database
    table = params.table
    url = params.url

    # the backup files are gzipped, and it's important to keep the correct extension
    # for pandas to be able to open the file
    if url.endswith('.csv.gz'):
        csv_name = 'output.csv.gz'
    else:
        csv_name = 'output.csv'

    os.system(f'wget {url} -O {csv_name}')

    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{database}')

    df_iter = pd.read_csv(csv_name, iterator=True, chunksize=1000)

    df = next(df_iter)

    df.lpep_pickup_datetime = pd.to_datetime(df.lpep_pickup_datetime)
    df.lpep_dropoff_datetime = pd.to_datetime(df.lpep_dropoff_datetime)

    df.head(n=0).to_sql(name=table, con=engine, if_exists='replace')

    df.to_sql(name=table, con=engine, if_exists='append', index=False)

    while True:
        try:
            t_start = time()

            df = next(df_iter)

            df.lpep_pickup_datetime = pd.to_datetime(df.lpep_pickup_datetime)
            df.lpep_dropoff_datetime = pd.to_datetime(df.lpep_dropoff_datetime)

            df.to_sql(name=table, con=engine, if_exists='append')

            t_end = time()

            print(f'Chunk ingested in {t_end - t_start} seconds')
            
        except StopIteration:
            print("Finished ingesting data into the postgres database")
            break

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Ingest CSV data into PostgreSQL')
    # user, password, host, port, database, table
    # url of the csv

    parser.add_argument('--user', type=str, help='PostgreSQL user')
    parser.add_argument('--password', type=str, help='PostgreSQL password')
    parser.add_argument('--host', type=str, help='PostgreSQL host')
    parser.add_argument('--port', type=str, help='PostgreSQL port')
    parser.add_argument('--database', type=str, help='PostgreSQL database')
    parser.add_argument('--table', type=str, help='PostgreSQL table')
    parser.add_argument('--url', type=str, help='URL of the CSV file')
    args = parser.parse_args()
    main(args)

