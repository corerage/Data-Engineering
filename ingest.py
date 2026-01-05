import pandas as pd
from sqlalchemy import create_engine
from time import time
import os
import argparse

def main(params):
    user = params.user
    password = params.password
    host = params.host
    port = params.port
    db = params.db
    table_name = params.table_name
    url = params.url 
    
    if url.endswith('.csv.gz'):
        csv_name = 'output.csv.gz'
    elif url.endswith('.csv'):
        csv_name = 'output.csv'
    else:
        raise ValueError("URL must point to a .csv or .csv.gz file")
        
    # download the csv
    os.system(f'curl -L "{url}" -o {csv_name}')

    connection_string = f"postgresql://{user}:{password}@{host}:{port}/{db}"
    engine = create_engine(connection_string)
    print("connected to database...")

    df_iter = pd.read_csv(csv_name, iterator=True, chunksize=100000)

    df = next(df_iter)
    
    if "tpep_pickup_datetime" in df.columns and "tpep_dropoff_datetime" in df.columns:

        df['tpep_pickup_datetime'] = pd.to_datetime(df['tpep_pickup_datetime'])
        df['tpep_dropoff_datetime'] = pd.to_datetime(df['tpep_dropoff_datetime'])
    else:
        
    

        #df.head(n=0).to_sql(name='yellow_taxi_data', con=engine, if_exists='replace')

        df.to_sql(name=table_name, con=engine, if_exists='append')

    while True:
        try:
            t_start = time()
            
            df = next(df_iter)
            if "tpep_pickup_datetime" in df.columns and "tpep_dropoff_datetime" in df.columns:
            
                df['tpep_pickup_datetime'] = pd.to_datetime(df['tpep_pickup_datetime'])
                df['tpep_dropoff_datetime'] = pd.to_datetime(df['tpep_dropoff_datetime'])
            
            df.to_sql(name=table_name, con=engine, if_exists='append', index=False)
            
            t_end = time()
            
            print('inserted another chunk..., took %.3f seconds' % (t_end - t_start))
            
        except StopIteration:
            print("ingestion complete")
            break


    # some stufss for the data ingestion process

    print("ingesting ran sucessfully...")
    
if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Ingest CSV data to Postgres")
    
    parser.add_argument('--table_name', required=True, help='Name of the table to write data to')
    parser.add_argument('--user', required=True, help='postgres username')
    parser.add_argument('--password', required=True, help='postgres password')
    parser.add_argument('--host', required=True, help='postgres host')
    parser.add_argument('--port', required=True, help='postgres port')
    parser.add_argument('--db', required=True, help='postgres database name')
    parser.add_argument('--url', required=True, help='URL of the CSV file')
    args = parser.parse_args()
    
    main(args)
