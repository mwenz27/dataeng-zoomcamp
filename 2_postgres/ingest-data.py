import argparse
import pandas as pd
from sqlalchemy import create_engine
from time import time


def main(params):

    user = params.user
    password = params.password
    host = params.host
    port = params.port
    db = params.db
    table_name = params.table_name

    parser = argparse.ArgumentParser(
        prog="ProgramName",
        description="Ingest CSV data to Postgres",
        epilog="Text at the bottom of help",
    )


    parser.add_argument("user", help="username for postgres")  # positional argument
    parser.add_argument("pass", help="password for postgres")
    parser.add_argument("host", help="host for postgres")
    parser.add_argument("port", help="port for postgres")
    parser.add_argument("db", help="database for postgres")
    parser.add_argument("table-name", help="name of the table for postgres")
    parser.add_argument("url", help="url of the csv file")

    engine = create_engine("postgresql://root:root@localhost:5432/ny_taxi")

    data = "/mnt/c/Users/Michael/projects/dataengineering-zoomcamp/2_postgres/my_taxi_postgres_data/yellow_tripdata_2021-01.csv"

    df = pd.read_csv(data, nrows=100)

    df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
    df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)

    print(pd.io.sql.get_schema(df, name="yellow taxi data", con=engine))

    df.head(n=0)

    df_iter = pd.read_csv(data, iterator=True, chunksize=90000)

    df = next(df_iter)

    df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
    df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)

    df.head(n=0).to_sql(name="yellow_taxi_data", con=engine, if_exists="replace")

    df.to_sql(name="yellow_taxi_data", con=engine, if_exists="append")

    while True:
        try:
            t_start = time()

            df = next(df_iter)

            df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
            df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)
            df.to_sql(name="yellow_taxi_data", con=engine, if_exists="append")

            t_end = time()

            print("insert another chunck --- took %.3f seconds" % (t_end - t_start))
        except StopIteration:
            print("completed")
            break
