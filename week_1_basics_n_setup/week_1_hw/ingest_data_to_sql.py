import argparse

import pandas as pd
from sqlalchemy import create_engine

def main(args):
    engine = create_engine(f"{args.dialect}+{args.driver}://{args.username}:{args.password}@{args.host}:{args.port}/{args.database}")
    for trip_data_file in args.trip_data:
        ingest_trip_data(trip_data_file, engine, args)
    for trip_data_file in args.zone_data:
        ingest_zone_data(trip_data_file, engine, args)

def ingest_trip_data(trip_data_file, engine, args):
    df = read_dataframe(trip_data_file)
    df.lpep_pickup_datetime = pd.to_datetime(df.lpep_pickup_datetime)
    df.lpep_dropoff_datetime = pd.to_datetime(df.lpep_dropoff_datetime)
    df.head(n=0).to_sql(name=args.trip_table_name, con=engine, if_exists='replace')
    df.to_sql(name=args.trip_table_name, con=engine, if_exists='append')

def ingest_zone_data(zone_data_file, engine, args):
    df = read_dataframe(zone_data_file)
    df.head(n=0).to_sql(name=args.zone_table_name, con=engine, if_exists='replace')
    df.to_sql(name=args.zone_table_name, con=engine, if_exists='append')


def read_dataframe(filepath):
    if filepath.endswith(".csv"):
        return pd.read_csv(filepath)
    else:
        return pd.read_parquet(filepath)


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--trip_data", nargs="+", required=True)
    parser.add_argument("--zone_data", nargs="+", required=True)
    parser.add_argument("--dialect", type=str, required=True)
    parser.add_argument("--driver", type=str, required=True)
    parser.add_argument("--username", type=str, required=True)
    parser.add_argument("--password", type=str, required=True)
    parser.add_argument("--host", type=str, required=True)
    parser.add_argument("--port", type=str, required=True)
    parser.add_argument("--database", type=str, required=True)
    parser.add_argument("--trip_table_name", type=str, required=True)
    parser.add_argument("--zone_table_name", type=str, required=True)
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    main(args)
"""
python ingest_data_to_sql.py \
    --trip_data green_tripdata_2019-01.csv \
    --zone_data taxi_zone_lookup.csv \
    --dialect "postgresql" \
    --driver "psycopg2" \
    --username "root" \
    --password "root" \
    --host "localhost" \
    --port "5432" \
    --database "ny_taxi" \
    --trip_table_name "green_trip_data" \
    --zone_table_name "taxi_zone_lookup"
"""