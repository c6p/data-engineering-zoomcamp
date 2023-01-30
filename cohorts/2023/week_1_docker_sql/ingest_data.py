#!/usr/bin/env python
# coding: utf-8

from time import time

import pandas as pd
from sqlalchemy import create_engine

user, password, host, port, db = 'root', 'root', 'localhost', 5432, 'ny_taxi'

def lpep_convert_datetime(df):
    df.lpep_pickup_datetime = pd.to_datetime(df.lpep_pickup_datetime)
    df.lpep_dropoff_datetime = pd.to_datetime(df.lpep_dropoff_datetime)

def insert_chunk(df_iter, tablename, con, preprocess=None, first=False):
    df = next(df_iter)
    if preprocess is not None:
        preprocess(df)
    if first:
        df.head(n=0).to_sql(name=tablename, con=con, if_exists='replace')
    df.to_sql(name=tablename, con=con, if_exists='append')

def insert_csv(filename, tablename, con, preprocess=None):
    df_iter = pd.read_csv(filename, iterator=True, chunksize=100000)
    insert_chunk(df_iter, tablename, con, preprocess, True) 
    while True:
        try:
            t_start = time()
            insert_chunk(df_iter, tablename, con, preprocess) 
            t_end = time()
            print('inserted another chunk, took %.3f second' % (t_end - t_start))
        except StopIteration:
            print("Finished ingesting data into the postgres database")
            break


if __name__ == '__main__':
    engine = create_engine(
        f'postgresql://{user}:{password}@{host}:{port}/{db}')

    insert_csv('green_tripdata_2019-01.csv.gz', 'green_tripdata', engine, lpep_convert_datetime)
    insert_csv('taxi+_zone_lookup.csv', 'zones', engine)
