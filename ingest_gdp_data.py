#!/usr/bin/env python
# coding: utf-8

import os,sys
import argparse

from time import time

import pandas as pd
from sqlalchemy import create_engine

#print(__name__)


def load_data(params):

    print(' ingest gdp data program name is ' + __name__)
    print(params)    

    parser = argparse.ArgumentParser(description='Ingest CSV data to Postgres')

    parser.add_argument('--user', required=True, help='user name for postgres')
    parser.add_argument('--password', required=True, help='password for postgres')
    parser.add_argument('--host', required=True, help='host for postgres')
    parser.add_argument('--port', required=True, help='port for postgres')
    parser.add_argument('--db', required=True, help='database name for postgres')
    parser.add_argument('--table_name', required=True, help='name of the table where we will write the results to')
    parser.add_argument('--filename', required=True, help='name of the file')
    parser.add_argument('--badfile', required=True, help='name of the filetered data file')
    parser.add_argument('--ctrlfile', required=True, help='name of the ctrl data file')

    args = parser.parse_args(params)
    user = args.user
    password = args.password
    host = args.host 
    port = args.port 
    db = args.db
    table_name = args.table_name
    filename = args.filename
    badfile = args.badfile
    ctrlfile = args.ctrlfile

    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')
    print(engine)

    # loading ctrlfile
    df_ctrl = pd.read_csv(ctrlfile, iterator=True, chunksize=100000)

    df_ctrl_iter = next(df_ctrl)

    df_ctrl_iter.to_sql(name=table_name+'_ctrl', con=engine, if_exists='append')

    # loading badfile
    df_bad = pd.read_csv(badfile, iterator=True, chunksize=100000)

    df_bad_iter = next(df_bad)

    df_bad_iter.to_sql(name=table_name+'_bad', con=engine, if_exists='append')

    # loading good data
    df_iter = pd.read_csv(filename, iterator=True, chunksize=100000)

    df = next(df_iter)

    df.to_sql(name=table_name, con=engine, if_exists='append')

    
"""
    while True: 

        try:
            df = next(df_iter)

            df.to_sql(name=table_name, con=engine, if_exists='append')
            
        except StopIteration:
            print("Finished ingesting data into the postgres database")
            break
"""
if __name__ == '__main__':
    load_data(sys.argv[1:])
    