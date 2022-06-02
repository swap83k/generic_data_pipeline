#!/usr/bin/env python
# coding: utf-8

import os,sys,argparse,pandas as pd,pathlib
from time import time
from sqlalchemy import create_engine

_parentdir = pathlib.Path(__file__).parent.parent.resolve()
sys.path.insert(0, str(_parentdir))

from config.definitions import ROOT_DIR

def load_data(params):    

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
    user,password,host,port,db = args.user,args.password,args.host,args.port,args.db
    table_name,filename,badfile,ctrlfile = args.table_name,args.filename,args.badfile,args.ctrlfile
     
    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')
    print(engine)

    # loading ctrlfile
    print('loading ' + os.path.join(ROOT_DIR,'sink',ctrlfile))
    df_ctrl = pd.read_csv(os.path.join(ROOT_DIR,'sink',ctrlfile), iterator=True, chunksize=100000)

    df_ctrl_iter = next(df_ctrl)

    df_ctrl_iter.to_sql(name=table_name+'_ctrl', con=engine, if_exists='append')

    # loading badfile
    print('loading ' + badfile)
    df_bad = pd.read_csv(os.path.join(ROOT_DIR,'sink',badfile), iterator=True, chunksize=100000)

    df_bad_iter = next(df_bad)

    df_bad_iter.to_sql(name=table_name+'_bad', con=engine, if_exists='append')

    # loading good data
    print('loading ' + filename)
    df_iter = pd.read_csv(os.path.join(ROOT_DIR,'sink',filename), iterator=True, chunksize=100000)

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