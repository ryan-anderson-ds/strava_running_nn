# -*- coding: utf-8 -*-
"""
Created on Sat Jan  4 20:03:32 2020
@author: rian-van-den-ander
"""

import sqlalchemy
from sqlalchemy import create_engine
import pymysql
import pandas as pd

engine = sqlalchemy.create_engine(
    # Equivalent URL:
    # mysql+pymysql://<db_user>:<db_pass>@/<db_name>?unix_socket=/cloudsql/<cloud_sql_instance_name>
    sqlalchemy.engine.url.URL(
        drivername="mysql+pymysql",
        username='',
        password='',
        database='runningplan',
        query={"unix_socket": "/cloudsql/howeffectiveismyrunningplan:europe-west1:runningplan"}
    ),
    # ... Specify additional properties here.
    # ...
)
    
engine = create_engine('mysql+pymysql://rian:[password]@34.76.12.142/runningplan')


def write_db_replace(df, name):
    df.to_sql(name, con=engine, if_exists='replace', index=False)
    
def write_db_insert(df, name):
    df.to_sql(name, con=engine, if_exists='append', index=False)

def read_db(df_name):    
    query = 'SELECT * FROM ' + df_name
    x =  pd.read_sql_query(query, engine)
    return x

def delete_rows(df_name):    
    rs = engine.execute('DELETE FROM ' + df_name + ';')

def test_conn_new():    
    
    try:
        query = engine.execute('show tables')
        return 'pass'
    except Exception as e:
        return str(e)

"""
processing_status = read_db('processing_status')
processing_status_index = processing_status[processing_status['athlete_id']==str(int(athlete_id))].index.values.astype(int)[0]    
processing_status.at[processing_status_index, 'status'] = 'processed'
write_db_replace(processing_status, 'processing_status')


SHOW / DROP TABLES

rs = engine.execute('show tables')

for row in rs:
    print (row)

rs = engine.execute('drop table metadata_athletes')
rs = engine.execute('drop table metadata_blocks')
rs = engine.execute('drop table all_athlete_activities')
rs = engine.execute('drop table all_athlete_weeks')
rs = engine.execute('drop table features_activities')
rs = engine.execute('drop table features_weeks')
rs = engine.execute('drop table features_blocks')
rs = engine.execute('drop table average_paces_and_hrs')


"""
