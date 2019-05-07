from dask import dataframe as dd
import pandas as pd
import numpy as np
from Reder import lastest_objects,range_keys
import boto3
import datetime
from sqlalchemy import create_engine
from dblinks import postgreSql_link,redshift_link
from psd_compute import get_date_series
from all_sqls import actual_sale_sql,suggest_order_sql,lastest_quantity
from concurrent.futures import ThreadPoolExecutor,ProcessPoolExecutor
from utils import load_df
import gen as lf
from database import delete_or_create_postgresql_table,to_store_info

def get_actual_sale_red(conn,cmid):
    source_id = f'{cmid}yyyyyyyyyyyyyyy'[0:15]
    #sql = actual_sale_sql.format(source_id=source_id,sdate=sdate,edate=edate)
    sql = actual_sale_sql.format(source_id=source_id)
    df = pd.read_sql_query(sql,conn)
    #df = df.astype(dtype = {'single_or_double':int})
    return df


create_table_fields = '''
            cmid INT,foreign_store_id VARCHAR,foreign_store_name VARCHAR,
            foreign_item_id VARCHAR,foreign_item_name VARCHAR,prediction_stock INT,
            theory_stock INT,date DATE
'''

if __name__ == "__main__":
    gre_engine = create_engine(postgreSql_link)
    red_engine = create_engine(redshift_link)

    actual_sale_df = get_actual_sale_red(red_engine,43)
    actual_sale_df = actual_sale_df.astype(dtype={'cmid':int,'foreign_store_id':str,'foreign_item_id':str,'date':str})

    monitor_df = pd.read_sql_query('select * from bd_product_history_43',gre_engine)
    monitor_df = monitor_df.astype(dtype={'cmid':int,'foreign_store_id':str,'foreign_item_id':str,'date':str})
    mix_df = monitor_df.merge(actual_sale_df,how='left',on=['cmid','foreign_store_id','foreign_item_id','date'])
    del mix_df["prediction_stock"]
    mix_df = mix_df.rename(columns={"actual_quantity":"prediction_stock"})
    mix_df.loc[mix_df['prediction_stock'].isna() == True,'prediction_stock'] = 0
    mix_df = mix_df.astype(dtype={"prediction_stock":int})

    mix_df = mix_df[['cmid','foreign_store_id','foreign_store_name','foreign_item_id','foreign_item_name','prediction_stock','theory_stock','date']]

    #print(mix_df.head())
    table_name = 'bd_product_history_43'
    delete_or_create_postgresql_table(gre_engine,table_name,create_table_fields)
    to_store_info(mix_df,gre_engine,table_name)

