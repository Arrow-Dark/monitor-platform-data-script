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


def get_daterange_data(bucket,prefix,suffix,days:int=0,sdate:str = None):
    range_urls = range_keys(bucket,prefix,suffix,days=days) if days else range_keys(bucket,prefix,suffix,sdate=sdate)
    df = (dd.read_csv(range_urls,blocksize=None,compression='gzip')).compute()
    return df

def get_lastest_date(bucket,prefix,suffix):
    lastest_url = lastest_objects(bucket,prefix,suffix)
    mfu_df = (dd.read_csv(lastest_url,blocksize=None,compression='gzip')).compute()
    return mfu_df

def get_mfu(cmid,gre_engine):
    mfu_df = pd.read_sql_query(f'select gid,qpc from haiding_{cmid}_gdqpc',red_engine)
    mfu_df = mfu_df.rename(columns={"gid": "foreign_item_id","qpc":"mfu"})
    mfu_df = mfu_df.astype(dtype={'foreign_item_id':str})
    print(mfu_df.count())
    mfu_df = mfu_df.drop_duplicates()
    print(mfu_df.count())

if __name__ == "__main__":
    red_engine = create_engine(redshift_link)
    get_mfu(43,red_engine)