import psycopg2
import pandas as pd
import numpy as np
from sqlalchemy import create_engine
import os
import click
import datetime
import io
import time
import boto3
import json
from concurrent.futures import ThreadPoolExecutor
from Reder import feach_allStore,dfReder,read_redshift
from dblinks import postgreSql_link,redshift_link
from all_sqls import read_store_product_sql




def read_store_product(conn,cmid):
    # read_store_product_sql = '''
    # select foreign_store_id,store_show_code,sum(cm_suggest) cm_suggest,sum(store_book) store_book,sum(inv_delivery) inv_delivery,sum(other_reason) other_reason,week_first_day date,EXTRACT('year' from week_first_day) y,EXTRACT('week' from week_first_day) wk
    # from
    # (select 
    # foreign_store_id,store_show_code,case when lost_reason = 1 then 1 else 0 end cm_suggest, case when lost_reason = 2 then 1 else 0 end store_book, case when lost_reason = 3 then 1 else 0 end inv_delivery,case when lost_reason = 4 then 1 else 0 end other_reason,date,date_trunc('week', date::date)::date week_first_day
    # from store_product_info_{cmid}) t
    # GROUP BY foreign_store_id,store_show_code,week_first_day
    # ORDER BY week_first_day
    # '''
    sql = read_store_product_sql.format(cmid=cmid)
    df = pd.read_sql_query(sql,conn)
    df.to_sql(f'store_product_week_{cmid}',conn,if_exists='replace',index=False)

if __name__ == "__main__":
    engine = create_engine(postgreSql_link)
    read_store_product(engine,85)
