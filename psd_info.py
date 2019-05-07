import pandas as pd
import numpy as np
from sqlalchemy import create_engine,DATE,VARCHAR,INT,FLOAT
from sqlalchemy.orm import sessionmaker
import click
import datetime
import io
import time
import json
from concurrent.futures import ThreadPoolExecutor
from Reder import feach_allStore,dfReder,read_redshift
from dblinks import postgreSql_link,redshift_link
from all_sqls import bd_product_info_sql
from move_data import write2db
import traceback
from dask import dataframe as dd
from database import to_store_info,delete_or_create_postgresql_table




def get_date_series(sdate:str):
    date_series = [sdate]
    flag = 1
    while 1:
        date = (datetime.datetime.strptime(sdate,'%Y-%m-%d')+datetime.timedelta(flag)).date()
        if date >= datetime.date.today():
            break
        flag += 1 
        date_series.append(str(date))
    return date_series

def compute_and_write_db_product_info(red_engine,cmid,sdate):
    sql = '''
    select 
        t1.cmid,t1.foreign_store_id,t1.store_name foreign_store_name,t1.foreign_item_id,t1.barcode,t1.item_name,t1.show_code item_show_code,null delivery_method,t1.store_status store_status,
        t1.days,t1.psd,t1.sum_total_sale total_sale,total_cost,t1.sum_total_sale/28 avg_sale,t1.lastin_price,t1.sale_price,t1.psd*t1.lastin_price psd_cost,t1.date update_date
    from
    (select t1.cmid,t1.foreign_store_id,t3.store_name,t3.store_status,t0.barcode,t1.foreign_item_id,t0.item_name,t0.show_code,
                    sum(case when t1.isexist = 1 or t2.exist = 1 then 1 else 0 end) days,sum(isnull(total_sale,0)) sum_total_sale,sum(isnull(total_cost,0)) total_cost,
                    case when days = 0 then 0 else sum_total_sale/days end psd,
                    max(lastin_price) lastin_price,
                    max(sale_price) sale_price,
                    '{sdate}' as date
    from
    (select cmid,foreign_item_id,barcode,show_code,item_name,item_status,lastin_price,sale_price from chain_goods where cmid = {cmid}) t0
    inner join
    (select cmid,foreign_store_id,foreign_item_id,date,case when isnull(quantity,0) > 0 then 1 else 0 end isexist from inventory_{cmid}yyyyyyyyyyyyy where date > '{sdate}' - 28 and date <= '{sdate}') t1
    on t0.cmid = t1.cmid and t0.foreign_item_id = t1.foreign_item_id
    left join
    (select cmid,foreign_store_id,foreign_item_id,date,total_sale,total_cost,case when isnull(total_sale,0) > 0 then 1 else 0 end exist from cost_{cmid}yyyyyyyyyyyyy where date > '{sdate}' - 28 and date <= '{sdate}') t2
    on t0.cmid = t2.cmid and t1.foreign_store_id = t2.foreign_store_id and t0.foreign_item_id = t2.foreign_item_id and t1.date = t2.date
    left join
    (select cmid,foreign_store_id,store_name,store_status from chain_store where cmid = {cmid}) t3
    on t3.cmid=t1.cmid and t3.foreign_store_id = t1.foreign_store_id
    GROUP BY t1.cmid,t1.foreign_store_id,t1.foreign_item_id,t0.show_code,t0.item_name,t3.store_name,t3.store_status,t0.barcode
    having t1.foreign_store_id != '1000000') t1
    ORDER BY psd desc
'''
    sql = sql.format(cmid=cmid,sdate=sdate)
    s3_url = [f's3://spark-test/psd-info-data/psd_info/{sdate}.csv']
    try:
        df = pd.read_sql_query(sql,red_engine)
        ddf = dd.from_pandas(df,npartitions=1)
        ddf.to_csv(s3_url,index=False)  
        return sdate
    except :
        traceback.print_exc()

if __name__ == "__main__":
    gre_engine = create_engine(postgreSql_link)
    red_engine = create_engine(redshift_link)
    dates = get_date_series('2019-02-05')
    with ThreadPoolExecutor(max_workers=3) as exec:
        for date in dates:
            exec.submit(compute_and_write_db_product_info,red_engine,43,date).add_done_callback(lambda res:print(res.result(),f'{date}.csv writed'))
        # compute_and_write_db_product_info(red_engine,gre_engine,43,date)