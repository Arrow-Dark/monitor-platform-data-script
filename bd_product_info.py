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
from dask import dataframe as dd
from Reder import feach_allStore,dfReder,read_redshift
from dblinks import postgreSql_link,redshift_link
# from all_sqls import bd_product_info_sql
from trans_sql_to_pandas import bd_product_info_sql
from move_data import write2db
import traceback
from database import to_store_info,delete_or_create_postgresql_table

        
create_table_fields = '''
            cmid INT,foreign_store_id VARCHAR,foreign_store_name VARCHAR,
            foreign_item_id VARCHAR,barcode VARCHAR,item_name VARCHAR,item_status VARCHAR,item_show_code VARCHAR,delivery_method VARCHAR,store_status VARCHAR,
            days INT,psd FLOAT,total_sale FLOAT,total_cost FLOAT,avg_sale FLOAT,lastin_price FLOAT,sale_price FLOAT,psd_cost FLOAT,update_date DATE
'''

def compute_and_write_db_product_info(red_engine,gre_engine,cmid):
    #dtype = {'cmid':INT,'foreign_store_id':VARCHAR,'foreign_item_id':VARCHAR,'item_show_code':VARCHAR,'update_date':DATE}
    table_name = f'bd_product_info_{cmid}'
    # source_id = f'{cmid}yyyyyyyyyyyyyyy'[0:15]
    # sql = bd_product_info_sql.format(source_id=source_id,cmid=cmid,gdstore_alc_name=f'haiding_gdstore_alc_{cmid}') \
    #         if cmid not in [34,85,92,98] else \
    #             bd_product_info_sql.format(source_id=source_id,cmid=cmid,gdstore_alc_name=f'hongye_gdstore_alc_{cmid}')
    # s3_url = ['s3://spark-test/psd-info-data/psd_info/{date}.csv'.format(date = str(datetime.date.today() - datetime.timedelta(1)))]
    try:
        # df = pd.read_sql_query(sql,red_engine)
        df = bd_product_info_sql(str((datetime.date.today() - datetime.timedelta(1))),cmid)
        df = df.astype(dtype={'days':int})
        delete_or_create_postgresql_table(gre_engine,table_name,create_table_fields)
        to_store_info(df,gre_engine,table_name)
        with gre_engine.connect() as connection:
            connection.execute(f''' CREATE INDEX {table_name}_index ON {table_name} (foreign_item_id) ''')
        # ddf = dd.from_pandas(df,npartitions=1)
        # ddf.to_csv(s3_url,index=False)  

    except :
        traceback.print_exc()

if __name__ == "__main__":
    gre_engine = create_engine(postgreSql_link)
    red_engine = create_engine(redshift_link)
    compute_and_write_db_product_info(red_engine,gre_engine,43)

