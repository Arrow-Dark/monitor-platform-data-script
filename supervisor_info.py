import pandas as pd
import numpy as np
from sqlalchemy import create_engine, DATE, VARCHAR, INT, FLOAT
from sqlalchemy.orm import sessionmaker
import click
import datetime
import io
import time
import json
from concurrent.futures import ThreadPoolExecutor
from dblinks import iam_link, postgreSql_link
from all_sqls import identity_sql, enterprise_sql, stores_sql
import traceback
from database import to_store_info, delete_or_create_postgresql_table

create_table_fields = '''
            first_name VARCHAR,foreign_store_id VARCHAR,user_id VARCHAR,user_name VARCHAR
'''

def get_stores(session, cmid):
    """获取企业所有门店"""
    data = session.execute(stores_sql.format(cmid))
    return [dict(id=store) for store, in data]

def get_enterprise(session):
    """获取企业id"""
    data = session.execute(enterprise_sql)
    return [store for store, in data]

def read_or_create_cm_idenities(iam_engine, postgres_engine):
    """读入和创建门店与督导关系， 一级权限无数据储存， 需要将全部店铺获取赋予其上, """
    session = sessionmaker(bind=iam_engine)()
    cmids = get_enterprise(session)
    table_name = "store_supervisor_{}"
    for cmid in cmids:
        table = table_name.format(cmid)
        data = session.execute(identity_sql.format(cmid, cmid))

        all_stores = get_stores(session, cmid)
        stores_info = dict()
        for user_id, username, first_name, stores, level in data:
            if level == 1:
                stores = all_stores
            for store in stores:
                show_code = store.get("id")
                stores_info[show_code] = dict(foregin_store_id=show_code,user_id=user_id,user_name=username,first_name=first_name)
        # print(stores_info)
        df = pd.DataFrame(stores_info.values())
        #df = df[['foregin_store_id','user_id','user_name','first_name']]
        #df = df[df.isna() == True]
        # print(df)
        # break
        delete_or_create_postgresql_table(postgres_engine, table, table_fields=create_table_fields)
        to_store_info(df, postgres_engine, table)
        print("{} achieve".format(cmid))



if __name__ == "__main__":
    iam_engine = create_engine(iam_link)
    postgres_engine = create_engine(postgreSql_link)
    read_or_create_cm_idenities(iam_engine, postgres_engine)
    # red_engine = create_engine(redshift_link)
    # compute_and_write_db_product_info(red_engine, gre_engine, 43)

