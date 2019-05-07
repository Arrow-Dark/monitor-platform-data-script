from dask import dataframe as dd
import pandas as pd
import numpy as np
from Reder import lastest_objects, range_keys
import boto3
import datetime
import click
from sqlalchemy import create_engine, DATE, INT, VARCHAR, FLOAT
from dblinks import postgreSql_link, redshift_link, aim_link
from psd_compute import get_date_series
# from all_sqls import suggest_order_sql,lastest_quantity,actual_sale_sql_store_level
from trans_sql_to_pandas import actual_sale_sql_store_level
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor
from utils import load_df
import gen as lf
from database import delete_or_create_postgresql_table, to_store_info
import gc
import time
import traceback

# s3://suggest-order/params/43/2019-03-07/mfu.csv.gz

create_table_fields = '''
            cmid INT,foreign_store_id VARCHAR,foreign_store_name VARCHAR,
            foreign_item_id VARCHAR,foreign_item_name VARCHAR,fill_qty FLOAT,prediction_stock INT,
            theory_stock INT,date DATE
'''


def get_lastest_date(bucket, prefix, suffix):
    lastest_url = lastest_objects(bucket, prefix, suffix)
    mfu_df = (dd.read_csv(lastest_url, blocksize=None,
                          compression='gzip')).compute()
    return mfu_df


def get_actual_sale_red(cmid, store_id):
    # source_id = f'{cmid}yyyyyyyyyyyyyyy'[0:15]
    # sql = actual_sale_sql_store_level.format(cmid=cmid,source_id=source_id,store_id=store_id,item_id=item_id)
    # df = pd.read_sql_query(sql,conn)
    df = actual_sale_sql_store_level(
        str((datetime.date.today() - datetime.timedelta(1))), cmid, store_id)
    return df


def get_daterange_data(bucket, prefix, suffix, days: int = 0, sdate: str = None):
    range_urls = range_keys(bucket, prefix, suffix, days=days) if days else range_keys(
        bucket, prefix, suffix, sdate=sdate)
    df = (dd.read_csv(range_urls, blocksize=None, compression='gzip')).compute()
    return df


def mix_data(actual_sale_df, stock_df):
    df = stock_df.merge(actual_sale_df, how='left', on=[
                        'cmid', 'foreign_store_id', 'foreign_item_id'])
    df.sort_values(by=['cmid', 'foreign_store_id',
                       'foreign_item_id', 'date'], inplace=True)
    df['fill_qty'] = 0
    df['should_fill'] = 0
    return df


def compute_inventory(mfu, df):
    input_df, _, cur_stock = load_df(df)
    input_df.reset_index(inplace=True)
    computed_df = lf.compute_optimal_seq(input_df, cur_stock, mfu)
    computed_df = computed_df[['cmid', 'foreign_store_id', 'foreign_store_name',
                               'foreign_item_id', 'foreign_item_name', 'date', 'fill_qty', 'cur_stock']]
    return computed_df


def concat_data(values, _df):
    print('coming')
    gre_engine = create_engine(postgreSql_link)
    red_engine = create_engine(redshift_link)
    cmid, foreign_store_id = values

    # source_id = f'{cmid}yyyyyyyyyyyyyyy'[0:15]
    # sql = actual_sale_sql_store_level.format(cmid=cmid,source_id=source_id,store_id=foreign_store_id)
    # actual_sale_df = pd.read_sql_query(sql,red_engine)
    actual_sale_df = get_actual_sale_red(cmid, foreign_store_id)
    actual_sale_df = actual_sale_df.astype(
        dtype={'cmid': int, 'foreign_store_id': str, 'foreign_item_id': str, 'date': str})

    def deal_data(values, item_df):
        empty_df = pd.DataFrame([], columns=['empty'])
        if item_df.empty:
            return empty_df
        try:
            item_id, mfu = values
            # actual_sale_df = get_actual_sale_red(red_engine,cmid,foreign_store_id,item_id)
            # actual_sale_df = actual_sale_df.astype(dtype={'cmid':int,'foreign_store_id':str,'foreign_item_id':str,'date':str})
            lastest_stock_df = (actual_sale_df.loc[(actual_sale_df['rank_num'] == 1) & (
                actual_sale_df['foreign_item_id'] == item_id), ('cmid', 'foreign_store_id', 'foreign_item_id', 'actual_quantity')]).copy()

            item_df = item_df.merge(lastest_stock_df, how='left', on=[
                                    'cmid', 'foreign_store_id', 'foreign_item_id'])
            item_df = item_df.rename(columns={"actual_quantity": "quantity"})

            # print(f'{item_id}:开始合并数据！')
            mix_df = mix_data(actual_sale_df, item_df)
            mix_df = mix_df[mix_df['date'].isna() == False]
            mix_df['single_or_double'] = mix_df['date'].apply(lambda x: 1 if (
                datetime.datetime.strptime(x, '%Y-%m-%d')).day % 2 == 1 else 2)
            mix_df.loc[mix_df['sale_quantity'].isna() == True,
                       'sale_quantity'] = 0
            mix_df['can_request_order'] = mix_df.apply(lambda x: 0 if x.empty or (x[['period_type_num']]).empty or (
                x[['single_or_double']]).empty else 1 if x['period_type_num'] == 3 or x['single_or_double'] == x['period_type_num'] else 0, axis=1)
            print(f'{item_id}:数据合并完成')

            stime = time.time()
            # print(f'{item_id}:开始计算！开始时间：{stime}')
            theory_df = mix_df[['cmid', 'foreign_store_id', 'foreign_store_name', 'foreign_item_id', 'foreign_item_name',
                                'sale_quantity', 'can_request_order', 'should_fill', 'fill_qty', 'quantity', 'mfu', 'date']]
            theory_df = theory_df.rename(
                columns={"sale_quantity": "demand", "quantity": "cur_stock"})
            theory_df = compute_inventory(mfu, theory_df)
            # print(theory_df.head())

            prediction_df = mix_df[['cmid', 'foreign_store_id', 'foreign_store_name',
                                    'foreign_item_id', 'foreign_item_name', 'actual_quantity', 'date']]

            theory_df = theory_df.rename(columns={"cur_stock": "theory_stock"})
            prediction_df = prediction_df.rename(
                columns={"actual_quantity": "prediction_stock"})

            done_df = theory_df.merge(prediction_df, how='left', on=[
                                      'cmid', 'foreign_store_id', 'foreign_store_name', 'foreign_item_id', 'foreign_item_name', 'date'])
            done_df = done_df[['cmid', 'foreign_store_id', 'foreign_store_name', 'foreign_item_id',
                               'foreign_item_name', 'fill_qty', 'prediction_stock', 'theory_stock', 'date']]
            done_df = done_df.astype(dtype={'prediction_stock': int})
        except:
            traceback.print_exc()
            print('数据异常,跳过该商品')
            return empty_df
        # print(done_df.info(memory_usage='deep'))
        # print(done_df.head())

        etime = time.time()
        print(f'计算结束！结束时间：{etime}')
        print(f'消耗时长：{etime-stime}')

        return done_df

    with ThreadPoolExecutor(max_workers=10) as exec:
        futures = [exec.submit(deal_data, values, item_df)
                   for values, item_df in _df.groupby(['foreign_item_id', 'mfu'])]
        res = [future.result()
               for future in futures if not (future.result()).empty]
    if len(res):
        # results_df = pd.concat(res,sort=False)
        # to_store_info(results_df,gre_engine,f'bd_product_history_{cmid}_temp')
        return 1
    else:
        return 0


def concurrent_compute_inventory(df, works=4):
    with ProcessPoolExecutor(max_workers=works) as exec:
        futures = [exec.submit(concat_data, values, _df)
                   for values, _df in df.groupby(['cmid', 'foreign_store_id'])]
        res = [future.result() for future in futures]
    # results_df = pd.concat(res,sort=False)
    return True
    # futures = []
    # for values,_df in df.groupby(['cmid','foreign_store_id']):
    #     res = concat_data(values,_df)
    #     futures.append(res)
    # results_df = pd.concat(res,sort=False)
    # return results_df

# def entrance(cmid,sdate,edate):
# @click.command()
# @click.option('--cmid', default=43, help='Number of cmid.')


def task_entrance(cmid):
    gre_engine = create_engine(postgreSql_link)
    red_engine = create_engine(redshift_link)
    aim_conn = create_engine(aim_link)

    delivery_period_df = pd.read_sql_query(
        f'select foreign_store_id,dly_cycle period_type from store_dly_cycle where cmid = {cmid}', red_engine)
    #delivery_period_df = delivery_period_df.drop_duplicates()
    delivery_period_df['period_type_num'] = delivery_period_df['period_type'].apply(
        lambda x: 1 if x == '单日配' else 2 if x == '双日配' else 3)
    delivery_period_df = delivery_period_df[[
        'foreign_store_id', 'period_type_num']]
    delivery_period_df = delivery_period_df.astype(
        dtype={'foreign_store_id': str, "period_type_num": int})

    mfu_sql = f'select gid,qpc from haiding_{cmid}_gdqpc where isdu = 2' if cmid not in [
        34, 92, 98] else f'select foreign_item_id,qpc from hongye_{cmid}_gdqpc'
    mfu_df = pd.read_sql_query(mfu_sql, red_engine)
    mfu_df = mfu_df.rename(columns={"gid": "foreign_item_id", "qpc": "mfu"})
    mfu_df = mfu_df.astype(dtype={"foreign_item_id": int})
    mfu_df = mfu_df.drop_duplicates()
    mfu_df = mfu_df.astype(dtype={'foreign_item_id': str})

    # stock_df = pd.read_sql_query(
    #     f"select cmid,foreign_store_id,foreign_store_name,foreign_item_id,item_name foreign_item_name from bd_product_info_{cmid}", gre_engine)
    
    stock_df = stock_df.astype(
        dtype={'cmid': int, 'foreign_store_id': str, 'foreign_item_id': str})
    stock_df = stock_df.merge(mfu_df, how='left', on=["foreign_item_id"]).merge(
        delivery_period_df, how='left', on=['foreign_store_id'])

    table_name_temp = f'bd_product_history_{cmid}_temp'
    table_name = f'bd_product_history_{cmid}'
    delete_or_create_postgresql_table(
        gre_engine, table_name_temp, table_fields=create_table_fields)
    results = concurrent_compute_inventory(stock_df)

    if results:
        with gre_engine.connect() as connection:
            connection.execute(
                f'''ALTER table IF EXISTS {table_name} RENAME TO {table_name}_bck ;
                    ALTER table IF EXISTS {table_name_temp} RENAME TO {table_name};
                    drop table IF EXISTS {table_name}_bck; 
                    CREATE INDEX {table_name}_union_index ON {table_name} (foreign_item_id,foreign_store_id);
                    CREATE INDEX {table_name}_index ON {table_name} (foreign_store_id);''')
            # short_df = pd.read_sql_query(f'select * from {table_name_temp} where date >= (select max(date) from table_name)',gre_engine)
            # to_store_info(short_df,gre_engine,table_name)
            # connection.execute(f'drop table IF EXISTS {table_name}_temp')
            # connection.execute(f"delete table IF EXISTS {table_name} where date = (select max(date) from {table_name}) ; insert into {table_name} (select * from {table_name_temp} where date > (select max(date) from {table_name}));drop table IF EXISTS {table_name_temp}")
        with aim_conn.connect() as connection:
            sql = f'''
                    SELECT a.attname AS field,t.typname AS type
                    FROM pg_class c,pg_attribute a,pg_type t
                    WHERE c.relname = '{table_name}' and a.attnum > 0 and a.attrelid = c.oid and a.atttypid = t.oid
                    ORDER BY a.attnum
                '''
            df = pd.read_sql_query(sql, gre_engine)
            df['create_table_fields'] = df['field'] + ' ' + df['type']
            table_fields = ','.join(df['create_table_fields'].tolist())

            connection.execute(
                f"drop table IF EXISTS {table_name}_temp;create table IF not EXISTS {table_name}_temp({table_fields})")
            tab_df = pd.read_sql_query(
                f'select * from {table_name}', gre_engine)
            to_store_info(tab_df, aim_conn, f'{table_name}_temp')

            connection.execute(
                f'''ALTER table IF EXISTS {table_name} RENAME TO {table_name}_bck ; 
                    ALTER table IF EXISTS {table_name}_temp RENAME TO {table_name};
                    drop table IF EXISTS {table_name}_bck; 
                    CREATE INDEX {table_name}_union_index ON {table_name} (foreign_item_id,foreign_store_id);
                    CREATE INDEX {table_name}_index ON {table_name} (foreign_store_id);''')


if __name__ == "__main__":
    # gre_engine = create_engine(postgreSql_link)
    # red_engine = create_engine(redshift_link)
    task_entrance(43)
    # df = get_actual_sale_red(red_engine,43,'2019-03-01','2019-03-07')
    # gre_engine = create_engine(postgreSql_link)
    # red_engine = create_engine(redshift_link)
    # df = get_suggest_order(red_engine,43,'2019-03-01','2019-03-07')
    # print(df.head())

    # dates = get_date_series('2018-12-05')
    # dates.reverse()
    # print(dates)
