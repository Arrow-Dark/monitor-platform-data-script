import pandas as pd
import numpy as np
from sqlalchemy import create_engine, DATE, FLOAT, INT, VARCHAR, null
import os
import click
import datetime
import io
import time
import boto3
import json
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor
from Reder import feach_allStore, dfReder, read_redshift, lastest_objects, date_objects
from dblinks import postgreSql_link, redshift_link, iam_link
from database import Databse
from trans_sql_to_pandas import get_chain_store
from database import append_to_table, write_to_table
import requests
import json
import itertools

store_info_table = 'new_store_info_{}'
store_product_table = 'new_store_product_info_{}'

engine = create_engine(postgreSql_link)
red_engine = create_engine(redshift_link)


def judge_reason(x):
    if x['date'] >= x['use_date']:
        reason = 4 if x['cause'] == '退货' else \
            1 if x['cm_suggest'] == 0 or np.isnan(x['cm_suggest']) or x['cm_suggest'] <= x['inv_delivery'] else \
            2 if x['cm_suggest'] > x['store_book'] and x['inv_delivery'] >= x['store_book'] else \
            3 if x['store_book'] >= x['cm_suggest'] else 5
    else:
        reason = 4 if x['cause'] == '退货' else \
            2 if x['store_book'] == 0 or np.isnan(
                x['store_book']) or x['store_book'] <= x['inv_delivery'] else 3
    return reason


def get_obj_keys(cmid, filt_stores_id, date: str):
    bucket, prefix = 'replenish', f'so_metrics/stock_out_rate_detail_new/{cmid}'
    urls = date_objects(bucket, prefix, filt_stores_id, date)
    return urls


def get_s3_data(urls, sheet_name, dtype=None):
    df_reader = dfReder()
    df = df_reader.read_DF(urls, sheet_name, dtype)
    if df.empty:
        return df
    if sheet_name == 'Sheet1':
        df = df[['cmid', '门店ID', '门店名称', '门店缺货率',
                 '现门店已缺货 SKU 数', '门店统配商品全量 SKU 数', 'date']]
        df['date'] = pd.to_datetime(df['date'], format='%Y-%m-%d')
        df = df.rename(columns={"门店名称": "store_name", "门店ID": "foreign_store_id", "门店缺货率": "store_lost_rate",
                                "门店统配商品全量 SKU 数": "all_sku", "现门店已缺货 SKU 数": "lost_sku"})
        df['store_score'] = np.nan
        df['update_score'] = np.nan
        return df
    elif sheet_name == 'Sheet2':
        df = df[['cmid', '门店ID', '商品名称', '当前库存', '商品show_code', '今日销量',
                 '密度', '原因', '上上次建议单', '上上次订货单', '上次送货单', 'date']]
        df = df.rename(columns={"门店ID": "foreign_store_id", "商品名称": "item_name", '商品show_code': 'item_show_code', '今日销量': 'avg_qty', '密度': 'strategy_type', '原因': 'cause',
                                "当前库存": "actual_stock", "上上次建议单": "cm_suggest", '上上次订货单': 'store_book', '上次送货单': 'inv_delivery'})
        #df['avg_qty'] = np.nan
        df['date'] = pd.to_datetime(df['date'], format="%Y-%m-%d")
        use_date_df = use_cm_df[['cmid','foreign_store_id','date']].rename(columns={'date':'use_date'})
        df = df.merge(use_date_df,how="left",on=['cmid','foreign_store_id'])
        df['actual_lost'] = np.nan
        df['theory_stock'] = np.nan
        # df['use_date'] = np.nan
        df['theory_stock'] = np.nan
        df['actual_lost'] = np.nan
        df['lost_reason'] = df.apply(lambda x: judge_reason(x), axis=1)
        df['strategy_type'] = df['strategy_type'].apply(
            lambda x: '规则' if x == '低' else '算法')
        df['item_score'] = np.nan
        df['update_score'] = np.nan
        # df['date'] = df['date'].astype(str)
        return df


def deal_product_info(df, conn):
    dfs = []

    def dfs_append(res):
        dfs.append(res.result())

    def mix_psd(keys, local_df):
        cmid, date = keys
        # psd_df = pd.read_sql_query(
        #     f"select foreign_store_id,foreign_item_id,psd_cost,item_show_code,date from psd_info_{cmid} where date = '{date}'", conn)
        try:
            psd_df = pd.read_csv(f's3://backend-standard-data/projects/monitor/psd_info/{cmid}/{date.date()}.csv.gz', usecols=[
                'cmid', 'foreign_store_id', 'foreign_item_id', 'item_show_code', 'psd_cost', 'item_show_code', 'date'],dtype={'item_show_code':str,'foreign_item_id':str,'foreign_store_id':str})
        except ValueError as e:
            print(
                f's3://backend-standard-data/projects/monitor/psd_info/{cmid}/{date.date()}.csv.gz not exists!')
            psd_df = pd.DataFrame([],['cmid', 'foreign_store_id', 'foreign_item_id', 'item_show_code', 'psd_cost', 'item_show_code', 'date'])
            print(e)
            # raise e
        local_df = local_df.astype(
            dtype={'foreign_store_id': str, 'item_show_code': str, 'date': str})
        psd_df = psd_df.astype(
            dtype={'foreign_store_id': str, 'item_show_code': str, 'date': str})
        df = local_df.merge(psd_df, how='left', on=[
                            'cmid', 'foreign_store_id', 'item_show_code', 'date'])
        return df

    with ThreadPoolExecutor(max_workers=10) as executor:
        # with ProcessPoolExecutor(max_workers=4) as executor:
        for keys, _df in df.groupby(['cmid', 'date']):
            #executor.submit(mix_psd,date,_df).add_done_callback(lambda res:dfs.append(res.result()))
            executor.submit(mix_psd, keys, _df).add_done_callback(dfs_append)
    all_df = pd.concat(dfs, sort=False)
    all_df = all_df[['cmid', "foreign_store_id", "store_show_code", 'foreign_item_id', 'item_show_code', 'item_name', 'actual_stock', 'cm_suggest', 'store_book',
                     'inv_delivery', 'use_date', 'date', 'theory_stock', 'avg_qty', 'actual_lost', 'lost_reason', 'strategy_type', 'psd_cost', 'item_score', 'update_score']]
    all_df = all_df.astype(dtype={'cmid':int, "foreign_store_id":str, "store_show_code":str, 'foreign_item_id':str, 'item_show_code':str})
    return all_df


def write2db(df, conn, table_name, model='replace', dtype={'date': DATE}):
    if model == 'replace':
        write_to_table(df, table_name, conn, if_exists=model, dtype=dtype)
    else:
        append_to_table(df, table_name, conn, if_exists=model)


def get_use_date(cmid):
    # suggest-order/dist/43/1000040
    bucket, prefix, suffix = 'suggest-order', f'dist/{cmid}', 'csv.gz'
    last_urls = lastest_objects(bucket, prefix, suffix, reverse=False)
    use_date = (last_urls[0].split('/')[-1]).replace('.csv.gz',
                                                     '') if len(last_urls) else str(datetime.date.today())
    return use_date


def raise_err_to_dding(err_message):
    url = 'https://oapi.dingtalk.com/robot/send?access_token=3f7ad08fe5393e50b4253e37172bd44beaf77142701f3fb1418024eb59158acb'
    headers = {"Content-Type": "application/json"}
    data = {"msgtype": "text",
            "text": {
                "content": err_message
            },
            "at": [],
            "isAtAll": False
            }
    res = requests.post(url, data=json.dumps(data), headers=headers)
    print(res.status_code, res.text)


def get_date_series(sdate: str, edate: str):
    date_series = [sdate]
    flag = 1
    target_date = (datetime.datetime.strptime(edate, '%Y-%m-%d')).date() if (datetime.datetime.strptime(edate, '%Y-%m-%d')
                                                                             ).date() <= datetime.date.today() else datetime.date.today() - datetime.timedelta(1)
    while 1:
        date = (datetime.datetime.strptime(sdate, '%Y-%m-%d') +
                datetime.timedelta(flag)).date()
        if date >= target_date:
            break
        flag += 1
        date_series.append(str(date))
    return date_series


def deal_sheet(sheet_name, keys, sub_df):
    cmid, date = keys
    if sheet_name == 'Sheet1':
        table_name = store_info_table.format(cmid)
        sub_df = sub_df[['foreign_store_id', 'store_name', 'store_lost_rate', 'lost_sku',
                         'all_sku', 'date', 'store_score', 'update_score', 'store_show_code']]
        with engine.connect() as connection:
            connection.execute(f'''
                                    CREATE TABLE if not exists {table_name} (
                                        foreign_store_id text,
                                        store_name text,
                                        store_lost_rate float8,
                                        lost_sku int8,
                                        all_sku int8,
                                        date date,
                                        store_score float8,
                                        update_score float8,
                                        store_show_code text
                                        );
                                    delete from {table_name} where date = '{date}' ''')
        write2db(sub_df, engine, table_name, model='append')
        print(sub_df.head())
    else:
        table_name = store_product_table.format(cmid)
        sub_df = sub_df[['foreign_store_id', 'store_show_code', 'foreign_item_id', 'item_show_code', 'item_name', 'actual_stock', 'cm_suggest', 'store_book',
                         'inv_delivery', 'use_date', 'date', 'theory_stock', 'avg_qty', 'actual_lost', 'lost_reason', 'strategy_type', 'psd_cost', 'item_score', 'update_score']]
        with engine.connect() as connection:
            connection.execute(f'''
                                    CREATE TABLE if not exists {table_name} (
                                        foreign_store_id text,
                                        store_show_code text,
                                        foreign_item_id text,
                                        item_show_code text,
                                        item_name text,
                                        actual_stock float8,
                                        cm_suggest float8,
                                        store_book float8,
                                        inv_delivery float8,
                                        use_date date,
                                        date date,
                                        theory_stock float8,
                                        avg_qty float8,
                                        actual_lost float8,
                                        lost_reason int8,
                                        strategy_type text,
                                        psd_cost float8,
                                        item_score float8,
                                        update_score float8
                                        );
                                    delete from {table_name} where date = '{date}' ''')
        write2db(sub_df, engine, table_name, model='append', dtype={
            'date': DATE, 'use_date': DATE, 'store_book': FLOAT, 'cm_suggest': FLOAT})
        with engine.connect() as connection:
            connection.execute(f''' REINDEX table {table_name} ''')
        print(sub_df.head())


# @click.command()
# @click.option('--cmid', default=43, help='Number of cmid.')
def new_store_info_main():
    global use_cm_df
    use_cm_df = pd.read_sql_query(
        "select * from use_cm_stores where cmid = 43 and (deal_status > 0 or execute_date <= end_date)", engine, parse_dates=['date', 'start_date', 'end_date', 'execute_date'])
    if use_cm_df.empty:
        return
    # use_cm_storeids = use_cm_df['foreign_store_id'].tolist()
    mask = (use_cm_df['deal_status'] > 0) & (
        use_cm_df['execute_date'] <= use_cm_df['end_date'])
    # deal data_set of sheet1
    exec_df = use_cm_df.loc[mask].copy()
    exec_df = exec_df.astype(
        {'date': str, 'start_date': str, 'end_date': str, 'execute_date': str})
    s3_urls = list(itertools.chain.from_iterable(exec_df.apply(lambda x: list(map(
        lambda y: f'''s3://replenish/so_metrics/stock_out_rate_detail_new/{x['cmid']}/{x['foreign_store_id']}/{y}.xlsx''', get_date_series(x['execute_date'], x['end_date']))), axis=1)))

    #deal sheet1
    s3_info_df = get_s3_data(s3_urls, 'Sheet1', dtype={
                             '门店ID': str, '门店名称': str, '门店缺货率': float})
    if s3_info_df.empty:
        print('数据集为空，请稍后重试！')
        return

    info_df = s3_info_df.merge(use_cm_df[['cmid', 'foreign_store_id', 'show_code']], on=[
                               'cmid', 'foreign_store_id'])
    info_df = info_df.rename(columns={"show_code": "store_show_code"})
    info_df['date'] = pd.to_datetime(info_df['date'], format="%Y-%m-%d")
    with ThreadPoolExecutor(max_workers=20) as executor:
        [executor.submit(deal_sheet, 'Sheet1', keys, sub_df)
         for keys, sub_df in info_df.groupby(['cmid', 'date'])]


    # deal data_set of sheet2
    s3_product_df = get_s3_data(s3_urls, 'Sheet2', dtype={
                                '门店ID': str, '门店名称': str, '商品show_code': str})
    if s3_product_df.empty:
        print('数据集为空，请稍后重试！')
        return

    product_df = s3_product_df.merge(use_cm_df[['cmid', 'foreign_store_id', 'show_code']], on=[
        'cmid', 'foreign_store_id'])
    product_df = product_df.rename(columns={"show_code": "store_show_code"})
    product_df = deal_product_info(product_df, engine)
    product_df['date'] = pd.to_datetime(product_df['date'], format="%Y-%m-%d")
    stime = time.time()
    print(f'数据写库开始：{stime}')
    with ThreadPoolExecutor(max_workers=20) as executor:
        [executor.submit(deal_sheet, 'Sheet2', keys, sub_df)
         for keys, sub_df in product_df.groupby(['cmid', 'date'])]
    print(f'数据写库结束，总耗时：{(time.time() - stime)}')

    use_cm_df['execute_date'] = pd.to_datetime(
        str(datetime.date.today()), format="%Y-%m-%d")
    use_cm_df.loc[mask, 'deal_status'] = use_cm_df.loc[mask].apply(
        lambda df: 0 if df['end_date'] < df['execute_date'] else 1, axis=1)
    use_cm_df = use_cm_df[['cmid', 'date', 'show_code', 'foreign_store_id',
                           'foreign_store_name', 'start_date', 'end_date', 'deal_status', 'execute_date']]
    use_cm_df['sql'] = use_cm_df.apply(lambda x:f'''update use_cm_stores set deal_status = {x['deal_status']} , execute_date = '{x['execute_date'].date()}' where cmid = {x['cmid']} and foreign_store_id = '{x['foreign_store_id']}' ''' , axis=1)

    # 使用sql链接引擎更改use_cm_stores表
    with engine.connect() as connection:
        sql = ';'.join(use_cm_df['sql'].tolist())
        connection.execute(sql)

    # 使用pandas重建use_cm_stores并填充表
    # write2db(use_cm_df, engine, 'use_cm_stores', model='replace', dtype={
    #          'cmid': INT, 'date': DATE, 'show_code': VARCHAR, 'foreign_store_id': VARCHAR, 'foreign_store_name': VARCHAR, 'start_date': DATE, 'end_date': DATE, 'deal_status': INT, 'execute_date': DATE})


if __name__ == "__main__":
    new_store_info_main()
