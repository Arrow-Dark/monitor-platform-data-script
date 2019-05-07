import psycopg2
import pandas as pd
import numpy as np
from sqlalchemy import create_engine,DATE,FLOAT,null
import os
import click
import datetime
import io
import time
import boto3
import json
from concurrent.futures import ThreadPoolExecutor,ProcessPoolExecutor
from Reder import feach_allStore,dfReder,read_redshift,lastest_objects,date_objects
from dblinks import postgreSql_link,redshift_link,iam_link
from database import Databse
from trans_sql_to_pandas import get_chain_store
from database import append_to_table,write_to_table
import requests
import json

store_info_table = 'store_info_{}'
store_product_table = 'store_product_info_{}'



def judge_reason(x):
    if x['foreign_store_id'] in use_cm_storeids:
        reason = 4 if x['cause'] == '退货' else \
                    1 if x['cm_suggest'] == 0 or np.isnan(x['cm_suggest']) or x['cm_suggest'] <= x['inv_delivery']  else \
                    2 if x['cm_suggest'] > x['store_book'] and x['inv_delivery'] >= x['store_book'] else \
                    3 if x['store_book'] >= x['cm_suggest'] else 5
    else:
        reason = 4 if x['cause'] == '退货' else \
                    2 if x['store_book'] == 0 or np.isnan(x['store_book']) or x['store_book'] <= x['inv_delivery'] else 3
    return reason

# def get_obj_keys(cmid):
#     bucket,prefix = 'replenish',f'so_metrics/stock_out_rate_detail/{cmid}'
#     obj_keys = list(feach_allStore(bucket,prefix))
#     urls = ((pd.Series(obj_keys,dtype=np.str)).apply(lambda key:f's3://{bucket}/{key}')).tolist()
#     return urls


def get_obj_keys(cmid,filt_stores_id,date:str):
    bucket,prefix = 'replenish',f'so_metrics/stock_out_rate_detail_new/{cmid}'
    urls = date_objects(bucket,prefix,filt_stores_id,date)
    return urls


def get_s3_data(urls,sheet_name,dtype=None):
    df_reader = dfReder()
    df = df_reader.read_DF(urls,sheet_name,dtype)
    if df.empty:
        return df
    if sheet_name == 'Sheet1':
        df = df[['门店ID', '门店名称', '门店缺货率', '现门店已缺货 SKU 数', '门店统配商品全量 SKU 数','date']]
        df['date'] = pd.to_datetime(df['date'],format='%Y-%m-%d')
        df = df.rename(columns={"门店名称": "store_name", "门店ID": "foreign_store_id", "门店缺货率": "store_lost_rate",
                            "门店统配商品全量 SKU 数": "all_sku", "现门店已缺货 SKU 数": "lost_sku"})
        df['store_score'] = np.nan
        df['update_score'] = np.nan
        return df
    elif sheet_name == 'Sheet2':
        df = df[['门店ID', '商品名称', '当前库存','商品show_code','今日销量','密度','原因','上上次建议单', '上上次订货单', '上次送货单','date']]
        df = df.rename(columns={"门店ID": "foreign_store_id", "商品名称": "item_name",'商品show_code':'item_show_code','今日销量':'avg_qty','密度':'strategy_type','原因':'cause',
                           "当前库存": "actual_stock", "上上次建议单": "cm_suggest",'上上次订货单':'store_book','上次送货单':'inv_delivery'})
        #df['avg_qty'] = np.nan
        df['actual_lost'] = np.nan
        df['theory_stock'] = np.nan
        df['use_date'] = np.nan
        df['theory_stock'] = np.nan
        df['actual_lost'] = np.nan
        df['lost_reason'] = df.apply(lambda x:judge_reason(x),axis=1)
        df['strategy_type'] = df['strategy_type'].apply(lambda x:'规则' if x == '低' else '算法')
        df['item_score'] = np.nan
        df['update_score'] = np.nan
        return df


def deal_product_info(df,conn,cmid):
    dfs=[]
    def dfs_append(res):
        dfs.append(res.result())
    def mix_psd(date,local_df):
        # psd_df = pd.read_sql_query(f"select foreign_store_id,foreign_item_id,psd_cost,item_show_code,date from psd_info_{cmid} where date = '{date}' ",conn)
        psd_df = pd.read_csv(f's3://backend-standard-data/projects/monitor/psd_info/{cmid}/{date}.csv.gz', usecols=[
                'cmid', 'foreign_store_id', 'foreign_item_id', 'item_show_code', 'psd_cost', 'item_show_code', 'date'],dtype={'item_show_code':str,'foreign_item_id':str,'foreign_store_id':str})
        #print((psd_df.loc[psd_df['foreign_store_id'] == '1000381']).head())

        # psd_df = psd_df.rename(columns={"update_date": "date"})
        local_df = local_df.astype(dtype={'foreign_store_id':str,'item_show_code':str,'date':str})
        psd_df = psd_df.astype(dtype={'foreign_store_id':str,'item_show_code':str,'date':str})
        df = local_df.merge(psd_df,how='left',on=['foreign_store_id','item_show_code','date'])

        return df

    with ThreadPoolExecutor(max_workers=10) as executor:
    #with ProcessPoolExecutor(max_workers=4) as executor:
        for date,_df in df.groupby(['date']):
            #executor.submit(mix_psd,date,_df).add_done_callback(lambda res:dfs.append(res.result()))
            executor.submit(mix_psd,date,_df).add_done_callback(dfs_append)
    all_df = pd.concat(dfs,sort=False)
    all_df = all_df[["foreign_store_id","store_show_code",'foreign_item_id','item_show_code','item_name','actual_stock','cm_suggest','store_book','inv_delivery','use_date','date','theory_stock','avg_qty','actual_lost','lost_reason','strategy_type','psd_cost','item_score','update_score']]

    return all_df

def write2db(df,conn,table_name,model='replace',dtype = {'date':DATE}):
    # df.to_sql(table_name,conn,if_exists=model,index=False,dtype = dtype)
    if model=='replace':
        write_to_table(df, table_name, conn,if_exists = model,dtype=dtype)
    else:
        append_to_table(df, table_name, conn, if_exists = model)


def get_use_date(cmid):
    #suggest-order/dist/43/1000040
    bucket,prefix,suffix = 'suggest-order',f'dist/{cmid}','csv.gz'
    last_urls = lastest_objects(bucket,prefix,suffix,reverse=False)
    use_date = (last_urls[0].split('/')[-1]).replace('.csv.gz','') if len(last_urls) else str(datetime.date.today())
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


# @click.command()
# @click.option('--cmid', default=43, help='Number of cmid.')
def main(cmid,date):
    engine = create_engine(postgreSql_link)
    red_engine = create_engine(redshift_link)
    # iam_engine = create_engine(iam_link)
    global use_cm_df, use_cm_storeids
    use_cm_df = pd.read_sql_query(
        "select * from use_cm_stores", engine, parse_dates=['date', 'start_date', 'end_date', 'execute_date'])
    use_cm_storeids = use_cm_df['foreign_store_id'].tolist()
    sdate = str((datetime.datetime.strptime(date,'%Y-%m-%d') - datetime.timedelta(84)).date())
    try:
        store_df = get_chain_store(date,f'{cmid}YYYYYYYYYYYYY'[0:15])
    except FileNotFoundError as err:
        # print('FileNotFoundError:',err)
        # if cmid == 43 or cmid == '43':
        err_message = f'FileNotFoundError: {err} 尝试获取昨日闭店数据代替！'
        raise_err_to_dding(err_message)
        store_df = get_chain_store(str((datetime.datetime.strptime(date,'%Y-%m-%d') - datetime.timedelta(1)).date()),f'{cmid}YYYYYYYYYYYYY'[0:15])
    except Exception as err:
        err_message = f'Exception: {err} 闭店数据获取异常，稍后会再次尝试！'
        raise_err_to_dding(err_message)
        raise
    shut_store_ids = (store_df[store_df['store_status'] == '闭店']['foreign_store_id']).tolist()
    s3_urls = get_obj_keys(cmid ,None,date)
    if not len(s3_urls):
        # if cmid == 43 or cmid == '43':
        err_message = f'FileNotFoundError: replenish/so_metrics/stock_out_rate_detail_new/{cmid}/*/{date}.xlsx。请联络数据组同事更新数据，或者稍后重试！'
        raise_err_to_dding(err_message)
        return
    # elif len(s3_urls) < 100:
    #     if cmid == 43 or cmid == '43':
    #         err_message = f'LackOfData: replenish/so_metrics/stock_out_rate_detail_new/{cmid} 更新数据只有{len(s3_urls)}家。请联络数据组同事更新数据，或者稍后重试！'
    #         raise_err_to_dding(err_message)


    s3_info_df = get_s3_data(s3_urls,'Sheet1',dtype={'门店ID':str,'门店名称':str, '门店缺货率':float})
    if s3_info_df.empty:
        print('数据集为空，请稍后重试！')
        return
    red_df = read_redshift(red_engine,'chain_store',cmid,'foreign_store_id','show_code')
    info_df = s3_info_df.merge(red_df,on=['foreign_store_id'])
    info_df = info_df.rename(columns={"show_code": "store_show_code"})
    info_df['date'] = pd.to_datetime(info_df['date'], format = "%Y-%m-%d")
    info_df = info_df[~info_df['foreign_store_id'].isin(shut_store_ids)]
    table_name = store_info_table.format(cmid)
    with engine.connect() as connection:
        connection.execute(f'''delete from {table_name} where date = '{date}' or date <= '{sdate}' ''')
    write2db(info_df,engine,table_name,model='append')
    print(info_df.head())



    s3_product_df = get_s3_data(s3_urls,'Sheet2',dtype={'门店ID':str,'门店名称':str,'商品show_code':str})
    if s3_product_df.empty:
        print('数据集为空，请稍后重试！')
        return
    # s3_product_df.loc[s3_product_df['cm_suggest'] == 'empty','cm_suggest'] = np.nan
    s3_product_df.loc[:,'use_date'] = get_use_date(cmid)
    product_df = s3_product_df.merge(red_df,how='left',left_on=['foreign_store_id'],right_on=['foreign_store_id'])
    product_df = product_df.rename(columns={"show_code": "store_show_code"})
    product_df = deal_product_info(product_df,engine,cmid)
    product_df['date'] = pd.to_datetime(product_df['date'], format = "%Y-%m-%d")
    product_df = product_df[~product_df['foreign_store_id'].isin(shut_store_ids)]
    
    # print(product_df.loc[product_df['psd_cost'].isna() == False,'psd_cost'].head())
    # print(product_df.loc[('foreign_store_id',"store_show_code")])
    stime = time.time()
    print(f'数据写库开始：{stime}')
    table_name = store_product_table.format(cmid)
    with engine.connect() as connection:
        connection.execute(f'''delete from {table_name} where date = '{date}' or date <= '{sdate}' ''')
    write2db(product_df,engine,table_name,model='append',dtype = {'date':DATE,'use_date':DATE,'store_book':FLOAT,'cm_suggest':FLOAT})
    with engine.connect() as connection:
        connection.execute(f''' REINDEX table {table_name} ''')
    print(product_df.head())
    print(f'数据写库结束，总耗时：{(time.time() - stime)}')






if __name__ == "__main__":
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

    date_series = get_date_series('2019-05-02')
    # date_series = ['2019-03-01']
    with ProcessPoolExecutor(max_workers=4) as exec:
        futures = list(exec.submit(main,43,date) for date in date_series)
    [future.result() for future in futures]
    # for date in date_series:
    #     main(43,date)