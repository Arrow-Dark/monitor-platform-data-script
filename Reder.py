import datetime
import time
import boto3
import json
import pandas as pd
import numpy as np
from dask import dataframe as dd
import gc
from concurrent.futures import ThreadPoolExecutor
import traceback
import xlrd


def feach_allStore(bucket, prefix, suffix=None):
    S3 = boto3.resource('s3')
    objects = S3.Bucket(bucket).objects.filter(Prefix=prefix)
    objects = [obj for obj in objects if obj.key.endswith(
        suffix)] if suffix else objects
    #obj_keys=set('/'.join(obj.key.split('/')[0:3]) for obj in objects)
    obj_keys = set('/'.join(obj.key.split('/')) for obj in objects)
    return obj_keys


def lastest_objects(bucket, prefix, suffix, reverse=True):
    S3 = boto3.resource('s3')
    objects = S3.Bucket(bucket).objects.filter(Prefix=prefix)
    filter_objs = [obj for obj in objects if obj.key.endswith(suffix)]
    sort_objs = sorted(filter_objs, key=lambda obj: int(
        obj.last_modified.strftime("%s")), reverse=reverse)
    lastest_objs = sort_objs[0] if len(sort_objs) else None
    # if lastest_objs and lastest_objs.key.endswith(suffix):
    if lastest_objs:
        return [f's3://{bucket}/{lastest_objs.key}']
    else:
        return []

# replenish/so_metrics/stock_out_rate_detail_test/43/1002363/2019-03-11.xlsx


def date_objects(bucket, prefix, filt_stores_id, date: str):
    suffix = f'{date}.xlsx'
    S3 = boto3.resource('s3')
    objects = S3.Bucket(bucket).objects.filter(Prefix=prefix)
    #filter_obj_keys = [obj.key for obj in objects if obj.key.endswith(suffix) and obj.key.split('/')[-2] not in filt_stores_id]
    filter_obj_keys = [obj.key for obj in objects if obj.key.endswith(suffix)]
    urls = ((pd.Series(filter_obj_keys, dtype=np.str)).apply(
        lambda key: f's3://{bucket}/{key}')).tolist()
    return urls


def range_keys(bucket, prefix, suffix, days: int = 0, sdate: str = None):
    S3 = boto3.resource('s3')
    objects = S3.Bucket(bucket).objects.filter(Prefix=prefix)
    sort_objs = sorted(objects, key=lambda obj: int(
        obj.last_modified.strftime("%s")), reverse=True)
    if days:
        thirty_objs = set(f's3://{bucket}/{obj.key}' for obj in sort_objs[0:100] if obj.key.endswith(suffix) and datetime.datetime.strptime(
            obj.key.split('/')[-2], '%Y-%m-%d').date() >= datetime.date.today()-datetime.timedelta(days)) if len(sort_objs) else set()
    elif sdate:
        thirty_objs = set(f's3://{bucket}/{obj.key}' for obj in sort_objs[0:100] if obj.key.endswith(suffix) and datetime.datetime.strptime(
            obj.key.split('/')[-2], '%Y-%m-%d').date() >= datetime.datetime.strptime(sdate, '%Y-%m-%d').date()) if len(sort_objs) else set()
    return list(thirty_objs)


def get_metric_urls(bucket, obj_key):
    path = 's3://'+bucket+'/'+obj_key
    jdf = pd.read_json(path, orient='records')
    _list = jdf['fm_res'] if 'fm_res' in jdf.keys() else []
    metric_urls = [res['result']['metric_url']
                   for res in _list if 'metric_url' in res['result'].keys()]
    return metric_urls


def get_trainfit_urls(bucket, obj_key):
    path = 's3://'+bucket+'/'+obj_key
    jdf = pd.read_json(path, orient='records')
    _list = jdf['fm_res'] if 'fm_res' in jdf.keys() else []
    trainfit_dict = sum([res['result']['trainfit_urls']
                         for res in _list if 'trainfit_urls' in res['result'].keys()], [])
    trainfit_urls = [res['url']
                     for res in trainfit_dict if 'url' in res.keys()]
    return trainfit_urls


def get_plot_urls(bucket, obj_key):
    path = 's3://'+bucket+'/'+obj_key
    jdf = pd.read_json(path, orient='records')
    _list = jdf['fm_res'] if 'fm_res' in jdf.keys() else []
    plot_dict = sum([res['result']['plot_urls']
                     for res in _list if 'plot_urls' in res['result'].keys()], [])
    plot_urls = [res['url'] for res in plot_dict if 'url' in res.keys()]
    return plot_urls


def read_redshift(conn, table_name, cmid=None, *fields):
    assert conn, '请提供数据库链接引擎！'
    fields = ','.join(list(fields)) if len(list(fields)) else '*'
    sql = f'select {fields} from {table_name} where cmid = {cmid}' if cmid else f'select {fields} from {table_name}'
    df = pd.read_sql_query(sql, conn)
    return df


def dask_read_csv(urls):
    df = dd.read_csv(urls)
    df = df.compute()
    return df


class dfReder():
    def __init__(self):
        self.__DFS = []

    def __appendDF(self, df):
        df = df.result()
        if not df.empty:
            self.__DFS.append(df)

    def __get_DF(self, url, sheet_name, dtype):
        try:
            df = pd.read_excel(url, sheet_name=sheet_name, dtype=dtype)
        except:
            # traceback.print_exc()
            print(url, f'No sheet named {sheet_name} or link not exists!')
            return pd.DataFrame([], columns=['empty'])
        df['cmid'] = int(url.split('/')[-3])
        df['date'] = url.split('/')[-1].replace('.xlsx', '')
        return df

    def read_DF(self, urls, sheet_name, dtype, works=20):
        with ThreadPoolExecutor(max_workers=works) as executor:
            for i in urls:
                executor.submit(self.__get_DF, i, sheet_name,
                                dtype).add_done_callback(self.__appendDF)
        print('数据获取成功')
        print('开始合并数据')
        if not len(self.__DFS):
            print('数据集为空，结束合并！')
            return pd.DataFrame([], columns=['empty'])
        big_df = pd.concat(self.__DFS, sort=False)
        print('合并数据完成')
        self.__DFS = []
        gc.collect()
        return big_df
