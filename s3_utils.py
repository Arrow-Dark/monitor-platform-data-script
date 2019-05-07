import pandas as pd
import numpy as np
from Reder import feach_allStore,dfReder
import boto3
import dask.dataframe as dd
from move_data import use_cm_storeids,judge_reason
from concurrent.futures import ThreadPoolExecutor
import gc


def get_obj_keys(cmid):
    bucket,prefix = 'replenish',f'so_metrics/stock_out_rate_detail_new/{cmid}'
    obj_keys = list(feach_allStore(bucket,prefix))
    urls = ((pd.Series(obj_keys,dtype=np.str)).apply(lambda key:f's3://{bucket}/{key}')).tolist()
    return urls




def get_s3_data(urls,sheet_name,dtype=None):
    df_reader = dfReder()
    df = df_reader.read_DF(urls,sheet_name,dtype)
    if sheet_name == 'Sheet1':
        if df.empty:
            return df
        df = df[['门店ID', '门店名称', '门店缺货率', '现门店已缺货 SKU 数', '门店统配商品全量 SKU 数','date']]
        # df['date'] = pd.to_datetime(df['date'],format='%Y-%m-%d')
        df = df.rename(columns={"门店名称": "store_name", "门店ID": "foreign_store_id", "门店缺货率": "store_lost_rate",
                            "门店统配商品全量 SKU 数": "all_sku", "现门店已缺货 SKU 数": "lost_sku"})
        df['store_score'] = np.nan
        df['update_score'] = np.nan
        return df
    elif sheet_name == 'Sheet2':
        if df.empty:
            return df
        df = df[['门店ID', '商品名称', '当前库存','商品show_code', '上上次建议单', '上上次订货单', '上次送货单','date']]
        df = df.rename(columns={"门店ID": "foreign_store_id", "商品名称": "item_name",'商品show_code':'item_show_code',
                           "当前库存": "actual_stock", "上上次建议单": "cm_suggest",'上上次订货单':'store_book','上次送货单':'inv_delivery'})
        df['avg_qty'] = np.nan
        df['actual_lost'] = np.nan
        df['theory_stock'] = np.nan
        df['use_date'] = np.nan
        df['theory_stock'] = np.nan
        df['actual_lost'] = np.nan
        # df['lost_reason'] = df.apply(lambda x:1 if x['cm_suggest'] == 0 else \
        #                     2 if (x['cm_suggest'] > 0 and x['store_book'] != x['cm_suggest']) else \
        #                     3 if (x['cm_suggest'] > 0 and x['store_book'] > 0 and x['inv_delivery'] != x['store_book']) else 4,axis=1)
        df['lost_reason'] = df.apply(lambda x:judge_reason(x),axis=1)
        df['strategy_type'] = np.nan
        df['item_score'] = np.nan
        df['update_score'] = np.nan
        #print(df)
        return df


def get_s3_dask(load_dates, cmid):
    cost_urls = []
    for d in load_dates:
        url = "s3://replenish/so_metrics/store_cm_sku/{}/{}.csv.gz".format(cmid, d.isoformat())
        cost_urls.append(url)
    # _costs = [
    #     dd.read_csv(
    #         url,
    #         compression="gzip",
    #         blocksize=None,
    #         dtype={
    #             "foreign_item_id": str,
    #             "foreign_store_id": str,
    #             "date": str
    #         },
    #         usecols=[
    #             "foreign_item_id",
    #             "foreign_store_id",
    #             "date"
    #         ]
    #     )
    #     for url in cost_urls
    # ]
    _costs = []
    for url in cost_urls:
        try:
            ddf = dd.read_csv(
                url,
                compression="gzip",
                blocksize=None,
                dtype={
                    "foreign_item_id": str,
                    "foreign_store_id": str,
                    "date": str
                },
                usecols=[
                    "foreign_item_id",
                    "foreign_store_id",
                    "date"
                ]
            )
            df = ddf.compute()
            _costs.append(df)
        except:
            print(f'{url}:is not exists!')
            df = pd.DataFrame([],columns=["foreign_item_id","foreign_store_id","date"])
            _costs.append(df)
        
    # costs = dd.concat(_costs).compute()
    costs = pd.concat(_costs,sort=True)
    return costs