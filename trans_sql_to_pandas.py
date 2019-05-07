import pandas as pd
import numpy as np
from dask import dataframe as dd
import datetime
from pandasql import sqldf
from dblinks import redshift_link
from sqlalchemy import create_engine
import gc
from Reder import lastest_objects


# def get_date_series(edate:str):
#     date_series = [edate]
#     flag = 1
#     while 1:
#         date = (datetime.datetime.strptime(edate,'%Y-%m-%d') - datetime.timedelta(flag)).date()
#         if date <= datetime.datetime.strptime(edate,'%Y-%m-%d') - datetime.timedelta(28):
#             break
#         flag += 1
#         date_series.append(str(date))
#     return date_series


def get_date_series(edate, lens=27):
    date_series = []
    for i in range(lens, 0, -1):
        date = (datetime.datetime.strptime(
            edate, '%Y-%m-%d') - datetime.timedelta(i)).date()
        date_series.append(str(date))
    date_series.append(edate)
    return date_series


def generate_urls(suffix, source_id, date_series: list, head='s3', prefix='standard-data'):
    urls = []
    for date in date_series:
        year, month, day = date.split('-')
        url = f'{head}://{prefix}/{year}/{month}/{day}/{source_id}/{suffix}/*.gz'
        urls.append(url)
    return urls


def get_chain_goods(edate, source_id):
    year, month, day = edate.split('-')

    chain_goods_url = lastest_objects('standard-data',f'{year}/{month}/{day}/{source_id}/chain_goods','.gz')[0]
    # chain_goods_url = f's3://standard-data/{year}/{month}/{day}/{source_id}/chain_goods/*.gz'
    # chain_goods = dd.read_csv(chain_goods_url, blocksize=None,  usecols=[0, 1, 2, 3, 4, 5, 7, 16], compression='gzip',sep = ',',
    #                         names=['cmid', 'barcode', 'foreign_item_id', 'item_name',
    #                                 'lastin_price', 'sale_price', 'item_status', 'show_code'],
    #                         dtype={'cmid': np.int, 'foreign_item_id': np.str, 'barcode': np.str, 'show_code': np.str, 'item_name': np.str, 'item_status': np.str,
    #                                 'lastin_price': np.float, 'sale_price': np.float}
    #                         )
    
    # chain_goods = chain_goods.compute()

    chain_goods = pd.read_csv(chain_goods_url,engine='c',usecols=[0, 1, 2, 3, 4, 5, 7, 16], compression='gzip',sep = ',',
                            names=['cmid', 'barcode', 'foreign_item_id', 'item_name',
                                    'lastin_price', 'sale_price', 'item_status', 'show_code'],
                            dtype={'cmid': np.int, 'foreign_item_id': np.str, 'barcode': np.str, 'show_code': np.str, 'item_name': np.str, 'item_status': np.str}
                            )
    
    #过滤掉csv字段中含有分隔符的行
    filter_list = []
    def test(df):
        try:
            float(df['lastin_price'])
            float(df['sale_price'])
        except ValueError:
            filter_list.append(df['foreign_item_id'])
    chain_goods.apply(test,axis=1)

    chain_goods = chain_goods.loc[~chain_goods['foreign_item_id'].isin(filter_list)]
    chain_goods = chain_goods[(chain_goods['item_status'] != '淘汰') & (
        chain_goods['item_status'] != '淘汰品')]
    chain_goods = chain_goods.astype(dtype={'lastin_price': np.float, 'sale_price': np.float})
    chain_goods = chain_goods[['cmid', 'foreign_item_id', 'barcode',
                               'show_code', 'item_name', 'item_status', 'lastin_price', 'sale_price']]
    return chain_goods


def get_cost(date_series, source_id):
    urls = generate_urls(suffix='cost', source_id=source_id,
                         date_series=date_series)
    ddf = dd.read_csv(urls, blocksize=None, engine='c', usecols=[1, 2, 3, 5, 6, 7, 13], compression='gzip',sep = ',',
                      names=['foreign_store_id', 'foreign_item_id', 'date',
                             'sale_quantity', 'total_sale', 'total_cost', 'cmid'],
                      parse_dates=['date'],
                      dtype={'cmid': np.int, 'foreign_store_id': np.str, 'foreign_item_id': np.str, 'sale_quantity': np.float, 'total_sale': np.float, 'total_cost': np.float})
    df = ddf.compute()
    df.loc[df['total_sale'].isna() == True, 'total_sale'] = 0
    df['exist'] = df['total_sale'].apply(lambda x: 1 if x > 0 else 0)
    df = df[['cmid', 'foreign_store_id', 'foreign_item_id', 'date',
             'sale_quantity', 'total_sale', 'total_cost', 'exist']]
    return df


def get_inventory(date_series, source_id):
    urls = generate_urls(suffix='inventory',
                         source_id=source_id, date_series=date_series)
    ddf = dd.read_csv(urls, blocksize=None, engine='c', usecols=[0, 1, 2, 3, 4], compression='gzip',sep = ',',
                      names=['cmid', 'foreign_store_id',
                             'foreign_item_id', 'date', 'quantity'],
                      parse_dates=['date'],
                      dtype={'cmid': np.int, 'foreign_store_id': np.str, 'foreign_item_id': np.str, 'quantity': np.float})
    df = ddf.compute()
    df.loc[df['quantity'].isna() == True, 'quantity'] = 0
    return df


def get_chain_store(edate, source_id):
    year, month, day = edate.split(
        '-') if edate else str((datetime.date.today() - datetime.timedelta(1)).date()).split('-')
    chain_gstore_url = f's3://standard-data/{year}/{month}/{day}/{source_id}/chain_store/chain_store000.gz'
    df = pd.read_csv(chain_gstore_url, engine='c', usecols=[0, 1, 2, 6], compression='gzip',
                     names=['cmid', 'foreign_store_id',
                            'store_name', 'store_status'],
                     dtype={'cmid': np.int, 'foreign_store_id': np.str, 'store_name': np.str, 'store_status': np.str})
    return df

# select foreign_store_id,foreign_item_id,alc_way from {gdstore_alc_name} where foreign_store_id != '1000000'


def get_gdstore_alc(cmid):
    red_engine = create_engine(redshift_link)
    sql = f"select foreign_store_id,foreign_item_id,alc_way from haiding_gdstore_alc_{cmid} where foreign_store_id != '1000000'" \
        if cmid not in [34, 85, 92, 98] else \
        f"select foreign_store_id,foreign_item_id,alc_way from hongye_gdstore_alc_{cmid} where foreign_store_id != '1000000'"
    df = pd.read_sql_query(sql, red_engine)
    return df


def compute_psd_sql(edate, cmid):
    source_id = f'{cmid}YYYYYYYYYYYYY'[0:15]
    date_series = get_date_series(edate)
    cost_df = get_cost(date_series, source_id)
    del cost_df['sale_quantity']
    inventory_df = get_inventory(date_series, source_id)
    chain_goods = get_chain_goods(edate, source_id)
    # cost_df = cost_df.astype(dtype={'date':str})
    cost_df['date'] = pd.to_datetime(cost_df['date'])
    inventory_df['date'] = pd.to_datetime(inventory_df['date'])
    # inventory_df = inventory_df.astype(dtype={'date':str})
    full_sku_df = pd.merge(cost_df, inventory_df, how='outer', on=[
                           'cmid', 'foreign_store_id', 'foreign_item_id', 'date'])
    full_sku_df.loc[full_sku_df['quantity'].isna() == True, 'quantity'] = 0
    full_sku_df.loc[full_sku_df['total_sale'].isna() == True, 'total_sale'] = 0
    full_sku_df.loc[full_sku_df['total_cost'].isna() == True, 'total_cost'] = 0
    full_sku_df.loc[full_sku_df['exist'].isna() == True, 'exist'] = 1

    cost_inv_cgs_df = full_sku_df.merge(chain_goods, how='left', on=[
                                        'cmid', 'foreign_item_id'])

    cost_inv_cgs_df = cost_inv_cgs_df.groupby(['cmid', 'foreign_store_id', 'foreign_item_id', 'show_code'], as_index=False)\
        .agg({'lastin_price': max, 'sale_price': max, 'exist': sum, 'total_sale': sum, 'total_cost': sum})\
        .rename(columns={'exist': 'days', 'total_sale': 'sum_total_sale', 'total_cost': 'sum_total_cost', 'show_code': 'item_show_code'})
    cost_inv_cgs_df['psd'] = cost_inv_cgs_df.apply(
        lambda x: 0 if x['days'] == 0 else x['sum_total_sale']/x['days'], axis=1)
    cost_inv_cgs_df['psd_cost'] = cost_inv_cgs_df.apply(
        lambda x: x['psd'] * x['lastin_price'], axis=1)
    # datetime.date.today() - datetime.timedelta(1)
    cost_inv_cgs_df['date'] = edate
    cost_inv_cgs_df['date'] = pd.to_datetime(cost_inv_cgs_df['date'])
    cost_inv_cgs_df = cost_inv_cgs_df[['cmid', 'foreign_store_id', 'foreign_item_id',
                                       'sum_total_sale', 'psd', 'lastin_price', 'psd_cost', 'date', 'item_show_code']]
    cost_inv_cgs_df.reset_index()
    cost_inv_cgs_df.drop_duplicates(inplace=True)
    print(f'{edate}:done')
    return cost_inv_cgs_df
    # print(cost_inv_cgs_df.head())


def bd_product_info_sql(edate, cmid):
    source_id = f'{cmid}YYYYYYYYYYYYY'[0:15]
    date_series = get_date_series(edate)
    cost_df = get_cost(date_series, source_id)
    del cost_df['sale_quantity']
    inventory_df = get_inventory(date_series, source_id)
    cost_df = cost_df[cost_df['foreign_store_id'] != '1000000']
    inventory_df = inventory_df[inventory_df['foreign_store_id'] != '1000000']

    full_sku_df = pd.merge(cost_df, inventory_df, how='outer', on=[
                           'cmid', 'foreign_store_id', 'foreign_item_id', 'date'])
    full_sku_df.loc[full_sku_df['quantity'].isna() == True, 'quantity'] = 0
    full_sku_df.loc[full_sku_df['total_sale'].isna() == True, 'total_sale'] = 0
    full_sku_df.loc[full_sku_df['total_cost'].isna() == True, 'total_cost'] = 0
    full_sku_df.loc[full_sku_df['exist'].isna() == True, 'exist'] = 1
    # del cost_df,inventory_df
    # gc.collect()

    chain_goods = get_chain_goods(edate, source_id)
    chain_store = get_chain_store(edate, source_id)

    full_sku_df = full_sku_df.merge(chain_store, how='left', on=['cmid', 'foreign_store_id'])\
        .merge(chain_goods, how='left', on=['cmid', 'foreign_item_id'])
    full_sku_df = full_sku_df[['cmid', 'foreign_store_id', 'foreign_item_id', 'date', 'exist', 'total_cost', 'total_sale',
                               'lastin_price', 'sale_price', 'item_name', 'item_status', 'show_code', 'store_name', 'store_status', 'barcode']]
    # del chain_goods,chain_store
    # gc.collect()

    gdstore_alc = get_gdstore_alc(cmid)

    full_sku_df = full_sku_df.merge(gdstore_alc, how='left', on=[
                                    'foreign_store_id', 'foreign_item_id'])
    # del gdstore_alc
    # gc.collect()

    full_sku_df = full_sku_df.groupby(['cmid', 'foreign_store_id', 'foreign_item_id', 'item_name', 'item_status', 'show_code', 'store_name', 'store_status', 'barcode', 'alc_way'], as_index=False)\
        .agg({'exist': sum, 'total_sale': sum, 'total_cost': sum, 'lastin_price': max, 'sale_price': max})\
        .rename(columns={'exist': 'days', 'show_code': 'item_show_code', 'alc_way': 'delivery_method', 'store_name': 'foreign_store_name'})
    full_sku_df['psd'] = full_sku_df.apply(
        lambda x: 0 if x['days'] == 0 else x['total_sale']/x['days'], axis=1)
    full_sku_df['psd_cost'] = full_sku_df.apply(
        lambda x: x['psd'] * x['lastin_price'], axis=1)
    full_sku_df['avg_sale'] = full_sku_df.apply(
        lambda x: x['total_sale']/28, axis=1)
    full_sku_df['update_date'] = edate
    full_sku_df['update_date'] = pd.to_datetime(full_sku_df['update_date'])
    full_sku_df = full_sku_df[['cmid', 'foreign_store_id', 'foreign_store_name', 'foreign_item_id', 'barcode', 'item_name', 'item_status', 'item_show_code',
                               'delivery_method', 'store_status', 'days', 'psd', 'total_sale', 'total_cost', 'avg_sale', 'lastin_price', 'sale_price', 'psd_cost', 'update_date']]
    full_sku_df.drop_duplicates(inplace=True)
    full_sku_df = full_sku_df.astype(dtype={'days': int})
    print(f'{edate}:done')
    return full_sku_df


def actual_sale_sql_store_level(edate, cmid, store_id):
    source_id = f'{cmid}YYYYYYYYYYYYY'[0:15]
    date_series = get_date_series(edate)
    # date_series = date_series[:-1]
    cost_df = get_cost(date_series, source_id)
    cost_df = cost_df[cost_df['foreign_store_id'] == store_id]
    del cost_df['total_sale'], cost_df['total_cost'], cost_df['exist']

    inventory_df = get_inventory(date_series, source_id)
    inventory_df = inventory_df[inventory_df['foreign_store_id'] == store_id]

    full_sku_df = pd.merge(cost_df, inventory_df, how='outer', on=[
                           'cmid', 'foreign_store_id', 'foreign_item_id', 'date'])
    full_sku_df.loc[full_sku_df['quantity'].isna() == True, 'quantity'] = 0
    full_sku_df.loc[full_sku_df['sale_quantity'].isna() == True,
                    'sale_quantity'] = 0
    full_sku_df.drop_duplicates(inplace=True)
    full_sku_df.reset_index()
    # select cmid,foreign_store_id,foreign_item_id,date,quantity,sale_quantity,row_number() over(PARTITION by foreign_store_id,foreign_item_id ORDER BY date) rank_num
    full_sku_df['rank_num'] = full_sku_df.groupby(['foreign_store_id', 'foreign_item_id'])[
        'date'].rank(ascending=True, method='dense')  # min
    full_sku_df = full_sku_df.rename(columns={'quantity': 'actual_quantity'})
    full_sku_df = full_sku_df[['cmid', 'foreign_store_id', 'foreign_item_id',
                               'actual_quantity', 'sale_quantity', 'date', 'rank_num']]
    return full_sku_df


if __name__ == "__main__":
    edate = '2018-10-30'
    cmid = 43

    # sdate = str(datetime.date.today() - datetime.timedelta(28))

    # chain_goods = get_chain_goods(edate,'43YYYYYYYYYYYYY')
    # print(chain_goods.head(10))

    # date_series = get_date_series(edate)
    # cost_df = get_cost(date_series,'43YYYYYYYYYYYYY')
    # print(cost_df.head(10))

    # inventory_df = get_inventory(date_series,'43YYYYYYYYYYYYY')
    # print(inventory_df.head(10))

    # compute_psd_sql(date_series,43)
    # print(get_date_series('2018-12-01'))

    # date_series = get_date_series('2019-03-21')
    # df = get_chain_store(date_series[-1],'43YYYYYYYYYYYYY')
    # print(df.head())

    # bd_product_info_sql(edate,cmid)
    # df = actual_sale_sql_store_level(edate, cmid, '1000040')
    df = compute_psd_sql(edate, cmid)
    print(df.head())
