import logging
import pandas as pd
import datetime as dt
from db_utils import get_df, REDSHIFT_DB_URL, SERVERDB_DB_URL, write_to_table
from s3_utils import get_obj_keys, get_s3_data, get_s3_dask
import datetime
from sqlalchemy import create_engine
logger = logging.getLogger('sku_info')


def assert_df(df):
    logger.info('shape: {}'.format(df.shape))
    try:
        assert df.shape[0] != 0, '表不應該為空'
    except Exception as e:
        logger.error(e)
        raise e


def cache_store_item_sku(source_id, cmid, item_cat1_ignores, days_delay):
    '''
        動銷SKU數
        粒度: 到小类 到品 月至今
        表來源: goodsflow
    '''
    sql = '''         SELECT m.foreign_item_id,
                             m.foreign_store_id
				        FROM 
				    ((SELECT DISTINCT t1.foreign_item_id, t1.foreign_store_id
                        FROM cost_{source_id} t1
                   LEFT JOIN chain_goods t2
                          ON t1.cmid = t2.cmid
                         AND t1.foreign_item_id = t2.foreign_item_id
                       WHERE date >= (getdate()::date - 28 - {days_delay})
                         AND date <= (getdate()::date - {days_delay})
                         AND t1.cmid = {cmid}
                         AND t2.foreign_category_lv1 not in ({item_cat1_ignores}))
                       union 
                     (SELECT DISTINCT t1.foreign_item_id, t1.foreign_store_id
                        FROM inventory_{source_id} t1
                   LEFT JOIN chain_goods t2
                          ON t1.cmid = t2.cmid
                         AND t1.foreign_item_id = t2.foreign_item_id
                       WHERE date >= (getdate()::date - 28 - {days_delay})
                         AND date <= (getdate()::date - {days_delay})
                         AND t1.cmid = {cmid}
                         AND t2.foreign_category_lv1 not in ({item_cat1_ignores})
                         AND t1.quantity > 0
                         AND t1.amount > 0))m
                            '''.format(source_id=source_id,
                                       cmid=cmid,
                                       item_cat1_ignores="''" if not item_cat1_ignores else ','.join(
                                           ["\'{}\'".format(s) for s in item_cat1_ignores]),
                                       days_delay=days_delay)
    logger.info(sql)
    df = get_df(REDSHIFT_DB_URL, sql)

    sql = '''
            SELECT
                foreign_item_id,
                item_name
            FROM chain_goods
            WHERE cmid = {cmid}
        '''.format(cmid=cmid)
    logger.info(sql)
    item_df = get_df(REDSHIFT_DB_URL, sql)
    print(item_df.head(5))

    sql = '''
                SELECT
                    foreign_store_id,
                    store_name
                FROM chain_store
                WHERE cmid = {cmid}
            '''.format(cmid=cmid)
    logger.info(sql)
    store_df = get_df(REDSHIFT_DB_URL, sql)
    print(store_df.head(5))

    df = pd.merge(df, item_df, how='left', on='foreign_item_id')
    df = pd.merge(df, store_df, how='left', on='foreign_store_id')

    df.rename(columns={'item_name': 'foreign_item_name'}, inplace=True)
    df.rename(columns={'store_name': 'foreign_store_name'}, inplace=True)

    write_to_table(df, 'bd_product_store_{}'.format(cmid), SERVERDB_DB_URL)
    return df


def get_cur_monday(date: str):
    date = (datetime.datetime.strptime(date, '%Y-%m-%d')).date()
    weekday = date.weekday()
    date = str(date - datetime.timedelta(weekday))
    return date


def get_week_store_info(cmid, days_delay):
    engine = create_engine(SERVERDB_DB_URL)
    end = dt.date.today() - dt.timedelta(days=days_delay)
    week_day = end.weekday()
    cur_monday = end - dt.timedelta(days=week_day)
    before_monday = cur_monday - dt.timedelta(days=77)
    print(end, before_monday)

    # s3_urls = get_obj_keys(cmid)
    # s3_product_df = get_s3_data(s3_urls, 'Sheet2', dtype={'门店ID': str, '门店名称': str, '商品show_code': str})
    # if s3_product_df.empty:
    #     # s3_product_df = pd.DataFrame([],columns=['foreign_store_id', 'item_show_code', 'date'])
    #     return
    s3_product_df = pd.read_sql_query(
        f'select foreign_store_id,item_show_code,date from store_product_info_{cmid}', engine)
    # s3_product_df = s3_product_df[['foreign_store_id', 'item_show_code','date']]
    s3_product_df = s3_product_df.astype(dtype={'date': str})

    # s3_product_df = s3_product_df[['foreign_store_id','date']]
    s3_product_df['lost_sku'] = 1
    db_product_df = pd.read_sql_query(
        f'select foreign_store_id,date,lost_sku,all_sku from store_info_{cmid}', engine)
    db_product_df = db_product_df.astype(dtype={'date': str})

    # s3_product_df['date'] = pd.to_datetime(s3_product_df['date'])

    # s3_product_df = s3_product_df[(s3_product_df['date']<=end)&(s3_product_df['date']>=before_monday)]
    # s3_product_df['y'] = s3_product_df['date'].dt.strftime('%Y')
    # s3_product_df['wk'] = s3_product_df['date'].dt.strftime('%V')
    # del s3_product_df['date']
    # s3_product_df = s3_product_df.drop_duplicates()

    # week_list = [cur_monday]
    # for i in range(11):
    #     cur_monday = cur_monday - dt.timedelta(days=7)
    #     week_list.append(cur_monday)
    # week_list.reverse()
    # dates = pd.DataFrame({'date': week_list})
    # dates['date'] = pd.to_datetime(dates['date'])
    # dates['y'] = dates['date'].dt.strftime('%Y')
    # dates['wk'] = dates['date'].dt.strftime('%V')

    # s3_product_df = pd.merge(s3_product_df, dates, how='left', on=('y', 'wk'))
    # del s3_product_df['y']
    # del s3_product_df['wk']
    s3_product_df['date'] = s3_product_df['date'].apply(get_cur_monday)
    s3_product_df = s3_product_df.drop_duplicates()
    del s3_product_df['item_show_code']
    used_1_df = db_product_df[db_product_df['lost_sku']
                              == 0][['foreign_store_id', 'date', 'lost_sku']]
    used_1_df['date'] = used_1_df['date'].apply(get_cur_monday)
    s3_product_df = pd.concat([s3_product_df, used_1_df], sort=False)
    s3_product_df['date'] = pd.to_datetime(s3_product_df['date'])
    s3_product_df = s3_product_df[(s3_product_df['date'] <= end) & (
        s3_product_df['date'] >= before_monday)]

    # s3_product_df = s3_product_df.groupby(['foreign_store_id', 'date']).count().reset_index()
    # s3_product_df.rename(columns={'item_show_code': 'lost_sku'}, inplace=True)
    s3_product_df = s3_product_df.groupby(
        ['foreign_store_id', 'date'], as_index=False).agg({"lost_sku": sum})
    # used_1_df['date'] = used_1_df['date'].apply(get_cur_monday)
    # used_1_df['date'] = pd.to_datetime(used_1_df['date'])
    # used_1_df = used_1_df[(used_1_df['date']<=end)&(used_1_df['date']>=before_monday)]
    # used_1_df = used_1_df.groupby(['foreign_store_id', 'date'],as_index=False).agg({"lost_sku":sum})
    # s3_product_df = pd.concat([s3_product_df,used_1_df],sort=False)

    print(s3_product_df.head(20))

    # days = (end - before_monday).days
    # date_list = [end]
    # for i in range(days):
    #     end = end - dt.timedelta(days=1)
    #     date_list.append(end)

    # # all_df = get_s3_dask(date_list, cmid)
    # all_df = get_s3_data(s3_urls, 'Sheet1', dtype={'门店ID': str, '门店名称': str})
    # if all_df.empty:
    #     return

    all_df = db_product_df[['foreign_store_id', "all_sku", 'date']]
    all_df = all_df.astype(dtype={'date': str})
    all_df['date'] = all_df['date'].apply(get_cur_monday)
    all_df['date'] = pd.to_datetime(all_df['date'])
    all_df = all_df.groupby(['foreign_store_id', 'date'],
                            as_index=False).agg({"all_sku": max})

    # all_df['date'] = pd.to_datetime(all_df['date'])
    # all_df['y'] = all_df['date'].dt.strftime('%Y')
    # all_df['wk'] = all_df['date'].dt.strftime('%V')
    # del all_df['date']
    # all_df = all_df.drop_duplicates()
    # all_df = pd.merge(all_df, dates, how='left', on=('y', 'wk'))
    # del all_df['y']
    # del all_df['wk']

    # all_df['date'] = all_df['date'].apply(get_cur_monday)
    # all_df['date'] = pd.to_datetime(all_df['date'])
    # all_df = all_df.drop_duplicates()
    # print(all_df.head(20))

    # all_df = all_df.groupby(['foreign_store_id', 'date']).count().reset_index()
    print(all_df.head(20))

    df = pd.merge(s3_product_df, all_df, how='left',
                  on=('foreign_store_id', 'date'))
    print('df:', (df[df['foreign_store_id'] != '1000000']).head(20))

    # df.rename(columns={'item_show_code': 'lost_sku'}, inplace=True)
    # df.rename(columns={'foreign_item_id': 'all_sku'}, inplace=True)

    df['store_lost_rate'] = df['lost_sku']/df['all_sku']
    df = df.set_index('date').shift(6, freq='D').reset_index()
    df['y'] = df['date'].dt.strftime('%Y')
    df['wk'] = df['date'].dt.strftime('%V')

    sql = '''
                    SELECT
                        foreign_store_id,
                        show_code,
                        store_name
                    FROM chain_store
                    WHERE cmid = {cmid}
                '''.format(cmid=cmid)
    logger.info(sql)
    store_df = get_df(REDSHIFT_DB_URL, sql)

    df = pd.merge(df, store_df, how='left', on='foreign_store_id')
    df.rename(columns={'show_code': 'store_show_code'}, inplace=True)

    write_to_table(df, 'store_info_week_{}'.format(cmid), SERVERDB_DB_URL)
