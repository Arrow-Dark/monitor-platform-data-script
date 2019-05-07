from sqlalchemy import create_engine,MetaData,Table,Column,DATE,VARCHAR,INT,FLOAT
import pandas as pd
import datetime
import io
import gzip
import boto3
from dblinks import postgreSql_link,redshift_link,iam_link
from concurrent.futures import ThreadPoolExecutor,ProcessPoolExecutor
from database import to_store_info
# from all_sqls import compute_psd_sql
from trans_sql_to_pandas import compute_psd_sql
from move_data import get_obj_keys
from Reder import dfReder


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


def compute_psd(date,cmid):
    df = compute_psd_sql(date,cmid)
    return (df,date)


def store_to_s3_file(bucket, key, df, include_index=False):
    csv_buffer = io.StringIO()
    df.to_csv(csv_buffer, index=include_index)

    csv_buffer.seek(0)
    gz_buffer = io.BytesIO()

    with gzip.GzipFile(mode='w', fileobj=gz_buffer) as gz_file:
        gz_file.write(bytes(csv_buffer.getvalue(), 'utf-8'))

    s3_resource = boto3.resource('s3')
    s3_object = s3_resource.Object(bucket, key)
    s3_object.put(Body=gz_buffer.getvalue())

    print(' * uploading a df.shape:{} to path:{}/{}'.format(df.shape, bucket, key))
    return True


def read_concat_data(date_series,cmid):
    gre_engine = create_engine(postgreSql_link)
    table_name = f'psd_info_{cmid}'
    # db_obj = Databse()
    def write_psd_to_db(res):
        # df,date = res.result()
        df,date = res
        sdate = str((datetime.datetime.strptime(date,'%Y-%m-%d') - datetime.timedelta(84)).date())
        if df.empty:
            print('数据集为空！')
            return
        df['cmid'] = cmid
        df.astype(dtype={'cmid':int,'foreign_store_id':str,'foreign_item_id':str,'item_show_code':str},inplace=True)
        #df = df.rename(columns={"show_code": "item_show_code"})
        #df['date'] = pd.to_datetime(df['date'], format = "%Y-%m-%d")
        # with gre_engine.connect() as connection:
        #     connection.execute(f"delete from {table_name} where date = '{date}' or date <= '{sdate}'")
        # to_store_info(df,gre_engine,table_name)
        store_to_s3_file('backend-standard-data',f'projects/monitor/psd_info/{cmid}/{date}.csv.gz',df)

    date = date_series
    res = compute_psd(date,cmid)
    write_psd_to_db(res)
    
    # return all_df

def start_psd_compute(cmid):
    date_series = get_date_series(str(datetime.date.today() - datetime.timedelta(1)))
    # with ProcessPoolExecutor(max_workers=6) as executor:
    #     [executor.submit(read_concat_data,date,cmid) for date in date_series]
    [read_concat_data(date,cmid) for date in date_series]


if __name__ == "__main__":
    
    # date_series = get_date_series('2018-09-06')
    date_series = ['2018-10-31','2018-10-30']
    cmid = 43
    with ProcessPoolExecutor(max_workers=6) as executor:
        [executor.submit(read_concat_data,date,cmid) for date in date_series]

    


