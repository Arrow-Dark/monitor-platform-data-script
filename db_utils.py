import os
from sqlalchemy import create_engine, exc, text as sql_text
from sqlalchemy.ext.declarative import declarative_base
import pandas as pd
import io
from dblinks import postgreSql_link,redshift_link

# REDSHIFT_DB_URL = os.environ.get('REDSHIFT_DATA')
# SERVERDB_DB_URL = os.environ.get('MONITOR_DATA')
# SERVERDB_DB_URL = os.environ.get('DJANGO_DB_URL')

REDSHIFT_DB_URL = redshift_link
SERVERDB_DB_URL = postgreSql_link

assert REDSHIFT_DB_URL != None, 'Redshift URL == None'
assert SERVERDB_DB_URL != None, 'Django DB URL == None'

SLIDE_NUM = 5000


INDEX_SQL = {
    "day_item_history": ['CREATE INDEX dih_{cmid}_fiid_fsid_idx ON day_item_history_{cmid} ("foreign_item_id", "foreign_store_id");'],
    "month_item_history": ['CREATE INDEX mih_{cmid}_fiid_fsid_idx ON month_item_history_{cmid} ("foreign_item_id", "foreign_store_id")'],
    "week_item_history": ['CREATE INDEX wih_{cmid}_fiid_fsid_idx ON week_item_history_{cmid} ("foreign_item_id", "foreign_store_id");'],
    "item_ring": ['CREATE INDEX itmr_{cmid}_sc_fiid_idx ON item_ring_{cmid} ("show_code", "foreign_item_id");'],
    "opv3_item": ['CREATE INDEX opv3_{cmid}_fiid_idx ON opv3_item_{cmid} ("foreign_item_id");'],
    "store_category_daily_report": ['CREATE INDEX scdr_{cmid}_fsid_lv_idx ON store_category_daily_report_{cmid} ("foreign_store_id", "level");'],
    "store_category_monthly_report": ['CREATE INDEX scmr_{cmid}_fsid_lv_idx ON store_category_monthly_report_{cmid} ("foreign_store_id", "level");'],
    "store_category_weekly_report": ['CREATE INDEX scwr_{cmid}_fsid_lv_idx ON store_category_weekly_report_{cmid} ("foreign_store_id", "level");'],
    "store_item_daily_report": [
        'CREATE INDEX sidr_{cmid}_fsid_fiid_idx ON store_item_daily_report_{cmid} ("foreign_store_id", "foreign_item_id");',
        'CREATE INDEX sidr_{cmid}_fsid_cat_idx ON store_item_daily_report_{cmid} ("foreign_store_id", "foreign_category_lv3");'
        ],
    "store_item_monthly_report": [
        'CREATE INDEX simr_{cmid}_fsid_fiid_idx ON store_item_monthly_report_{cmid} ("foreign_store_id", "foreign_item_id");',
        'CREATE INDEX simr_{cmid}_fsid_cat_idx ON store_item_monthly_report_{cmid} ("foreign_store_id", "foreign_category_lv3");'
        ],
    "store_item_weekly_report": [
        'CREATE INDEX siwr_{cmid}_fsid_fiid_idx ON store_item_weekly_report_{cmid} ("foreign_store_id", "foreign_item_id");',
        'CREATE INDEX siwr_{cmid}_fsid_cat_idx ON store_item_weekly_report_{cmid} ("foreign_store_id", "foreign_category_lv3");'
        ]

}

def get_df(db_url, query):
    engine = create_engine(db_url)
    con = engine.connect()
    try:
        return pd.read_sql_query(sql_text(query), con=engine)
    except exc.SQLAlchemyError as e:
        print(e)
    finally:
        con.close()


def set_df(db_url, df, databases):
    engine = create_engine(db_url)
    con = engine.connect()
    try:
        df.to_sql(databases, con=engine, if_exists='replace', index=False)
    except exc.SQLAlchemyError as e:
        print(e)
    finally:
        con.close()


def append_set_df(db_url, df, databases):
    engine = create_engine(db_url)
    con = engine.connect()
    try:
        df.to_sql(databases, con=engine, if_exists='append', index=False)
    except exc.SQLAlchemyError as e:
        print(e)
    finally:
        con.close()


def exist_df(db_url, databases):
    engine = create_engine(db_url)
    Base = declarative_base()
    Base.metadata.reflect(engine)
    tables = Base.metadata.tables
    if databases in tables.keys():
        return True
    else:
        return False


def append_set_slide_df(db_url, df, databases):
    df_size = df.shape[0]
    if df_size <= SLIDE_NUM:
        append_set_df(db_url, df, databases)
    else:
        append_set_df(db_url, df.iloc[0: SLIDE_NUM], databases)
        engine = create_engine(SERVERDB_DB_URL)
        con = engine.connect()
        num = (df_size // SLIDE_NUM) + 1
        try:
            for i in range(1, num):
                print(i)
                slidf_df = df.iloc[SLIDE_NUM*i: SLIDE_NUM*i+SLIDE_NUM]
                slidf_df.to_sql(databases, con=engine, if_exists='append', index=False)
        except exc.SQLAlchemyError as e:
            print(e)
        finally:
            con.close()


def set_slide_df(db_url, df, databases):
    df_size = df.shape[0]
    if df_size <= SLIDE_NUM:
        set_df(db_url, df, databases)
    else:
        set_df(db_url, df.iloc[0: SLIDE_NUM], databases)
        engine = create_engine(SERVERDB_DB_URL)
        con = engine.connect()
        num = (df_size // SLIDE_NUM) + 1
        try:
            for i in range(1, num):
                print(i)
                slidf_df = df.iloc[SLIDE_NUM*i: SLIDE_NUM*i+SLIDE_NUM]
                slidf_df.to_sql(databases, con=engine, if_exists='append', index=False)
        except exc.SQLAlchemyError as e:
            print(e)
        finally:
            con.close()


def write_to_table(df, table_name, db_url, if_exists='replace'):
    db_engine = create_engine(db_url)# 初始化引擎
    string_data_io = io.StringIO()
    df.to_csv(string_data_io, sep='|', index=False)
    pd_sql_engine = pd.io.sql.pandasSQL_builder(db_engine)
    table = pd.io.sql.SQLTable(table_name, pd_sql_engine, frame=df,
                               index=False, if_exists=if_exists,schema = '')
    table.create()
    string_data_io.seek(0)
    string_data_io.readline()  # remove header
    with db_engine.connect() as connection:
        with connection.connection.cursor() as cursor:
            cmid = table_name.split('_')[-1]
            tname = '_'.join(table_name.split('_')[:-1])
            if tname in INDEX_SQL:
                for _sql in INDEX_SQL[tname]:
                    cursor.execute(_sql.format(cmid=cmid))
            copy_cmd = "COPY %s FROM STDIN WITH DELIMITER '|' CSV" %table_name
            cursor.copy_expert(copy_cmd, string_data_io)
        connection.connection.commit()


def append_to_table(df, table_name, db_url, if_exists='append'):
    db_engine = create_engine(db_url)# 初始化引擎
    string_data_io = io.StringIO()
    df.to_csv(string_data_io, sep='|', index=False)
    string_data_io.seek(0)
    string_data_io.readline()  # remove header
    with db_engine.connect() as connection:
        with connection.connection.cursor() as cursor:
            copy_cmd = "COPY %s FROM STDIN WITH DELIMITER '|' CSV" %table_name
            cursor.copy_expert(copy_cmd, string_data_io)
        connection.connection.commit()
