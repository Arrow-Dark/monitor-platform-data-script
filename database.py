import psycopg2
import pandas as pd
from sqlalchemy import *
from sqlalchemy.schema import CreateSchema
import os
import click
import datetime
import io


engine = create_engine("postgresql://monitor:123456@10.2.1.4:5432/monitor")
store_info_table = 'store_info_{}'
store_product_table = 'store_product_{}'

class Databse:
    def __int__(self):
        self.conn = None

    def create_store_table(self, table_name,engine):
        metadata = MetaData(engine)
        '''
        '''
        #table_name = store_info_table.format(cmid)
        #table_name = table_name
        Table(table_name, metadata,
              Column('foreign_store_id', VARCHAR),
              Column('store_name', VARCHAR, nullable=False),
              Column('store_lost_rate', Float),
              Column('lost_sku', INT),
              Column('all_sku', INT),
              Column('date', Date),
              Column('store_show_code', VARCHAR, nullable=True),
              Column('store_score', Float, nullable=True),
              Column('update_score', Float, nullable=True))

        metadata.create_all(engine)

    def create_store_product_table(self, table_name,engine):
        metadata = MetaData(engine)
        '''
        '''
        #table_name = store_product_table.format(cmid)
        Table(table_name, metadata,
              Column('foreign_store_id', VARCHAR),
              Column('item_name', VARCHAR),
              Column('actual_stock', Float),
              Column('cm_suggest', INT),
              Column('store_book', INT),
              Column('inv_delivery', INT),
              Column('avg_qty', INT),
              Column('actual_lost', INT),
              Column('barcode', VARCHAR, nullable=True),
              Column('foreign_item_id', VARCHAR, nullable=True),
              Column('store_show_code', VARCHAR, nullable=True),
              Column('date', Date, nullable=True),
              Column('theory_stock', INT, nullable=True),
              Column('actual_lost', INT, nullable=True),
              Column('lost_reason', INT, nullable=True),
              Column('psd_cost', Float, nullable=True),
              Column('item_score', Float, nullable=True),
              Column('update_score', Float, nullable=True)
              )

        metadata.create_all(engine)

    def create_conn(self):
        self.conn = psycopg2.connect(host='10.2.1.4', port=5432, database='monitor', user='monitor', password='123456')

    def to_store_info(self,df,engine,table_name):
        # excel_list = [('2019-03-03', 43), ('2019-03-04', 43), ('2019-03-02', 43), ('2019-03-01', 43),
        #               ('2019-02-28', 43)]
        # for i, cmid in excel_list:
        #     df = pd.read_excel("./{}.xlsx".format(i), sheet_name='Sheet1')
        #     df = df[['门店ID', '门店名称', '门店缺货率', '现门店已缺货 SKU 数', '门店统配商品全量 SKU 数']]
        #     df = df.rename(columns={"门店名称": "store_name", "门店ID": "foreign_store_id", "门店缺货率": "store_lost_rate",
        #                 "门店统配商品全量 SKU 数": "all_sku", "现门店已缺货 SKU 数": "lost_sku"})
        #     df['date'] = i
        #     df['show_code'] = None
        #     df['store_score'] = None
        #     df['update_score'] = None
        #     table_name = store_info_table.format(cmid)
            string_data_io = io.StringIO()
            df.to_csv(string_data_io, sep='|', index=False)
            string_data_io.seek(0)
            string_data_io.readline()  # remove header
            with engine.connect() as connection:
                with connection.connection.cursor() as cursor:
                    copy_cmd = "COPY %s FROM STDIN WITH DELIMITER '|' CSV" % table_name
                    cursor.copy_expert(copy_cmd, string_data_io)
                connection.connection.commit()

            # df = pd.read_excel("./{}.xlsx".format(i), sheet_name='Sheet2')
            # df = df[['门店ID', '商品名称', '当前库存', '上上次建议单', '上上次订货单', '上次送货单']]
            # df.rename(columns={"门店ID": "foreign_store_id", "商品名称": "item_name",
            #                    "上上次建议单": "actual_stock", "上上次订货单": "cm_suggest",
            #                    "上次送货单": "inv_delivery"})
            # df['avg_qty'] = None
            # df['actual_lost'] = None
            # df['barcode'] = None
            # df['foreign_item_id'] = None
            # df['store_show_code'] = None
            # df['date'] = i
            # df['theory_stock'] = None
            # df['actual_lost'] = None
            # df['lost_reason'] = None
            # df['psd_cost'] = None
            # df['item_score'] = None
            # df['update_score'] = None

            # table_name = store_product_table.format(cmid)
            # string_data_io = io.StringIO()
            # df.to_csv(string_data_io, sep='|', index=False)
            # string_data_io.seek(0)
            # string_data_io.readline()  # remove header
            # with engine.connect() as connection:
            #     with connection.connection.cursor() as cursor:
            #         copy_cmd = "COPY %s FROM STDIN WITH DELIMITER '|' CSV" % table_name
            #         cursor.copy_expert(copy_cmd, string_data_io)
            #     connection.connection.commit()


def to_store_info(df,engine,table_name):
    string_data_io = io.StringIO()
    df.to_csv(string_data_io, sep='|', index=False)
    string_data_io.seek(0)
    string_data_io.readline()  # remove header
    with engine.connect() as connection:
        with connection.connection.cursor() as cursor:
            copy_cmd = "COPY %s FROM STDIN WITH DELIMITER '|' CSV" % table_name
            cursor.copy_expert(copy_cmd, string_data_io)
        connection.connection.commit()

def delete_or_create_postgresql_table(conn,table_name,table_fields=None):
    with conn.connect() as connection:
        table_tup = connection.execute(f"SELECT tablename FROM pg_catalog.pg_tables WHERE schemaname = 'public' and tablename = '{table_name}' ").fetchall()
        tables = [tup[0] for tup in table_tup if len(tup)]
        if table_name in tables:
            #connection.execute(f"delete from {table_name}")
            connection.execute(f"drop table {table_name}")
            connection.execute(f"create table {table_name}({table_fields})")
        elif table_fields:
            connection.execute(f"create table {table_name}({table_fields})")


def write_to_table(df, table_name, db_engine, if_exists='replace',dtype=None):
    # db_engine = create_engine(db_url)# 初始化引擎
    string_data_io = io.StringIO()
    df.to_csv(string_data_io, sep='|', index=False)
    pd_sql_engine = pd.io.sql.pandasSQL_builder(db_engine)
    table = pd.io.sql.SQLTable(table_name, pd_sql_engine, frame=df,
                               index=False, if_exists=if_exists,schema = '',dtype=dtype)
    table.create()
    string_data_io.seek(0)
    string_data_io.readline()  # remove header
    with db_engine.connect() as connection:
        with connection.connection.cursor() as cursor:
            # cmid = table_name.split('_')[-1]
            # tname = '_'.join(table_name.split('_')[:-1])
            # if tname in INDEX_SQL:
            #     for _sql in INDEX_SQL[tname]:
            #         cursor.execute(_sql.format(cmid=cmid))
            copy_cmd = "COPY %s FROM STDIN WITH DELIMITER '|' CSV" %table_name
            cursor.copy_expert(copy_cmd, string_data_io)
        connection.connection.commit()


def append_to_table(df, table_name, db_engine, if_exists='append'):
    # db_engine = create_engine(db_url)# 初始化引擎
    string_data_io = io.StringIO()
    df.to_csv(string_data_io, sep='|', index=False)
    # pd_sql_engine = pd.io.sql.pandasSQL_builder(db_engine)
    # table = pd.io.sql.SQLTable(table_name, pd_sql_engine, frame=df,
    #                            index=False, if_exists=if_exists,schema = '')
    # table.create()
    string_data_io.seek(0)
    string_data_io.readline()  # remove header
    with db_engine.connect() as connection:
        with connection.connection.cursor() as cursor:
            copy_cmd = "COPY %s FROM STDIN WITH DELIMITER '|' CSV" %table_name
            cursor.copy_expert(copy_cmd, string_data_io)
        connection.connection.commit()


if __name__ == "__main__":
    pass

