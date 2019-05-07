import pandas as pd
import numpy as np
from sqlalchemy import create_engine
import datetime
from dblinks import postgreSql_link,redshift_link,aim_link,backdb_link
from database import Databse
from database import to_store_info



def move_data(org_conn,aim_conn):
    #original_link = "postgresql://monitor:123456@10.2.1.4:5432/monitor"
    
    # with org_conn.connect() as connection:
    #     table_tup = connection.execute(f"SELECT tablename FROM pg_catalog.pg_tables WHERE schemaname = 'public'").fetchall()
    #     tables = [tup[0] for tup in table_tup if len(tup) and (tup[0] not in ['bd_product_history_85','bd_product_history_85_temp','task_info','task_item_43','task_store_43','trigger_store_product_43'] and 'auth_' not in tup[0] and 'dis_config_' not in tup[0] and 'django_' not in tup[0])]
    tables = ['store_info_43','store_info_week_43','store_product_info_43','store_product_week_43']
    with aim_conn.connect() as connection:
        # table_tup_2 = connection.execute(f"SELECT tablename FROM pg_catalog.pg_tables WHERE schemaname = 'public'").fetchall()
        # tables_2 = [tup[0] for tup in table_tup_2 if len(tup)]
        for table in tables:
            sql = f'''
                    SELECT a.attname AS field,t.typname AS type
                    FROM pg_class c,pg_attribute a,pg_type t
                    WHERE c.relname = '{table}' and a.attnum > 0 and a.attrelid = c.oid and a.atttypid = t.oid
                    ORDER BY a.attnum
                '''
            df = pd.read_sql_query(sql,org_conn)
            df['create_table_fields'] = df['field'] + ' ' + df['type']
            table_fields = ','.join(df['create_table_fields'].tolist())

            connection.execute(f"drop table IF EXISTS {table}_temp;create table IF not EXISTS {table}_temp({table_fields})")
            tab_df = pd.read_sql_query(f'select * from {table}',org_conn)
            # tab_df = tab_df.replace(np.nan,None)
            to_store_info(tab_df,aim_conn,f'{table}_temp')
            if 'store_product_info_' in table or 'bd_product_info_' in table:
                connection.execute(
                            f'''ALTER table IF EXISTS {table} RENAME TO {table}_bck ; 
                                ALTER table IF EXISTS {table}_temp RENAME TO {table};
                                drop table IF EXISTS {table}_bck;
                                CREATE INDEX {table}_union_index ON {table} (foreign_item_id,foreign_store_id);
                                CREATE INDEX {table}_index ON {table} (foreign_item_id);''')
            else:
                connection.execute(
                            f'''ALTER table IF EXISTS {table} RENAME TO {table}_bck ; 
                                ALTER table IF EXISTS {table}_temp RENAME TO {table};
                                drop table IF EXISTS {table}_bck;''')
            print(f'{table} copied!')

def backup_data(aim_conn,back_conn):
    with aim_conn.connect() as connection:
        table_tup = connection.execute(f"SELECT tablename FROM pg_catalog.pg_tables WHERE schemaname = 'public'").fetchall()
        # tables = [tup[0] for tup in table_tup if len(tup) if tup[0] not in ['bd_product_history_43_temp','task_info','task_item_43','task_store_43','trigger_store_product_43']]
        tables = [tup[0] for tup in table_tup if len(tup) if tup[0]]
    # tables = ['bd_product_info_43','psd_info_43','store_product_info_43','store_product_week_43','bd_product_history_43']
    # tables = ['bd_product_history_43']
    with back_conn.connect() as connection:
        # table_tup_2 = connection.execute(f"SELECT tablename FROM pg_catalog.pg_tables WHERE schemaname = 'public'").fetchall()
        # tables_2 = [tup[0] for tup in table_tup_2 if len(tup)]
        for table in tables:
            sql = f'''
                    SELECT a.attname AS field,t.typname AS type
                    FROM pg_class c,pg_attribute a,pg_type t
                    WHERE c.relname = '{table}' and a.attnum > 0 and a.attrelid = c.oid and a.atttypid = t.oid
                    ORDER BY a.attnum
                '''
            df = pd.read_sql_query(sql,aim_conn)
            df['create_table_fields'] = df['field'] + ' ' + df['type']
            table_fields = ','.join(df['create_table_fields'].tolist())

            connection.execute(f"drop table IF EXISTS {table}_temp;create table IF not EXISTS {table}_temp({table_fields})")
            tab_df = pd.read_sql_query(f'select * from {table}',aim_conn)
            to_store_info(tab_df,back_conn,f'{table}_temp')
            # if table 
            connection.execute(f"drop table IF EXISTS {table}_bck;ALTER table IF EXISTS {table} RENAME TO {table}_bck ; ALTER table IF EXISTS {table}_temp RENAME TO {table};drop table IF EXISTS {table}_bck")
            print(f'{table} copied!')

    


if __name__ == "__main__":
    #print(get_use_date(43))
    org_conn = create_engine(postgreSql_link)
    aim_conn = create_engine(aim_link)
    back_conn = create_engine(backdb_link)
    move_data(org_conn,aim_conn)
    # backup_data(aim_conn,org_conn)