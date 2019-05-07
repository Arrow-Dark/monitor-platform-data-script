from sqlalchemy import create_engine
import pandas as pd
import os
import click
import datetime
import io
import time
import boto3
import json
from concurrent.futures import ThreadPoolExecutor
from Reder import feach_allStore, dfReder, read_redshift
from dblinks import postgreSql_link


def new_read_store_product(conn, cmid):
    read_store_product_sql = '''
        drop table IF EXISTS new_store_product_week_{cmid};
        create TABLE new_store_product_week_{cmid} as 
        (select 
        foreign_store_id,
        store_show_code,
        sum(cm_suggest) cm_suggest,
        sum(store_book) store_book,
        sum(inv_delivery) inv_delivery,
        sum(return_goods) return_goods,
        sum(other_reason) other_reason,
        week_first_day date,
        EXTRACT('year' from week_first_day) y,
        EXTRACT('week' from week_first_day) wk
        from
        (select 
        foreign_store_id,
        store_show_code,
        case when lost_reason = 1 then 1 else 0 end cm_suggest, 
        case when lost_reason = 2 then 1 else 0 end store_book, 
        case when lost_reason = 3 then 1 else 0 end inv_delivery,
        case when lost_reason = 4 then 1 else 0 end return_goods,
        case when lost_reason = 5 then 1 else 0 end other_reason,
        date,
        date_trunc('week', date::date)::date + 6 week_first_day
        from new_store_product_info_{cmid}) t
        GROUP BY foreign_store_id,store_show_code,week_first_day
        ORDER BY week_first_day);
        drop table IF EXISTS new_store_info_week_{cmid};
        create TABLE new_store_info_week_{cmid} as 
        (select 
            t1.foreign_store_id,
            t2.store_show_code,
            t1.store_name,
            t2.lost_sku,
            t1.all_sku,
            t2.lost_sku/t1.all_sku::FLOAT store_lost_rate,
            t1.week_first_day date,
            EXTRACT('year' from t1.week_first_day) y,
            EXTRACT('week' from t1.week_first_day) wk
        from
            (select 
                foreign_store_id,
                store_name,
                max(all_sku) all_sku,
                date_trunc('week', date)::date + 6 week_first_day 
            from new_store_info_{cmid}
            GROUP BY foreign_store_id,store_name,week_first_day) t1
        left join
            (select 
                foreign_store_id,
                store_show_code,
                date_trunc('week', date)::date + 6 week_first_day,
                count(DISTINCT foreign_item_id) lost_sku 
            FROM new_store_product_info_{cmid} 
            GROUP BY foreign_store_id,store_show_code,week_first_day) t2
        on t1.foreign_store_id = t2.foreign_store_id and t1.week_first_day = t2.week_first_day)
    '''
    sql = read_store_product_sql.format(cmid=cmid)
    with conn.connect() as connection:
        connection.execute(sql)


if __name__ == "__main__":
    engine = create_engine(postgreSql_link)
    new_read_store_product(engine, 85)
