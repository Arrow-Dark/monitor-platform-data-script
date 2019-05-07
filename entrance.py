from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from dblinks import postgreSql_link,redshift_link,iam_link,aim_link
from bd_product_info import compute_and_write_db_product_info
from move_data import main
from supervisor_info import read_or_create_cm_idenities
from theory_inventory_map import task_entrance
from product_week import read_store_product
from psd_compute import start_psd_compute
from original2aim import move_data
# from compute_cache import compute_cache
import sys
import datetime
import requests
if __name__ == "__main__":
    gre_engine = create_engine(postgreSql_link)
    red_engine = create_engine(redshift_link)
    iam_engine = create_engine(iam_link)
    aim_conn = create_engine(aim_link)
    cmids = [43,85]
    for cmid in cmids:
        start_psd_compute(cmid)
        print(f'psd_info_{cmid} 完成')
        sys.exit()
        compute_and_write_db_product_info(red_engine,gre_engine,cmid)
        print(f'bd_product_info_{cmid} 完成')
        main(cmid,str(datetime.date.today() - datetime.timedelta(1)))
        print(f'store_info_{cmid},store_product_info_{cmid} 完成')
        read_store_product(gre_engine,cmid)
        print(f'store_product_week_{cmid} 完成')
        read_or_create_cm_idenities(iam_engine, gre_engine)
        print(f'store_product_week_{cmid} 完成')
        # compute_cache(cmid)
        print(f'store_info_week_{cmid} 完成')
        #task_entrance(cmid)
        #move_data(gre_engine,aim_conn)
    
    # move_data(gre_engine,aim_conn)
    # res = requests.get('https://big-monitor.chaomengdata.com/monitor/cache/flush/',
    #                     headers = {'TIMESTAMP':'1554780611','SECRET':'e467ac7ee199bb5107d2134c94a64cb2'})
    #print(res.json())    


