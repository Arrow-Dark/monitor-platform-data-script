
import click

from settings import IAM_API_BASE_URL, CACHE_IAM_PASSWORD, CACHE_IAM_USER
from iam_utils import get_iamclient, get_enterprise_conf
from tables_utils import get_week_store_info, cache_store_item_sku

import datetime as dt

# @click.command()
# @click.argument('cmid')
def compute_cache(cmid):
    """
        1. 通過IAM服務拿取企業配置
        2. 對指定企業計算緩存
        3. 把計算給果存放在指定的目錄
    """

    print(' * 通過IAM服務獲取企業: {} 的配置...'.format(cmid))

    iam_json = {"IAM_API_BASE_URL": IAM_API_BASE_URL, "CACHE_IAM_USER": CACHE_IAM_USER,  "CACHE_IAM_PASSWORD": CACHE_IAM_PASSWORD}

    iamclient = get_iamclient(iam_json)
    conf = get_enterprise_conf(cmid, iamclient, IAM_API_BASE_URL)
    if conf['status'] != 'ok':
        print(conf['msg'])
        return

    source_id = conf['source_id']
    days_delay = conf['days_delay']
    item_cat1_ignores = conf['filtered_conf']['metrics_ignores']['item_cat1']

    if int(cmid) == 3201 or int(cmid) == 3202:
        source_id = '32yyyyyyyyyyyyy'

    cache_store_item_sku(source_id, cmid, item_cat1_ignores, days_delay)

    get_week_store_info(cmid, days_delay)


if __name__ == '__main__':
    compute_cache(43)
