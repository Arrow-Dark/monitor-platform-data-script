import os
import pandas as pd
from iam_client.iamclient import IAMClient


def get_iamclient(iam_json):
    '''

    :param iam_json:
    :return:
    '''
    IAM_API_BASE_URL = iam_json['IAM_API_BASE_URL']
    CACHE_IAM_USER = iam_json['CACHE_IAM_USER']
    CACHE_IAM_PASSWORD = iam_json['CACHE_IAM_PASSWORD']

    iamclient = IAMClient(IAM_API_BASE_URL,
                          CACHE_IAM_USER,
                          CACHE_IAM_PASSWORD)
    return iamclient


def get_enterprise_list(client, IAM_API_BASE_URL):
    '''

    :param client:
    :param IAM_API_BASE_URL:
    :return:
    '''
    API_ENTERPRISE_LIST = os.path.join(
        IAM_API_BASE_URL,
        'auth/permissions/enterprises/'
    )
    res = client.get(API_ENTERPRISE_LIST)
    return res


def get_enterprise_conf(cmid, client, IAM_API_BASE_URL):
    '''

    :param cmid:
    :param client:
    :param IAM_API_BASE_URL:
    :return:
    '''
    API_ENTERPRISE_DETAIL = os.path.join(
        IAM_API_BASE_URL,
        'auth/permissions/enterprises/{cmid}/'
    )
    res = client.get(API_ENTERPRISE_DETAIL.format(cmid=cmid))
    return res


def get_item_status_map(cmid, client, IAM_API_BASE_URL):
    '''

    :param cmid:
    :param client:
    :param IAM_API_BASE_URL:
    :return:
    '''
    API_ENTERPRISE_ITEM_STATUS = os.path.join(
        IAM_API_BASE_URL,
        'auth/permissions/item-status-map/{cmid}/'
    )
    res = client.get(API_ENTERPRISE_ITEM_STATUS.format(cmid=cmid))
    return res


def update_cache_version(cmid, client, cv, IAM_API_BASE_URL, project='op-cache'):
    '''

    :param cmid:
    :param client:
    :param cv:
    :param IAM_API_BASE_URL:
    :param project:
    :return:
    '''
    API_UPDATE_CACHE_VERSION = os.path.join(
        IAM_API_BASE_URL,
        'auth/backend/update-cache-version/'
    )
    res = client.post(
        API_UPDATE_CACHE_VERSION,
        payload='cmid={cmid}&cv={cv}&project={project}'.format(cmid=cmid, cv=cv, project=project))
    return res


def get_refresh_datetime(cmid, client, IAM_API_BASE_URL):
    '''

    :param cmid:
    :param client:
    :param IAM_API_BASE_URL:
    :return:
    '''
    API_GET_REFRESH_DATETIME = os.path.join(
        IAM_API_BASE_URL,
        'auth/backend/get_ep_source_time/{cmid}/'
    )
    res = client.get(API_GET_REFRESH_DATETIME.format(cmid=cmid))
    return res


def get_store_property_map(cmid, client, IAM_API_BASE_URL):
    API_STORE_PROPERTY_MAP = os.path.join(
        IAM_API_BASE_URL,
        'auth/permissions/store-property-map/{cmid}/'
    )
    res = client.get(API_STORE_PROPERTY_MAP.format(cmid=cmid))
    return res


def get_store_property_frame(cmid, client, IAM_API_BASE_URL):
    store_property_map = get_store_property_map(cmid, client, IAM_API_BASE_URL)
    store_property_frame = pd.DataFrame(
        store_property_map['result'],
        columns=['store_property', 'cm_store_property_id']
    )
    store_property_frame.rename(columns={'store_property': 'property'}, inplace=True)
    return store_property_frame


if __name__ == '__main__':
    get_enterprise_conf(34)
