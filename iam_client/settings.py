import os
from dblinks import IAM_API_BASE_URL

IAM_API_BASE_URL = os.environ.get('IAM_API_BASE_URL')
assert IAM_API_BASE_URL != None, 'IAM_API_BASE_URL == None'

API_LOGIN = os.path.join(IAM_API_BASE_URL, 'auth/login/')
API_REFRESH_TOKEN = os.path.join(IAM_API_BASE_URL, 'auth/refresh-token/')