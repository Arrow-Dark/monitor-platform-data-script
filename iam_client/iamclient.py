import requests
from os.path import join


class IAMClient:

    def __init__(self, iam_url, username, password):
        self.has_logged_on = False
        self.API_REFRESH_TOKEN = join(iam_url, 'auth/refresh-token/')
        self.API_LOGIN = join(iam_url, 'auth/login/')
        r = requests.post(
            self.API_LOGIN,
            data={
                'username': username,
                'password': password
            }
        )
        res = r.json()
        status = res.get('status')
        if status == 'ok':
            self.access_token = res['access']
            self.refresh_token = res['refresh']
            self.has_logged_on = True
        else:
            raise Exception(res)

    def _has_logged_on(self):
        if not self.has_logged_on:
            raise Exception('需要重新登錄')

    def refresh_token(self):
        self._has_logged_on()

        r = requests.post(
            self.API_REFRESH_TOKEN,
            data={
                'refresh': self.refresh_token,
            }
        )
        res = r.json()
        status = res.get('status')
        if status == 'ok':
            self.access_token = res['access']
        else:
            raise Exception(res)

    def get(self, api, payload=None):
        self._has_logged_on()
        headers = {'Authorization': 'Bearer {}'.format(self.access_token)}
        r = requests.get(api, params=payload, headers=headers)
        return r.json()

    def post(self, api, payload=None):
        self._has_logged_on()
        headers = {
            'Authorization': 'Bearer {}'.format(self.access_token),
            'content-type': 'application/x-www-form-urlencoded'
        }
        r = requests.post(api, data=payload, headers=headers)
        return r.json()


if __name__ == '__main__':
    from .settings import IAM_API_BASE_URL
    import os
    username = os.environ.get('CACHE_IAM_USER')
    password = os.environ.get('CACHE_IAM_PASSWORD')
    assert username != None, 'CACHE_IAM_USER == None'
    assert password != None, 'CACHE_IAM_PASSWORD == None'

    iam_client = IAMClient(username, password)

    api = os.path.join(IAM_API_BASE_URL, 'auth/permissions/enterprises/34/')
    r = iam_client.get(api)
    print(r)
