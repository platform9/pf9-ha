# Copyright (c) 2016 Platform9 Systems Inc.
#
#    Licensed under the Apache License, Version 2.0 (the "License"); you may
#    not use this file except in compliance with the License. You may obtain
#    a copy of the License at
#
#         http://www.apache.org/licenses/LICENSE-2.0
#
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
#    WARRANTIES OR CONDITIONS OF ANY KIND, either expressed or implied. See the
#    License for the specific language governing permissions and limitations
#    under the License.

import logging
import requests
import time
import json

LOG = logging.getLogger(__name__)

def _get_auth_token(tenant, user, password):
    data = {
        "auth": {
            "tenantName": tenant,
            "passwordCredentials": {
                "username": user,
                "password": password
            }
        }
    }

    url = 'http://localhost:8080/keystone/v2.0/tokens'

    r = requests.post(url, json.dumps(data),
                      verify=False, headers={'Content-Type': 'application/json'})

    if r.status_code != requests.codes.ok:
        raise RuntimeError('Token request returned: %d' % r.status_code)

    return r.json()['access']['token']


def _need_refresh(token):
    """
    Return True if token should be refreshed.
    """

    # TODO check if token is valid by querying keystone

    str_exp_time = token['expires']
    token_time = time.strptime(str_exp_time, '%Y-%m-%dT%H:%M:%SZ')
    current_time = time.gmtime()

    return True if time.mktime(token_time) - time.mktime(current_time) < 60 * 5\
        else False


def get_token(tenant, user, password, old_token):

    token = old_token

    if not old_token or _need_refresh(old_token):
        LOG.debug('Refreshing token...')
        token = _get_auth_token(tenant, user, password)

    return token

