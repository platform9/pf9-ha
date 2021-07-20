# Copyright (c) 2019 Platform9 Systems Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import logging
import time
# import _striptime to avoid deadlock when call time.striptime
# in multiple thread environment
import _strptime
from keystoneclient.v3 import client as v3client
from keystoneclient.v3.tokens import TokenManager
from keystoneauth1.identity import v3
from keystoneauth1 import session
from shared.constants import LOGGER_PREFIX

LOG = logging.getLogger(LOGGER_PREFIX + __name__)


def _get_auth_token(auth_url, tenant, user, password, region_name):
    auth = v3.Password(auth_url=auth_url,
                       username=user,
                       password=password,
                       project_name=tenant,
                       project_domain_name='default',
                       user_domain_name='default'
                       )
    sess = session.Session(auth=auth)
    raw = sess.get_token()
    keystone = v3client.Client(session=sess, region_name=region_name)
    mgr = TokenManager(keystone)
    data = mgr.get_token_data(raw)
    token = data['token']
    token['id'] = raw
    return token


def _need_refresh(token):
    """Return True if token should be refreshed."""

    # ToDo(pratik): check if token is valid by querying keystone

    str_exp_time = token['expires_at']
    token_time = time.strptime(str_exp_time, '%Y-%m-%dT%H:%M:%S.%fZ')
    current_time = time.gmtime()

    # If the Token's expiry is in 300 secs or less, it needs refresh
    return True if time.mktime(token_time) - time.mktime(current_time) < 300.0\
        else False


def get_token(auth_url, tenant, user, password, old_token, region_name):

    token = old_token

    if not old_token or _need_refresh(old_token):
        LOG.debug('Refreshing token...')
        token = _get_auth_token(auth_url, tenant, user, password, region_name)

    return token
