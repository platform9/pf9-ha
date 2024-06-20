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

import json
import time

import logging
from oslo_config import cfg
from keystoneclient.v3 import client as v3client
from keystoneclient.v3.tokens import TokenManager
from keystoneauth1.identity import v3
from keystoneauth1 import session
import requests
from shared.constants import LOGGER_PREFIX

LOG = logging.getLogger(LOGGER_PREFIX + __name__)
CONF = cfg.CONF

DU_URL = "http://localhost:8158"
HAMGR_URL = "http://localhost:9083"

keystone_auth_grp = cfg.OptGroup(
    'keystone_authtoken',
    title='Options related to authentication with the Platform9 DU')
keystone_opts = [
    cfg.BoolOpt('insecure', default=False,
                help='Whether to use SSL when connecting to DU'),
    cfg.StrOpt('auth_url', help='The uri for keystone authentication'),
    cfg.StrOpt('username', help='User to be used with Masakari requests'),
    cfg.StrOpt('password', help='Password for masakari user'),
    cfg.StrOpt('project_name', help='Tenant associated with masakari user')
]

CONF.register_group(keystone_auth_grp)
CONF.register_opts(keystone_opts, keystone_auth_grp)


class Reporter(object):

    def __init__(self):
        # unless we change the url from ansible here
        # 'ansible-stack/roles/hamgr-configure/tasks/main.yml#73'
        # still use the local hardcoded base url
        # self.auth_url = CONF.keystone_authtoken.auth_url
        self.auth_url = '/'.join([DU_URL, 'keystone', 'v3'])
        self.insecure = CONF.keystone_authtoken.insecure
        self.token = self._get_token()

    def _get_token(self):

        try:
            auth_url = self.auth_url
            tenant = CONF.keystone_authtoken.project_name
            username = CONF.keystone_authtoken.username
            password = CONF.keystone_authtoken.password
            auth = v3.Password(auth_url=auth_url,
                               username=username,
                               password=password,
                               project_name=tenant,
                               project_domain_id='default',
                               user_domain_id='default'
                               )
            sess = session.Session(auth=auth)
            id = sess.get_token()
            keystone = v3client.Client(session=sess)
            mgr = TokenManager(keystone)
            data = mgr.get_token_data(id)
            token = data['token']
            token['id'] = id
            return token
        except Exception as e:
            LOG.warning('failed to request token, error : %s', str(e))

    def _need_refresh(self):
        """Return True if token should be refreshed."""
        if self.token is None or self.token.get('expires_at', None) is None:
            LOG.warning('token is null, need to refresh token : %s', str(self.token))
            return True

        str_exp_time = self.token['expires_at']
        token_time = time.strptime(str_exp_time, '%Y-%m-%dT%H:%M:%S.%fZ')
        current_time = time.gmtime()

        # If the Token's expiry is in 300 secs or less, it needs refresh
        if time.mktime(token_time) - time.mktime(current_time) < 300.0:
            return True
        return False

    def _refresh_token(self):
        self.token = self._get_token()

    def report_status(self, data):
        raise NotImplementedError()


class HaManagerReporter(Reporter):

    def __init__(self):
        self.hamgr_url = '/'.join([HAMGR_URL, 'v1', 'ha'])
        super(HaManagerReporter, self).__init__()

    def report_status(self, data):

        if data.event['eventType'] == 1:
            event = 'host-up'
        elif data.event['eventType'] == 2:
            event = 'host-down'
        payload = json.dumps({'event': event, 'event_details': data })
        try:
            if self._need_refresh():
                LOG.debug("Fetching new token as the old token has almost expired")
                self._refresh_token()
            headers = {
                "Content-Type": "application/json",
                "X-Auth-Token": self.token['id']
            }

            host_url = '/'.join([self.hamgr_url, data.event['hostName']])
            LOG.info('reporting to HA manager for host %s: %s', data.event['hostName'], str(payload))
            resp = requests.post(host_url, data=payload, headers=headers,
                                 verify=CONF.keystone_authtoken.insecure)
            if resp.status_code != requests.codes.ok:
                LOG.error('report to HA manager for host %s failed, returned %d', data.event['hostName'], resp.status_code)
                return False
            else:
                LOG.info('Status reported successfully to HA manager for host %s ', data.event['hostName'])
                return resp.json().get('success', False)
        except Exception:
            LOG.error('Status report to HA manager for host %s failed', data.event['hostName'], exc_info=True)
            return False
