# Copyright 2017 Platform9 Systems Inc.
# All Rights Reserved

import json
import time

from ha.utils import log as logging
from oslo_config import cfg

import requests

LOG = logging.getLogger(__name__)
CONF = cfg.CONF

DU_URL = "http://localhost:8158"

keystone_auth_grp = cfg.OptGroup(
    'keystone_authtoken',
    title='Options related to authentication with the Platform9 DU')
keystone_opts = [
    cfg.BoolOpt('insecure', default=False,
                help='Whether to use SSL when connecting to DU'),
    cfg.StrOpt('admin_user', help='User to be used with Masakari requests'),
    cfg.StrOpt('admin_password', help='Password for masakari user'),
    cfg.StrOpt('admin_tenant_name',
               help='Tenant associated with masakari user')
]

CONF.register_group(keystone_auth_grp)
CONF.register_opts(keystone_opts, keystone_auth_grp)


class Reporter(object):

    def __init__(self):
        self.keystone_token_url = '/'.join([DU_URL, 'keystone', 'v2.0',
                                            'tokens'])
        self.insecure = CONF.keystone_authtoken.insecure
        self.token = self._get_token()

    def _get_token(self):
        headers = {'Content-Type': 'application/json'}
        data = {
            'auth': {
                "tenantName": CONF.keystone_authtoken.admin_tenant_name,
                "passwordCredentials": {
                    "username": CONF.keystone_authtoken.admin_user,
                    "password": CONF.keystone_authtoken.admin_password
                }
            }
        }
        data = json.dumps(data)
        try:
            resp = requests.post(self.keystone_token_url, data=data,
                                 headers=headers, verify=self.insecure)
            if resp.status_code != requests.codes.ok:
                return False
            return resp.json()['access']['token']
        except Exception as e:
            LOG.warn('failed to request token, error : %s', str(e))

    def _need_refresh(self):
        """Return True if token should be refreshed."""
        str_exp_time = self.token['expires']
        token_time = time.strptime(str_exp_time, '%Y-%m-%dT%H:%M:%SZ')
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
        self.hamgr_url = '/'.join([DU_URL, 'hamgr', 'v1', 'ha'])
        super(HaManagerReporter, self).__init__()

    def report_status(self, data):
        if self._need_refresh():
            LOG.debug("Fetching new token as the old token has almost expired")
            self._refresh_token()
        headers = {
            "Content-Type": "application/json",
            "X-Auth-Token": self.token['id']
        }
        if data['eventType'] == 1:
            event = 'host-up'
        elif data['eventType'] == 2:
            event = 'host-down'
        payload = json.dumps({'event': event, 'event_details': data})
        try:
            host_url = '/'.join([self.hamgr_url, data['hostname']])
            LOG.debug('report to ha mgr : %s', str(payload))
            resp = requests.post(host_url, data=payload, headers=headers,
                                 verify=CONF.keystone_authtoken.insecure)
            if resp.status_code != requests.codes.ok:
                LOG.error('report to HA manager failed, returned %d', resp.status_code)
                return False
            else:
                LOG.info('Status reported successfully to HA manager')
                return resp.json().get('success', False)
        except Exception:
            LOG.error('Status report failed', exc_info=True)
            return False
