# Copyright 2016 Platform9 Systems Inc.
# All Rights Reserved

from ha.utils import log as logging
from oslo_config import cfg

import json
import requests
import time

LOG = logging.getLogger(__name__)
CONF = cfg.CONF

DU_URL = "http://localhost:8158"

keystone_auth_grp = cfg.OptGroup('keystone_authtoken',
            title='Options related to authentication with the Platform9 DU')
keystone_opts = [
    cfg.BoolOpt('insecure', default=False,
                help='Whether to use SSL when connecting to DU'),
    cfg.StrOpt('admin_user', help='User to be used with Masakari requests'),
    cfg.StrOpt('admin_password', help='Password for masakari user'),
    cfg.StrOpt('admin_tenant_name', help='Tenant associated with masakari user')
]

CONF.register_group(keystone_auth_grp)
CONF.register_opts(keystone_opts, keystone_auth_grp)


class Reporter(object):

    def __init__(self):
        self.keystone_token_url = '/'.join([DU_URL, 'keystone', 'v2.0', 'tokens'])
        self.insecure = CONF.keystone_authtoken.insecure
        self.token = self._get_token()
        LOG.info("Token initialized to {}".format(self.token))

    def _get_token(self):
        LOG.info("_get_token in Reported called")
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
        resp = requests.post(self.keystone_token_url, data=data,
                             headers=headers, verify=self.insecure)
        if resp.status_code != requests.codes.ok:
            LOG.info(" HTTP {} from keystone".format(resp.status_code))
            return False
        LOG.info("All OK from keystone")
        return resp.json()['access']['token']

    def _need_refresh(self):
        """
        Return True if token should be refreshed.
        """
        LOG.info("Checking if a token needs refresh")
        str_exp_time = self.token['expires']
        token_time = time.strptime(str_exp_time, '%Y-%m-%dT%H:%M:%SZ')
        current_time = time.gmtime()
        LOG.info("Token expiry:{}".format(token_time))
        LOG.info("Current time:{}".format(current_time))
        LOG.info("Token time diff: {}".format(time.mktime(token_time) - time.mktime(current_time)))
        # If the Token's expiry is in 300 secs or less, it needs refresh
        return True if time.mktime(token_time) - time.mktime(current_time) < 300.0 \
            else False

    def _refresh_token(self):
        self.token = self._get_token()
        LOG.info("Token refreshed to {}".format(self.token))

    def report_status(self, data):
        raise NotImplementedError()

class HaManagerReporter(Reporter):

    def __init__(self):
        self.hamgr_url = '/'.join([DU_URL, 'hamgr', 'v1', 'ha'])
        super(HaManagerReporter, self).__init__()

    def report_status(self, data):
        LOG.info("In report_status::")
        if self._need_refresh():
            LOG.info("Refresh needed")
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
            resp = requests.post(host_url, data=payload, headers=headers,
                                 verify=CONF.keystone_authtoken.insecure)
            if resp.status_code != requests.codes.ok:
                LOG.error('HA manager returned %d', resp.status_code)
                return False
            else:
                LOG.info('Status reported successfully to HA manager')
                return resp.json().get('success', False)
        except:
            LOG.error('Status report failed', exc_info=True)
            return False
