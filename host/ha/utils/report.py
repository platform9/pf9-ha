# Copyright 2016 Platform9 Systems Inc.
# All Rights Reserved

from ha.utils import log as logging
from oslo_config import cfg

import json
import requests

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

class MasakariReporter:

    def __init__(self):
        self.masakari_url = '/'.join([DU_URL, 'masakari'])
        self.keystone_token_url = '/'.join([DU_URL, 'keystone', 'v2.0', 'tokens'])
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
        resp = requests.post(self.keystone_token_url, data=data, headers=headers, verify=self.insecure)
        if resp.status_code != requests.codes.ok:
            return False
        return resp.json()['access']['token']['id']

    def _refresh_token(self):
        self.token = self._get_token()

    def report_status(self, data):
        headers = {
            "Content-Type": "application/json",
            "X-Auth-Token": self.token
        }
        payload = json.dumps(data)
        try:
            resp = requests.post(self.masakari_url, data=payload, headers=headers,
                                 verify=CONF.keystone_authtoken.insecure)
            if resp.status_code != requests.codes.ok:
                LOG.error('Masakari returned %s', resp.status_code)
                return False
            else:
                LOG.info('Status reported successfully to masakari')
                return True

        except:
            LOG.error('Status report failed', exc_info=True)
            return False
