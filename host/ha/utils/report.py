# Copyright 2017 Platform9 Systems Inc.
# All Rights Reserved

import json
import time

from ha.utils import log as logging
from oslo_config import cfg
from keystoneclient.v3 import client as v3client
from keystoneclient.v3.tokens import TokenManager
from keystoneauth1.identity import v3
from keystoneauth1 import session

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
    cfg.StrOpt('auth_uri', help='The uri for keystone authentication'),
    cfg.StrOpt('admin_user', help='User to be used with Masakari requests'),
    cfg.StrOpt('admin_password', help='Password for masakari user'),
    cfg.StrOpt('admin_tenant_name',
               help='Tenant associated with masakari user')
]

CONF.register_group(keystone_auth_grp)
CONF.register_opts(keystone_opts, keystone_auth_grp)


class Reporter(object):

    def __init__(self):
        # unless we change the url from ansible here
        # 'ansible-stack/roles/hamgr-configure/tasks/main.yml#73'
        # still use the local hardcoded base url
        #self.auth_uri = CONF.keystone_authtoken.auth_uri
        self.auth_uri = '/'.join([DU_URL, 'keystone', 'v3'])
        self.insecure = CONF.keystone_authtoken.insecure
        self.token = self._get_token()

    def _get_token(self):

        try:
            auth_uri = self.auth_uri
            tenant = CONF.keystone_authtoken.admin_tenant_name
            username = CONF.keystone_authtoken.admin_user
            password = CONF.keystone_authtoken.admin_password
            auth = v3.Password(auth_url=auth_uri,
                               username=username,
                               password=password,
                               project_name=tenant,
                               project_domain_name='default',
                               user_domain_name='default'
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
            LOG.warn('failed to request token, error : %s', str(e))

    def _need_refresh(self):
        """Return True if token should be refreshed."""
        if self.token is None or self.token.get('expires_at', None) is None:
            LOG.warn('token is null, need to refresh token : %s', str(self.token))
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
        self.hamgr_url = '/'.join([DU_URL, 'hamgr', 'v1', 'ha'])
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
            LOG.info('report to HA manager : %s', str(payload))
            resp = requests.post(host_url, data=payload, headers=headers,
                                 verify=CONF.keystone_authtoken.insecure)
            if resp.status_code != requests.codes.ok:
                LOG.error('report to HA manager failed, returned %d', resp.status_code)
                return False
            else:
                LOG.info('Status reported successfully to HA manager')
                return resp.json().get('success', False)
        except Exception:
            LOG.error('Status report to HA manager failed', exc_info=True)
            return False
