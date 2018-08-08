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

from ConfigParser import ConfigParser
import unittest

from hamgr.db import api as db_api
from hamgr import exceptions
from hamgr.providers.nova import get_provider
from hamgr import states

import mock
from hamgr.notification import publish

class FakeNovaClient(object):
    class Hypervisors(object):
        def __init__(self):
            self.hosts = []
            for i in range(4):
                m = mock.Mock()
                m.service.host = str(i)
                m.host_ip = '192.178.1.%d' % i
                self.hosts.append(m)

        def list(self):
            return self.hosts

    hypervisors = Hypervisors()

    class Aggregate(object):
        aggr = mock.Mock()

        def get(self, *args, **kwargs):
            return self.aggr

    Aggregate.aggr.hosts = [h.service.host
                            for h in hypervisors.list()]
    aggregates = Aggregate()


class NovaProviderTest(unittest.TestCase):

    def setUp(self):
        config = ConfigParser()
        config.add_section('database')
        config.set('database', 'sqlconnectURI', 'sqlite://')
        config.set('database', 'sqlite_synchronous', False)

        config.add_section('keystone_middleware')
        config.set('keystone_middleware', 'admin_user', 'fake')
        config.set('keystone_middleware', 'admin_password', 'fake')
        config.set('keystone_middleware', 'auth_uri', 'fake')
        config.set('keystone_middleware', 'admin_tenant_name', 'fake')

        config.add_section('nova')
        config.set('nova', 'region', 'fake')

        config.set('DEFAULT', 'event_report_threshold_seconds', '30')

        self._provider = get_provider(config)

        db_api.Base.metadata.create_all(db_api._engine)

        def get_client():
            return FakeNovaClient()

        def get_ips(*args, **kwargs):
            ip_lookup = {"0": "fake_ip_0", "1": "fake_ip_1",
                         "2": "fake_ip_2", "3": "fake_ip_3"}
            cluster_ip_lookup = {"0": "fake_cluster_ip_0",
                                 "1": "fake_cluster_ip_1",
                                 "2": "fake_cluster_ip_2",
                                 "3": "fake_cluster_ip_3"}
            return ip_lookup, cluster_ip_lookup

        self._provider._get_client = get_client
        self._provider._get_ips = get_ips

    def tearDown(self):
        db_api.Base.metadata.drop_all(db_api._engine)

    def _enable_aggregate(self, mock_del, mock_put, mock_get, mock_post,
                          mock_token, aggregate_id):
        host = {'id': "fake_id",
                'roles': ['fake_role_1', 'fake_role_2', 'fake_role_2'],
                'info': {'responding': True},
                'role_status': 'ok'}
        mock_resp = mock.Mock()
        mock_resp.status_code = 200
        mock_resp.raise_for_status = lambda *args: None
        mock_resp.json = lambda *args: dict(
            host, segment=dict(name='fake1', uuid='fake'), role_status='ok')
        mock_post.return_value = mock_resp
        mock_get.return_value = mock_resp
        mock_put.return_value = mock_resp
        mock_del.return_value = mock_resp
        mock_token.return_value = dict(id='1234sbds')
        self._provider.put(aggregate_id, 'enable')

    def _repeat_it(self):
        self._provider.put("fake1", "enable")
        self._provider.ha_enable_disable_request_processing();

    @mock.patch('hamgr.common.utils.get_token')
    @mock.patch('requests.post')
    @mock.patch('requests.get')
    @mock.patch('requests.put')
    @mock.patch('requests.delete')
    def test_enable(self, mock_del, mock_put, mock_get, mock_post, mock_token):
        self._enable_aggregate(mock_del, mock_put, mock_get, mock_post,
                               mock_token, aggregate_id='fake')
        # before request is processed
        aggregates = self._provider.get('fake')
        self.assertIsNotNone(aggregates, 'no aggregates found')
        self.assertTrue(len(aggregates) == 1, 'at least there is one aggregate')
        aggregate = aggregates[0]
        self.assertTrue(aggregate['enabled']== False)
        # after request is processed
        self._provider.ha_enable_disable_request_processing()
        aggregates = self._provider.get('fake')
        self.assertIsNotNone(aggregates)
        aggregate = aggregates[0]
        self.assertTrue(aggregate['enabled'] == True)


    @mock.patch('hamgr.common.utils.get_token')
    @mock.patch('requests.post')
    @mock.patch('requests.get')
    @mock.patch('requests.put')
    @mock.patch('requests.delete')
    def test_enable_with_hosts_in_another_cluster(
            self, mock_del, mock_put, mock_get, mock_post, mock_token):
        mock_resp = mock.Mock()
        mock_resp.status_code = 200
        mock_resp.raise_for_status = lambda *args: None
        mock_resp.json = lambda *args: dict(role_status='ok')
        self._enable_aggregate(mock_del, mock_put, mock_get, mock_post,
                               mock_token, aggregate_id="fake")
        self._provider.ha_enable_disable_request_processing();
        # Create 2nd aggregate with hosts from first cluster
        self.assertRaises(exceptions.HostPartOfCluster, self._repeat_it)

    @mock.patch('hamgr.common.utils.get_token')
    @mock.patch('requests.get')
    @mock.patch('requests.delete')
    def test_disable(self, mock_del, mock_get, mock_token):

        mock_resp = mock.Mock()
        mock_resp.status_code = 200
        mock_resp.raise_for_status = lambda *args: None

        def handle_get(url, headers=None):
            if 'keystone' in url:
                mock_resp.json = lambda *args: dict(id='ejkfskds')
            elif 'hosts' in url:
                hosts = [
                    {'name': str(i),
                     'failover_segment_id': 'fake_failover_segment_id',
                     'uuid': 'fake_uuid'}
                    for i in range(4)]
                mock_resp.json = lambda *args: dict(hosts=hosts)
            elif 'masakari' in url:
                mock_resp.json = \
                    lambda *args: dict(segments=[dict(name='fake',
                                                      uuid='fake')])
            return mock_resp

        mock_get.side_effect = handle_get
        mock_del.return_value = mock_resp
        mock_token.return_value = dict(id='12ewef')
        db_api.create_cluster_if_needed('fake', states.TASK_COMPLETED)
        db_api.update_cluster('fake', True)
        self._provider.put('fake', 'disable')
