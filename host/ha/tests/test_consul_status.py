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

import unittest
import uuid
import mock
import time
import json


class ConsulStatusTest(unittest.TestCase):

    def __init__(self, *args, **kwargs):
        super(ConsulStatusTest, self).__init__(*args, **kwargs)
        self._host_ids = []
        self._consul_cache = []
        self._host_status = dict()
        self._host_id_to_ip_map = dict()
        self._base_ip_format = '1.0.0.%s'
        self._consul_port = '8301'
        self._kv_key_format = '%s:%s' % (
            self._base_ip_format, self._consul_port)
        for i in range(1, 5):
            id = str(uuid.uuid4())
            self._host_ids.append(id)
            # initially all hosts are up
            self._host_status[id] = 1
            kv_key = self._kv_key_format % str(i - 1)
            self._host_id_to_ip_map[id] = kv_key
            # simulate the consul cluster status
            self._consul_cache.append({
                'Key': kv_key,
                'Value': id
            })
        self._host_id = self._host_ids[0]
        self._report_interval_seconds = 1

    @mock.patch('logging.getLogger')
    @mock.patch('logging.config.dictConfig')
    @mock.patch('consul.Consul')
    @mock.patch('oslo_config.cfg.CONF')
    def setUp(self,
              mock_conf,
              mock_consul,
              mock_config_dictConfig,
              mock_logger
              ):
        mock_conf.consul.last_update_file = './mocked-last-update-file'
        mock_conf.consul.key_reap_interval = 1
        mock_conf.log.file = './mocked-log-file'
        mock_conf.consul.report_interval = self._report_interval_seconds
        mock_conf.node.ip_address = self._base_ip_format % str(0)

        consul_instance = mock_consul.return_value
        consul_instance.kv.get.side_effect = self._consul_kv_get
        consul_instance.kv.put.side_effect = self._consul_kv_put
        consul_instance.kv.delete.side_effect = self._consul_kv_delete
        consul_instance.agent.members.side_effect = self._consul_agent_members
        consul_instance.status.leader.side_effect = self._consul_status_leader
        consul_instance.status.peers.side_effect = self._consul_status_peers

        logger_instance = mock_logger.return_value
        logger_instance.debug.side_effect = self._logging_to_console
        logger_instance.info.side_effect = self._logging_to_console
        logger_instance.warn.side_effect = self._logging_to_console
        logger_instance.exception.side_effect = self._logging_to_console

        # delay import after all mocks are setup
        from ha.utils import consul_helper
        self._consul_helper = consul_helper.consul_status(self._host_id)

    def tearDown(self):
        pass

    def _logging_to_console(self, *args, **kwargs):
        if len(args) > 0:
            msg = args[0] % (args[1:])
            print msg

    def _consul_kv_get(self, key, *args, **kwargs):
        if key == '':
            if kwargs is not None and kwargs['recurse']:
                # return all
                return ('', self._consul_cache)
            else:
                raise Exception('key can not be empty')
        else:
            # search in the cache list for the key
            for kv in self._consul_cache:
                if kv['Key'] == key:
                    return (key, kv)
            return (None, None)

    def _consul_kv_put(self, key, *args, **kwargs):
        existing = None
        for kv in self._consul_cache:
            if kv['Key'] == key:
                existing = kv
                break
        if existing is not None:
            self._consul_cache.remove(existing)

        self._consul_cache.append({"Key": key, "Value": args[0]})

    def _consul_kv_delete(self, key, *args, **kwargs):
        existing = None
        for kv in self._consul_cache:
            if kv['Key'] == key:
                existing = kv
                break
        if existing is not None:
            self._consul_cache.remove(existing)

    def _consul_agent_members(self, *args, **kwargs):
        members = []
        for id in self._host_ids:
            status = self._host_status[id]
            addr = self._host_id_to_ip_map[id]
            members.append({
                "Status": status,
                "Name": id,
                "Port": int(self._consul_port),
                "Addr": addr.split(":")[0]
            })
        return members

    def _consul_status_leader(self, *args, **kwargs):
        # use first node as leader
        return self._consul_cache[0]["Key"]

    def _consul_status_peers(self, *args, **kwargs):
        # the rest hosts are peers
        peers = []
        for i in range(1, 5):
            if i == 1:
                continue
            peers.append(self._consul_cache[i - 1]["Key"])
        return peers

    def _assert_initial_consul_status(self):
        # simulate the consul leader role
        self._consul_helper.leader = True
        # get initial consul status before any host goes down
        status = self._consul_helper.get_cluster_status()
        # initially there are no status to report
        self.assertIsNone(status)

    def _assert_host_status_change(self, host_index, host_status):
        id = self._consul_cache[host_index]["Value"]
        self._host_status[id] = host_status
        self._consul_helper.refresh_cache_from_consul()
        # check host down event is reported
        time.sleep(self._report_interval_seconds + 6)
        status = self._consul_helper.get_cluster_status()
        self.assertIsNotNone(status)
        self._consul_helper.update_reported_status(status)
        status = self._consul_helper.get_cluster_status()
        self.assertIsNone(status)

    def _assert_report_status(self, host_id, report_status):
        report = self._consul_kv_get(host_id)
        self.assertIsNotNone(report)
        self.assertIsNotNone(report[1])
        body = json.loads(report[1]['Value'])
        consul_info = body['consul']
        self.assertIsNotNone(consul_info)
        event_info = body['event']
        self.assertIsNotNone(event_info)
        self.assertTrue(event_info['reported'] is report_status)

    def _assert_host_down_then_up(self, host_index):
        # make host down
        host_id = self._consul_cache[host_index]["Value"]
        self._assert_host_status_change(host_index, 2)
        self._assert_report_status(host_id, True)

        # make host up
        host_id = self._consul_cache[host_index]["Value"]
        self._assert_host_status_change(host_index, 1)

        # after the host recovered from down status, its down status report
        # should be removed from consul
        report = self._consul_kv_get(host_id)
        self.assertIsNone(report[1])

    def test_initial_consul_cluster_runs(self):
        self._assert_initial_consul_status()

    def test_one_non_leader_host_down(self):
        # setup initial status
        self._assert_initial_consul_status()
        # set second host (non leader) as down
        host_index = 1
        host_id = self._consul_cache[host_index]["Value"]
        self._assert_host_status_change(host_index, 2)
        self._assert_report_status(host_id, True)

    def test_leader_host_down(self):
        # since we don't know whether consul cluster will be
        # still working as the leader is down, the cluster itself
        # should re-elect a new leader, then the scenario will be
        # the same as non leader host down
        pass

    def test_two_or_more_non_leader_hosts_down_continuously(self):
        # setup initial status
        self._assert_initial_consul_status()

        # make second host down
        host_index = 1
        host_id = self._consul_cache[host_index]["Value"]
        self._assert_host_status_change(host_index, 2)
        self._assert_report_status(host_id, True)

        # make third host down
        host_index = 2
        host_id = self._consul_cache[host_index]["Value"]
        self._assert_host_status_change(host_index, 2)
        self._assert_report_status(host_id, True)

    def test_non_leader_host_down_then_up(self):
        # setup initial status
        self._assert_initial_consul_status()

        # make second host down then up
        host_index = 1
        self._assert_host_down_then_up(host_index)

    def test_non_leader_host_repeatedly_down_and_up(self):
        # setup initial status
        self._assert_initial_consul_status()

        host_index = 1
        while host_index < 4:
            # simulate host down then up, for all hosts, one by one
            self._assert_host_down_then_up(host_index)

            host_index = host_index + 1
            time.sleep(.100)

    def test_consul_kv_store_cleanup(self):
        # setup initial status
        self._assert_initial_consul_status()
        # make host down so there will be report in consul
        host_index = 1
        host_id = self._consul_cache[host_index]["Value"]
        self._assert_host_status_change(host_index, 2)
        # check the reported report will be cleaned out
        time.sleep(self._report_interval_seconds + 60)
        self._consul_helper.cleanup_consul_kv_store()
        report = self._consul_kv_get(host_id)
        self.assertIsNone(report[1])
