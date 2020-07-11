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
import sys

g_consul_member_ids = [
    str(uuid.uuid4()),
    str(uuid.uuid4()),
    str(uuid.uuid4()),
    str(uuid.uuid4())
]

g_host_ids = []
g_consul_cache = []
g_host_status = dict()
g_host_id_to_ip_map = dict()
g_base_ip_format = '1.0.0.%s'
g_consul_port = '8301'
g_kv_key_format = '%s:%s' % (g_base_ip_format, g_consul_port)
g_report_interval_seconds = 1
g_host_id = None
g_join_ips = ''
g_cluster_details = []
g_excluded_host_ids = []

def g_init():
    global g_host_ids
    global g_consul_cache
    global g_host_status
    global g_host_id_to_ip_map
    global g_base_ip_format
    global g_consul_port
    global g_kv_key_format
    global g_report_interval_seconds
    global g_host_id
    global g_join_ips
    global g_cluster_details
    global g_excluded_host_ids

    g_join_ips = ''
    g_host_ids = []
    g_consul_cache = []
    g_host_status.clear()
    g_host_id_to_ip_map.clear()
    g_excluded_host_ids = []

    for i in range(1, 1 + len(g_consul_member_ids)):
        xid = g_consul_member_ids[i - 1]
        g_host_ids.append(xid)
        # initially all hosts are up
        g_host_status[xid] = 1
        kv_key = g_kv_key_format % str(i - 1)
        g_host_id_to_ip_map[xid] = kv_key
        # simulate the consul cluster status
        g_consul_cache.append({
            'Key': kv_key,
            'Value': xid.encode()
        })
        xip = g_base_ip_format % str(i-1)
        g_join_ips = g_join_ips + ' ' + xip
        g_cluster_details.append({'name':str(xid), 'addr': xip})
    g_join_ips = g_join_ips.strip(' ')
    g_host_id = g_host_ids[0]


def g_logger(*args, **kwargs):
    if len(args) > 0:
        msg = args[0] % (args[1:])
        print(msg)


def g_consul_kv_get(key, *args, **kwargs):
    if key == '':
        if kwargs is not None and kwargs['recurse']:
            # return all
            return ('', g_consul_cache)
        else:
            raise Exception('key can not be empty')
    else:
        # search in the cache list for the key
        for kv in g_consul_cache:
            if kv['Key'] == key:
                return (key, kv)
        return (None, None)


def g_consul_kv_put(key, *args, **kwargs):
    existing = None
    for kv in g_consul_cache:
        if kv['Key'] == key:
            existing = kv
            break
    if existing is not None:
        g_consul_cache.remove(existing)

    g_consul_cache.append({"Key": key, "Value": args[0].encode()})


def g_consul_kv_delete(key, *args, **kwargs):
    existing = None
    _key = None
    _val = None
    for kv in g_consul_cache:
        if kv['Key'] == key:
            existing = kv
            _key = key
            _val = kv['Value'].decode()
            break
    if existing is not None:
        ids = set(g_host_ids)
        if key in ids:
            g_logger('deleting from cache , key : %s , val %s', str(_key), str(_val))
        g_consul_cache.remove(existing)


def g_consul_agent_members(*args, **kwargs):
    members = []
    for xid in g_host_ids:
        if g_excluded_host_ids and xid in g_excluded_host_ids:
            continue
        status = g_host_status[xid]
        addr = g_host_id_to_ip_map[xid]
        members.append({
            "Status": status,
            "Name": xid,
            "Port": int(g_consul_port),
            "Addr": addr.split(":")[0],
            "Tags": {}
        })
    g_logger('consul members %s', str(members))
    return members


def g_consul_status_leader(*args, **kwargs):
    # use first node as leader
    return g_consul_cache[0]["Key"]


def g_consul_status_peers(*args, **kwargs):
    # the rest hosts are peers
    peers = []
    if len(g_consul_cache) <= 0:
        return peers

    for i in range(1, 5):
        if i == 1:
            continue
        if i > len(g_consul_cache):
            continue
        peers.append(g_consul_cache[i - 1]["Key"])
    return peers


def g_consul_add_excluded_host(host_id):
    g_excluded_host_ids.append(host_id)


def g_consul_clean_excluded_host(host_id):
    if host_id in g_excluded_host_ids:
        g_excluded_host_ids.remove(host_id)

class ConsulStatusTest(unittest.TestCase):
    def __init__(self, *args, **kwargs):
        super(ConsulStatusTest, self).__init__(*args, **kwargs)
        g_init()

    @mock.patch('logging.getLogger')
    @mock.patch('logging.config.dictConfig')
    @mock.patch('oslo_config.cfg.CONF')
    def setUp(self,
              mock_conf,
              mock_config_dictConfig,
              mock_logger
              ):
        mock_conf.consul.last_update_file = './mocked-last-update-file'
        mock_conf.consul.key_reap_interval = 1
        mock_conf.log.file = './mocked-log-file'
        mock_conf.consul.report_interval = g_report_interval_seconds
        mock_conf.node.ip_address = g_base_ip_format % str(0)

        logger_instance = mock_logger.return_value
        logger_instance.debug = g_logger
        logger_instance.info = g_logger
        logger_instance.warn = g_logger
        logger_instance.exception = g_logger

        # -------------------------------------------------------------
        # here we mock the consul.Consul module and class, and all
        # the common used filed and their methods
        # like Consul.kv, Consul.agent, Consul.agent.members
        # Consul.status, Consul.status.leader
        # Consul.status.peers
        #
        # because python module will be singleton , and the side_effect
        # of a mocked object only support static method (not method from
        # a class, that will be different one in different instances)
        # so we have to declare all buffers we used to simulate Consul
        # as global variable, that will make sure to be just single copy
        #
        # this is very important (i have spent several days to figure out)
        # --------------------------------------------------------------
        self.consul_instance = mock.Mock()
        kv_get = mock.Mock(side_effect=g_consul_kv_get)
        self.consul_instance.kv.get = kv_get

        kv_put = mock.Mock(side_effect=g_consul_kv_put)
        self.consul_instance.kv.put = kv_put

        kv_delete = mock.Mock(side_effect=g_consul_kv_delete)
        self.consul_instance.kv.delete = kv_delete

        agent_members = mock.Mock(side_effect=g_consul_agent_members)
        self.consul_instance.agent.members = agent_members

        status_leader = mock.Mock(side_effect=g_consul_status_leader)
        self.consul_instance.status.leader = status_leader

        status_peers = mock.Mock(side_effect=g_consul_status_peers)
        self.consul_instance.status.peers = status_peers

        consul = mock.MagicMock()
        consul.Consul = mock.Mock(return_value=self.consul_instance)
        sys.modules['consul'] = consul
        # -------------------------------------------------------------

        #
        # import the testing target need to be after the 'consul' is
        # mocked, otherwise we will have no way to modify the consul
        # member status to simulate it would be in reality
        #
        import importlib
        my_module = importlib.import_module('ha.utils.consul_helper')
        my_class = getattr(my_module, 'consul_status')
        self._consul_helper = my_class(g_host_id, g_join_ips.strip(' ').split(' '), g_cluster_details)
        self._consul_helper.leader = True

    def tearDown(self):
        #
        # don't reset the global buffers we used
        # for each test. as in reality, consul will
        # be just one instance with status kept
        #
        pass

    def _assert_initial_consul_status(self):
        # restore the 'consul members' to normal
        agent_members = mock.Mock(side_effect=g_consul_agent_members)
        self.consul_instance.agent.members = agent_members

        # simulate the consul leader role
        self._consul_helper.leader = True
        # clean existing reports by reset host to up
        for k in g_host_status.keys():
            g_host_status[k] = 1
            self._consul_helper.refresh_cache_from_consul()
            time.sleep(g_report_interval_seconds + 6)
            status = self._consul_helper.get_cluster_status()
            if status:
                self._consul_helper.update_reported_status(status)
            # self.assertIsNone(status)

    def _assert_host_status_change(self, host_index, host_status):
        xid = g_consul_cache[host_index]["Value"].decode()
        g_host_status[xid] = host_status
        self._consul_helper.refresh_cache_from_consul()
        # check host down event is reported
        time.sleep(g_report_interval_seconds + 60)
        # self._consul_helper.refresh_cache_from_consul()
        # time.sleep(g_report_interval_seconds + 60)
        g_logger('checking report for host %s with status %s', xid, host_status)
        status = self._consul_helper.get_cluster_status()
        msg = 'status %s for host %s is reported ? %s' % (str(host_status), str(xid), str(status))
        g_logger('%s', msg)
        self.assertIsNotNone(status, msg)
        msg = 'event in reported status %s, the status was set as %s' % (
                 str(status.event['eventType']), str(1 if host_status==1 else 2))
        g_logger(msg)
        self.assertEqual(status.event['eventType'], 1 if host_status==1 else 2, msg)
        g_logger('marking status as reported')
        self._consul_helper.update_reported_status(status)
        g_logger('checking reported status get removed')
        status = self._consul_helper.get_cluster_status()
        msg = 'reported status has not been removed ? : %s' % (str(status))
        g_logger('%s', msg)
        self.assertIsNone(status, msg)

    def _assert_report_status(self, host_id, report_status):
        report = g_consul_kv_get(host_id)
        self.assertIsNotNone(report)
        self.assertIsNotNone(report[1])
        body = json.loads(report[1]['Value'].decode())
        consul_info = body['consul']
        self.assertIsNotNone(consul_info)
        event_info = body['event']
        self.assertIsNotNone(event_info)
        self.assertTrue(event_info['reported'] is report_status)

    def _assert_host_down_then_up(self, host_index):
        # make host down
        host_id = g_consul_cache[host_index]["Value"].decode()
        g_logger('make host %s down and verify event is reported', host_id)
        self._assert_host_status_change(host_index, 2)
        self._assert_report_status(host_id, True)

        # make host up
        host_id = g_consul_cache[host_index]["Value"].decode()
        g_logger('make host %s up and check status', str(host_id))
        self._assert_host_status_change(host_index, 1)

        # after the host recovered from down status, its down status report
        # should be removed from consul
        g_logger('verify no kv store for host %s', host_id)
        report = g_consul_kv_get(host_id)
        self.assertIsNone(report[1])

    def test_initial_consul_cluster_runs(self):
        self._assert_initial_consul_status()

    def test_one_non_leader_host_down(self):
        # setup initial status
        self._assert_initial_consul_status()
        # set second host (non leader) as down
        host_index = 1
        host_id = g_consul_cache[host_index]["Value"].decode()
        self._assert_host_status_change(host_index, 3)
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
        host_id = g_consul_cache[host_index]["Value"].decode()
        self._assert_host_status_change(host_index, 5)
        self._assert_report_status(host_id, True)

        # make third host down
        host_index = 2
        host_id = g_consul_cache[host_index]["Value"].decode()
        self._assert_host_status_change(host_index, 7)
        self._assert_report_status(host_id, True)

    def test_non_leader_host_down_then_up(self):
        # setup initial status
        self._assert_initial_consul_status()

        # make second host down then up
        host_index = 1
        g_logger('verify host down then up for host : %s',g_consul_cache[host_index]["Value"].decode())
        self._assert_host_down_then_up(host_index)

    def test_non_leader_host_repeatedly_down_and_up(self):
        # setup initial status
        self._assert_initial_consul_status()

        host_index = 1
        while host_index < 4:
            # simulate host down then up, for all hosts, one by one
            g_logger('verify host %s down then up ', g_consul_cache[host_index]["Value"].decode())
            self._assert_host_down_then_up(host_index)

            host_index = host_index + 1
            time.sleep(.100)

    def test_consul_kv_store_cleanup(self):
        # setup initial status
        self._assert_initial_consul_status()
        # make host down so there will be report in consul
        host_index = 1
        host_id = g_consul_cache[host_index]["Value"].decode()
        g_logger('set host status to 9 and verify host-down is reported')
        self._assert_host_status_change(host_index, 9)
        # check the reported report will be cleaned out
        time.sleep(g_report_interval_seconds + 60)
        g_logger('verify old report will be cleaned')
        self._consul_helper.cleanup_consul_kv_store()
        report = g_consul_kv_get(host_id)
        self.assertIsNone(report[1])

    def test_event_still_reported_when_consul_reaped_host(self):
        # 'consul member' may not return 'left' or 'failed' node
        # this caused host event not reported, this test will
        # validate the host-down event still reported in such case

        # first set initial status
        self._assert_initial_consul_status()
        # make one host down
        host_index = 1
        host_id = g_consul_cache[host_index]["Value"].decode()
        g_host_status[host_index] = 9
        # try to remove the host from 'consul members' result
        g_consul_add_excluded_host(host_id)
        # update the cache to see whether still report the event
        self._consul_helper.refresh_cache_from_consul()
        # wait for event out of threshold window
        time.sleep(g_report_interval_seconds + 60)
        # report the event
        status = self._consul_helper.get_cluster_status()
        self.assertIsNotNone(status)
        # check event is in kv store
        report = g_consul_kv_get(host_id)
        self.assertIsNotNone(report[1])
        # now set it as reported
        self._consul_helper.update_reported_status(status)
        status = self._consul_helper.get_cluster_status()
        self.assertIsNone(status)
        # clean the excluded host
        g_consul_clean_excluded_host(host_id)
