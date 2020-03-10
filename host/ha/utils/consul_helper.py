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
import logging
import os
import re
from datetime import datetime
from datetime import timedelta
from uuid import uuid4

import consul
from netifaces import AF_INET
from netifaces import gateways
from netifaces import ifaddresses
from oslo_config import cfg

from shared.constants import LOGGER_PREFIX

LOG = logging.getLogger(LOGGER_PREFIX + __name__)
CONF = cfg.CONF
consul_grp = cfg.OptGroup('consul', title='Group for consul binary '
                                          'related options')
consul_opts = [
    cfg.IntOpt('report_interval', default=360,
               help='Time between cluster status change being reported to the'
                    'controller in seconds.'),
    cfg.StrOpt('last_update_file', default='/var/consul-status/last_update',
               help='Location of the status update cache file'),
    cfg.IntOpt('key_reap_interval', default=72 * 60,
               help='Minutes before stale key value entries are deleted.')
]
node_grp = cfg.OptGroup('node', title='Options related to a consul node')
node_opts = [
    cfg.StrOpt('ip_address', help='IP Address that provides connectivity to'
                                  ' other hosts', default=""),
    cfg.StrOpt('cluster_ip', help="IP address to set as bind address",
               default="")
]
CONF.register_group(consul_grp)
CONF.register_opts(consul_opts, consul_grp)
CONF.register_group(node_grp)
CONF.register_opts(node_opts, node_grp)
LAST_STATUS_UPDATE_FILE = CONF.consul.last_update_file
UUID_PATTERN = re.compile(r'^[\da-f]{8}-([\da-f]{4}-){3}[\da-f]{12}$',
                          re.IGNORECASE)
CONSUL_PORTS = [8300, 8301, 8302, 8400, 8500, 8600]


def _valid_ip_address(string):
    octets = string.split('.')
    if len(octets) != 4:
        return False
    try:
        return all(0 <= int(num) < 256 for num in octets)
    except ValueError:
        return False


def _valid_consul_port(string):
    try:
        port = int(string)
        if port in CONSUL_PORTS:
            return True
        return False
    except ValueError:
        return False


def valid_cluster_port(string):
    idx = string.find(':')
    if idx == -1:
        return False
    ip_address, port = string.split(':', 1)
    return _valid_ip_address(ip_address) and _valid_consul_port(port)


def get_bind_address():
    return CONF.node.cluster_ip


def get_ip_address():
    if CONF.node.ip_address:
        return CONF.node.ip_address

    # No IP address has been configured try to get it directly
    try:
        # Get the interface where the default gateway is configured
        default_interface = gateways()['default'].values()[0][1]
        # Get the IP address configured on that interface
        default_ip = ifaddresses(default_interface)[AF_INET][0]['addr']
        return default_ip
    except Exception:
        return ''


def get_consul_role_for_host(hostid):
    cc = consul.Consul()
    members = cc.agent.members()
    targets = [x for x in members if x['Name'] == hostid]
    if len(targets) == 1:
        return targets[0]['Tags']['role']
    return None


class report_object(dict):
    # the report send to hamgr should be like this
    # {
    #   "event" : {
    #   },
    #   "consul" : {
    #   }
    # }

    event = {}
    consul = {}

    def __init__(self, event_obj, consul_obj):
        self.event = event_obj
        self.consul = consul_obj
        dict.__init__(self, event=event_obj, consul=consul_obj)

    def __eq__(self, other):
        if not isinstance(other, report_object):
            return False
        if (self.event['hostName'] == other.event['hostName'] and
                self.event['eventType'] == other.event['eventType']):
            return True
        return False

    def __repr__(self):
        obj = {
            'event': json.dumps(self.event),
            'consul': json.dumps(self.consul)
        }
        return json.dumps(obj)

    @classmethod
    def from_str(cls, string):
        try:
            obj = json.loads(string)
            e_obj = obj['event']
            c_obj = obj['consul']
            return cls(e_obj, c_obj)
        except:
            LOG.exception('failed to parse json string : %s ', str(string))
        return None


class consul_status(object):
    last_status = {}
    last_status_update_time = None
    current_status = {}
    cluster = None
    cc = None
    dirty = False
    host_id = None
    changed_clusters = {}
    leader = False

    def __init__(self, host_id, hosts_ips, cluster_details):
        assert isinstance(hosts_ips, list)
        assert isinstance(cluster_details, list)
        self.cc = consul.Consul()
        self.host_id = host_id
        self.hosts_ips = []
        self.cluster_details = cluster_details
        if hosts_ips and len(hosts_ips) > 0:
            self.hosts_ips = hosts_ips
        reap_interval = CONF.consul.key_reap_interval
        self.reap_interval = timedelta(minutes=reap_interval)
        self.publish_hostid()

    def publish_hostid(self):
        """This function updates the KV store with the

        <ip_addres>:8301=<host ID>. If such a key already exists with the same
        value then it is not updated
        """
        key = '%s:%s' % (get_ip_address(), '8301')
        try:
            if self.cluster_alive():
                # KV store is not available when quorum is lost
                _, data = self.kv_fetch(key)
                if not data:
                    LOG.debug('Adding {key}={id}'.format(key=key,
                                                        id=self.host_id))
                    self.kv_update(key, self.host_id)
                else:
                    if data['Value'] != self.host_id:
                        LOG.debug('Updating {key} to {id}'.format(
                            key=key, id=self.host_id))
                        self.kv_update(key, self.host_id)
            else:
                LOG.warning('Not adding {id} to KV since cluster is '
                         'unavailable'.format(id=self.host_id))
        except Exception as e:
            LOG.warning('failed to publish host id %s, error : %s', key, str(e))

    def get_consul_status_report(self):
        _, kv_list = self.kv_fetch('', recurse=True)
        consul_report = {
            'leader': self.cc.status.leader(),
            'peers': self.cc.status.peers(),
            'members': self.cc.agent.members(),
            'kv': '',  # don't take kv store to avoid too large string
            'joins': str(CONF.consul.join),
        }

        return consul_report

    def get_cluster_report(self, current_time=datetime.now()):
        cluster_report = {}
        members = self.cc.agent.members()
        members_info = []

        for member in members:
            LOG.debug('member name %s addr %s status %s', str(member.get('Name')), str(member.get('Addr')),
                      str(member.get('Status')))
            if member.get('Status', 4) == 1:
                # Node alive
                event_type = 1
            else:
                # Node failed
                event_type = 2

            node_info = {"Name": member.get("Name"), "Addr": member.get("Addr"), "Status": member.get("Status")}
            members_info.append(node_info)

            if member.get('Addr') not in self.hosts_ips:
                # Cannot get the host id, which means that ha-slave is not
                # running. We cannot be sure of the cluster state and reporting
                # without the host id does not work hence skip this host.
                # LOG.warning('host %s is not registered in kv store', key)
                msg = 'ignore host %s with ip %s, which is not expected to be in consul cluster %s' % \
                      (str(member.get('Name')), str(member.get('Addr')), str(member.get('Tags')))
                LOG.warning(msg)
                continue

            hostname = member.get('Name')

            _, kv_list = self.kv_fetch('', recurse=True)
            consul_obj = {
                'leader': self.cc.status.leader(),
                'peers': self.cc.status.peers(),
                'members': self.cc.agent.members(),
                'kv': '',  # don't take kv store to avoid too large string
                'joins': str(CONF.consul.join),
            }

            event_obj = {
                'eventId': str(uuid4()),
                'eventType': event_type,
                'hostAddr': str(member.get('Addr')),
                'hostPort': str(member.get('Port')),
                'hostName': hostname,
                'reported': False,
                'reportedAt': None,
                'reportedBy': str(get_ip_address())
            }

            report = report_object(event_obj, consul_obj)

            cluster_report[member['Addr']] = report
        LOG.debug('latest cluster info : %s', str(members_info))
        if len(self.hosts_ips) != len(members_info):
            LOG.warning('num of consul members is not equal to num of expected hosts : %s', str(self.hosts_ips))
        LOG.debug('latest cluster status report from consul members : %s', str(cluster_report))
        return cluster_report

    def _should_report_change(self):
        report_interval = timedelta(seconds=CONF.consul.report_interval)
        retval = None
        reported_cls = None
        LOG.debug('now checking cached changes against latest status. '
                 'cached changes : %s', str(self.changed_clusters))
        for _, change in self.changed_clusters.items():
            detectedAt = datetime.strptime(change.event['detectedAt'], "%Y-%m-%d %H:%M:%S")
            hostname = change.event['hostName']
            if datetime.now() - detectedAt > report_interval:
                current_state = self.get_cluster_report()
                current_addrs = current_state.keys()
                addr = change.event['hostAddr']
                LOG.debug('checking detected change : %s', str(change))
                if (addr not in current_addrs) or \
                        (addr in current_addrs and current_state[addr].event['eventType'] == change.event['eventType']):
                    if addr not in current_state:
                        LOG.warning('host %s was not returned from consul cluster, still try to report event : %s',
                                 hostname, str(current_state))
                    if change.event['reported']:
                        staled = False
                        if change.event['reportedAt']:
                            reported_at = datetime.strptime(change.event['reportedAt'], "%Y-%m-%d %H:%M:%S")
                            staled = True if datetime.utcnow() - reported_at > self.reap_interval else False

                        LOG.debug('ignore detected change of event %s for host %s that has been reported, details : %s'
                                 ' , is report staled ? %s',
                                 str(change.event['eventType']), hostname, str(change), str(staled))
                        continue
                    reported_cls = change
                    retval = change
                    LOG.debug('found one change of event %s for host %s : %s',
                             str(change.event['eventType']), hostname, str(change))
                    break
                elif current_state[addr].event['eventType'] != \
                        change.event['eventType']:
                    LOG.debug('host %s status in consul has changed. '
                             'old : %s, current : %s',
                             hostname,
                             str(change.event['eventType']),
                             str(current_state[addr].event['eventType']))
            else:
                LOG.debug('change of event %s for host %s has not exceed report grace period. '
                         'now : %s , last change : %s, grace period : %s',
                         str(change.event['eventType']), hostname,
                         str(datetime.now()), str(detectedAt),
                         str(report_interval))
        if reported_cls:
            LOG.debug('examining founded change of event %s for host %s for reporting : %s',
                     str(reported_cls.event['eventType']), str(reported_cls.event['hostName']),
                     str(reported_cls))
            ignore, data = self.kv_fetch(retval.event['hostName'])
            if not data:
                LOG.debug('founded change of event %s for host %s has not reported to kv store, '
                         'now store it. change : %s ',
                         str(reported_cls.event['eventType']), str(reported_cls.event['hostName']),
                         str(reported_cls))
                LOG.info('report event %s for host %s to kv store : %s ',
                         str(retval.event['eventType']), retval.event['hostName'], json.dumps(reported_cls))
                self.kv_update(retval.event['hostName'], json.dumps(reported_cls))
            else:
                LOG.info('founded change of event %s for host %s already exist in kv store, '
                         'change : %s, report : %s',
                         str(reported_cls.event['eventType']), str(reported_cls.event['hostName']),
                         str(reported_cls), str(data))
                # two scenarios when report exist in kv store:
                # 1. eventType = 2
                #  (a) reported = False : when the above time check failed
                #  (b) reported = True  : already reported to hamgr
                # 2. eventType = 1
                #  host now alive from previous down state, should also report
                data_obj = data['Value']
                cls_obj = report_object.from_str(data_obj)
                if retval.event['eventType'] == 1:
                    LOG.debug('founded change for host %s is host up, and existed in kv '
                             'store. still report host up', cls_obj.event['hostName'])
                    # node become alive from down, need to report
                    # return retval
                if cls_obj.event['eventType'] == 2:
                    if cls_obj.event.get('reported'):
                        LOG.debug('founded change for host %s is host down, and exist in kv '
                                 'store, and already reported, so no need '
                                 'to report again', cls_obj.event['hostName'])
                        # Already reported once
                        retval = None
                    else:
                        LOG.debug('founded change is host down change, and '
                                 'exist in kv store, but not reported yet '
                                 'so report it. change : %s , kv report : %s',
                                 str(retval), str(data))
        if retval:
            LOG.debug('found change to be reported to hamgr for event %s for host %s : %s',
                     str(retval['event']['eventType']), str(retval['event']['hostName']),
                     str(retval))
        return retval

    def get_cluster_status(self):
        """This function will return a update the status file when called. If

        cluster status remains changed for x minutes then the changed status
        will be reported back else the older status is returned.
        x is fetched from the conf option - CONF.consul.report_interval and
        it defaults to 6 minutes.
        """
        if not self.leader:
            return None

        # refresh cache on leader role
        self.refresh_cache_from_consul()

        current_time = datetime.now()
        report_change = self._should_report_change()
        if not report_change:
            return None

        self.last_status_update_time = current_time
        self.last_status = self.current_status
        LOG.debug('detected status change for reporting to hamgr for event %s for host %s',
                 str(report_change['event']['eventType']), str(report_change['event']['hostName']))
        return report_change

    def cluster_leader(self):
        if not self.cluster_alive():
            # There is no cluster so no leader
            return None
        leader = self.cc.status.leader()
        return leader

    def am_i_cluster_leader(self):
        if not self.cluster_alive():
            # There is no cluster so no leader
            return False
        cluster_leader = self.cc.status.leader()
        leader_ip = cluster_leader.split(':')[0]
        my_ip = get_ip_address()
        am_i_leader = my_ip == leader_ip
        self.leader = am_i_leader
        return am_i_leader

    def refresh_cache_from_consul(self):
        # ----------------------------------------------------------------------
        # report in kv store :
        #  only record host down event (because leader could be down, needs new
        #  leader to continue to handle it)
        # memory cache  :
        #  record both host down and up event
        #
        # refresh cache specification:
        #  a) read kv store report if any
        #     (i) not reported :  if not in cache then add it
        #     (ii) reported : add to cache if not exist, will remove it
        #               after compare to current consul status if node is alive
        #  b) read consul current status
        #    (i)  node is dead :
        #      (i-1) report not exist : add to cache if not exist
        #                   [[scenario : alive --> dead, never reported]]
        #      (i-2) report exist :
        #        (i-2-1) reported:need to remove from cache and kv store
        #                     [[scenario: alive --> dead, already reported]]
        #        (i-2-2) not reported: add to cache if not exist
        #                     [[scenario: alive --> dead, not reported yet]]
        #    (ii) node is alive
        #      (ii-1) node has report
        #        (ii-1-1) reported:need to remove from cache and kv store
        #         [[scenario : dead --> alive, host up after previous down
        #                      event had been reported ]]
        #        (ii-1-2) not reported: need to remove from cache and kv store
        #         [[scenario : dead --> alive, host down then up so quick,
        #                        previous down event was not even reported]]
        #      (ii-2) node not have report :
        #        (ii-2-1) new host : add to cache
        #         [[scenario : ?? --> alive , new host is added to same
        #                             availability zone ]]
        #        (ii-2-2) existing host : ignore
        #         [[scenario : alive --> alive , no change for existing host]]
        #
        # ----------------------------------------------------------------------
        _, kv_list = self.kv_fetch('', recurse=True)

        fresh_msg = """
        -----------%s refresh-----------\n
        kv store       : %s\n
        change cache   : %s\n
        last status    : %s\n
        consul members : %s\n
        ------------------------------------\n
        """
        LOG.debug(fresh_msg, 'before', str(kv_list), str(self.changed_clusters), str(self.last_status), str(self.cc.agent.members()))

        if kv_list is None:
            kv_list = []
        for kv in kv_list:
            key = kv['Key']
            value = kv['Value']
            if UUID_PATTERN.match(key):
                cls = report_object.from_str(value)
                cached_item = self.changed_clusters.get(cls.event['hostName'], None)
                if not cached_item:
                    self.changed_clusters[cls.event['hostName']]= cls

        all_adds = [member['Addr'] for member in self.cc.agent.members()]
        common_adds = list(set(all_adds).intersection(set(self.hosts_ips)))
        missing_adds = list(set(self.hosts_ips).difference(set(common_adds)))
        unexpected_adds = list(set(all_adds).difference(set(common_adds)))

        if len(unexpected_adds):
            LOG.warning('hosts are unexpected but joined current cluster : %s', str(unexpected_adds))
        if len(missing_adds):
            LOG.warning('hosts are expected but miss fromm current cluster : %s', str(missing_adds))

        LOG.debug("cache is refreshed by using reports from kv store : %s ",
                  str(self.changed_clusters))
        # If the leader node goes down, then host down event is not
        # recorded in KV store. It is possible that leader was not able to
        # record some other failed nodes in KV store. Check the current status
        # and add any failed nodes in the changed_status array that are not
        # present in it already. Addresses bug: IAAS-7044
        current_status = self.get_cluster_report()

        LOG.debug('finding changes by comparing consul current status with '
                 'last status, current:%s , last:%s',
                 str(current_status), str(self.last_status))
        #
        # according to https://www.consul.io/docs/faq.html
        # Q: Are failed or left nodes ever removed?
        # To prevent an accumulation of dead nodes (nodes in either failed or left states),
        # Consul will automatically remove dead nodes out of the catalog.
        # This process is called reaping. This is currently done on a configurable
        # interval of 72 hours. Reaping is similar to leaving, causing all associated
        # services to be deregistered. Changing the reap interval for aesthetic reasons to
        # trim the number of failed or left nodes is not advised (nodes in the failed or
        # left state do not cause any additional burden on Consul).
        #
        # 'consul members' command does not return all the 'failed' or 'left' members, this caused
        # the problem where the host-down event for those 'failed' or 'left' nodes are not processed.
        # so rather than just iterator over what consul returned, we always iterator over the number
        # of join ips, because that's the desired num of members in consul
        for addr in self.hosts_ips:
            data = current_status.get(addr, {})
            # when the host is not returned from consul
            # we check whether host in last_status, if exist,
            # then the host is gone, should report. otherwise, the status of
            # the host is unknown (not joined cluster, or die), in this case
            # treat it as host-down, could be false alarm, but better not to
            # miss it. the only problem is that we don't have host id, but we
            # will use the settings in cluster_details to get the host id with
            # the host ip used in the cluster
            if not data:
                LOG.warning('host %s could not be found from consul cluster', str(addr))
                matched = [x['name'] for x in self.cluster_details if x['addr'] == addr]
                if len(matched) == 0:
                    LOG.warning('host %s is not in cluster, or its ip has changed',
                             str(addr))
                    continue

                last = self.last_status.get(addr, {})
                if last:
                    # mark the cached evenType as -1, so we can detect it
                    self.last_status[addr].event['eventType'] = -1
                    # clone the last status but set eventType to 2
                    import copy
                    data = copy.deepcopy(self.last_status[addr])
                    data.event['eventId'] = str(uuid4())
                    data.event['eventType'] = 2
                    data.event['reported'] = False
                    data.event['reportedBy'] = str(get_ip_address())
                    LOG.debug('cloned and modified event found from last status : %s', str(data))
                else:
                    host_name = matched[0]
                    # now create the 'data' and mark the host is down
                    event_obj = {
                        'eventId': str(uuid4()),
                        'eventType': 2,
                        'hostAddr': str(addr),
                        'hostName': host_name,
                        'reported': False,
                        'reportedAt': None,
                        'reportedBy': str(get_ip_address())
                    }
                    # just leave the consul info as empty
                    consul_obj = {}
                    data = report_object(event_obj, consul_obj)
                    LOG.debug('host-down event created without consul info for host not returned from consul : %s', str(data))

                # add back to current_status
                LOG.debug('made-up host down event for host %s which not returned from consul : %s', addr, str(data))
                current_status[addr] = data

            LOG.debug('checking event for host %s against cache : %s',
                     str(data.event['hostName']), str(data.event['eventType']))
            # first check if new node is added
            if addr not in self.last_status.keys():
                self.last_status[addr] = data
                LOG.debug('cache node found from consul members in last_status : %s', str(data))

            LOG.debug('checking whether need to update kv store for event %s for host %s',
                     str(data.event['eventType']), str(data.event['hostName']))
            status_has_changed = (self.last_status[addr].event['eventType'] != data.event['eventType'])
            if status_has_changed:
                data.event['detectedAt'] = datetime.strftime(datetime.now(),
                                                             '%Y-%m-%d %H:%M:%S')
            if data.event['eventType'] == 2:
                # Failed node
                cached_item = self.changed_clusters.get(data.event['hostName'], None)
                if not cached_item:
                    # check whether this down node has been reported before
                    reported_before = False
                    reported_but_staled = False
                    hostName = data.event['hostName']
                    _, kv_data = self.kv_fetch(hostName)
                    LOG.debug('host %s status is down, check if report exist in kv store',
                             hostName)
                    # if report exist, check whether reported
                    if kv_data:
                        LOG.debug('checking whether existing data for host %s has already reported : %s',
                                  hostName, str(kv_data))
                        existing_cls = report_object.from_str(kv_data['Value'])
                        if existing_cls.event['reported']:
                            reported_before = True
                            reported_time = datetime.strptime(existing_cls.event['reportedAt'],
                                                              "%Y-%m-%d %H:%M:%S")
                            if datetime.utcnow() - reported_time > self.reap_interval:
                                reported_but_staled = True
                                self.kv_delete(hostName)
                            LOG.debug('host %s status is down, and report exist in kv store.'
                                     ' report : %s , reported time : %s , is it staled ? %s',
                                     hostName, str(kv_data), str(reported_time), str(reported_but_staled))
                            #
                    else:
                        LOG.debug('host %s status is down, no report exist in kv store or cache',
                                 hostName)
                    # when host down, and not reported
                    if not reported_before or reported_but_staled:
                        LOG.debug('cache event %s for host %s : %s',
                                 str(data.event['eventType']), str(data.event['hostName']), str(data))
                        data.event['detectedAt'] = datetime.strftime(datetime.now(), '%Y-%m-%d %H:%M:%S')
                        self.changed_clusters[data.event['hostName']] = data

                else:
                    LOG.debug('there was cache record for host %s with event %s, reported ? %s',
                             cached_item.event['hostName'],
                             str(cached_item.event['eventType']), str(cached_item.event['reported']))
                    # check whether the event in cache is the same
                    if data.event['eventType'] != cached_item.event['eventType']:
                        LOG.debug('current event %s , which is different than cached event %s for host %s',
                                 str(data.event['eventType']),
                                 str(cached_item.event['eventType']),
                                 str(data.event['hostName']))
                        LOG.debug('remove old cached record : %s', str(cached_item))
                        self.changed_clusters.pop(cached_item.event['hostName'])
                        _, kv_data = self.kv_fetch(data.event['hostName'])
                        if kv_data:
                            LOG.debug('remove old kv store : %s', str(kv_data))
                            self.kv_delete(data.event['hostName'])
                        LOG.debug('replace old cache with new event : %s', str(data))
                        data.event['detectedAt'] = datetime.strftime(datetime.now(), '%Y-%m-%d %H:%M:%S')
                        self.changed_clusters[data.event['hostName']] = data

            elif data.event['eventType'] == 1:
                # for each current alive node, see if it is in cache
                cached_item = self.changed_clusters.get(data.event['hostName'], None)
                LOG.debug('host %s status is up, check if need to remove '
                         'report from cache and kv store. cache for host: %s',
                         data.event['hostName'],
                         str(cached_item))
                # check whether the node previously was down and reported
                # it is possible the host went down, but before the down event
                # is reported, it goes alive , so no need to report.
                # if host is alive after the previous down event was reported,
                # just need to remove report from kv store
                if not cached_item:
                    if status_has_changed:
                        LOG.debug('cache event %s for host %s, as its status changed and not in cache',
                                  str(data.event['eventType']), str(data.event['hostName']))
                        data.event['detectedAt'] = datetime.strftime(datetime.now(), '%Y-%m-%d %H:%M:%S')
                        self.changed_clusters[data.event['hostName']] = data
                    else:
                        LOG.debug('no need to cache event %s for host %s, as status has not changed',
                                  str(data.event['eventType']), str(data.event['hostName']))
                else:
                    # should be only one report in cache for each host
                    hostName = cached_item.event['hostName']
                    reported = cached_item.event['reported']
                    eventType = cached_item.event['eventType']
                    if reported:
                        # no need to cache reported change
                        LOG.debug('remove cached and reported status with event %s : %s',
                                 str(eventType), str(cached_item))
                        self.changed_clusters.pop(hostName)
                        # no need to store reported event in kv store
                        _, kv_data = self.kv_fetch(hostName)
                        if kv_data:
                            LOG.info('host %s status up, and previous report '
                                     'has been reported, so remove from '
                                     'kv store. report %s : %s',
                                     hostName, hostName, str(kv_data))
                            self.kv_delete(hostName)
                        else:
                            LOG.debug('host %s status up, previous report was '
                                     'reported, but report does not exist in '
                                     'kv store', hostName)

                        # add new state into cache if evenType is different
                        if eventType != data.event['eventType']:
                            LOG.debug('cache the new event %s, has changed since last cached and reported event %s',
                                     str(data.event['eventType']), str(eventType))
                            data.event['detectedAt'] = datetime.strftime(datetime.now(), '%Y-%m-%d %H:%M:%S')
                            self.changed_clusters[hostName] = data
                        else:
                            LOG.debug('not cache current event %s, has not changed since last status with event %s',
                                     str(data.event['eventType']), str(eventType))
                    else:
                        LOG.debug('host %s status up, but previous report in kv store has '
                                 'not been reported yet. report : %s',
                                 hostName, str(cached_item))

        LOG.debug("cache is refreshed by using current consul status: %s ",
                  str(self.changed_clusters))
        # set current status, but the last status will be set to current status
        # after the report process is completed
        self.current_status = current_status
        LOG.debug("cache is updated with info from kv store and latest consul status : %s ", str(self.changed_clusters))
        LOG.debug(fresh_msg, 'after', str(kv_list), str(self.changed_clusters), str(self.last_status), str(self.cc.agent.members()))

    def cluster_alive(self):
        try:
            return self.cc.status.leader() != ''
        except Exception:
            return False

    def kv_fetch(self, key, recurse=False):
        try:
            k, v = self.cc.kv.get(key, recurse=recurse)
            return k, v
        except Exception as e:
            LOG.warning('error when fetch value for key %s : %s', key, e)
        return None, None

    def kv_update(self, key, value):
        try:
            self.cc.kv.put(key, value)
        except Exception as e:
            LOG.warning('error when update key %s with value %s : %s', str(key), str(value), str(e))

    def kv_delete(self, key):
        try:
            LOG.debug('remove from kv store for key : %s', key)
            self.cc.kv.delete(key)
        except Exception as e:
            LOG.warning('error when {id} tried to delete {key}. {error}'.format(
                id=self.host_id, key=key, error=str(e)))

    def get_report_status(self, hostid):
        ignore, data = self.kv_fetch(hostid)
        if data:
            return json.loads(data['Value'])
        LOG.warning('{id} tried to access report status for {host} which '
                 'did not exist'.format(id=self.host_id, host=hostid))
        return None

    def update_reported_status(self, cluster_status):
        temp_cls = cluster_status
        # after change is report to hamgr, remove it from cache
        # the report in kv store is cleaned when refresh cache
        matched = self.changed_clusters.get(temp_cls.event['hostName'], None)
        if matched:
            LOG.debug('remove reported events from cache : %s', str(temp_cls))
            self.changed_clusters.pop(temp_cls.event['hostName'])
        old_status = self.get_report_status(cluster_status.event['hostName'])
        if not old_status:
            LOG.warning('status report for host %s does not exist in kv store',
                     str(cluster_status.event['hostName']))
            return

        # mark this event as reported and store to kv store
        LOG.debug('mark report of event %s for host %s as reported in kv store',
                 str(cluster_status['event']['eventType']), str(cluster_status['event']['hostName']))
        old_status['event']['reported'] = True
        old_status['event']['reportedAt'] = datetime.strftime(datetime.now(), '%Y-%m-%d %H:%M:%S')
        self.kv_update(cluster_status.event['hostName'], json.dumps(old_status))

        LOG.debug('change of event %s for host %s is now marked as done in kv store : %s',
                 str(cluster_status['event']['eventType']), str(cluster_status.event['hostName']),
                 str(self.kv_fetch(str(cluster_status.event['hostName']))))

    def cleanup_consul_kv_store(self):
        _, kv_list = self.kv_fetch('', recurse=True)
        LOG.debug('vk store before clean up staled items: %s', str(kv_list))
        for kv in kv_list:
            key = kv['Key']
            value = kv['Value']
            if UUID_PATTERN.match(key):
                # Dealing with a status report key-value pair
                value = report_object.from_str(value)
                report_time_str = value.event['reportedAt']
                report_uuid = value.event['eventId']
                hostname = value.event['hostName']
                eventType = value.event['eventType']
                if not report_uuid or not report_time_str:
                    # This key value pair was not reported.Don't delete the key
                    continue
                report_time = datetime.strptime(report_time_str,
                                                "%Y-%m-%d %H:%M:%S")
                utcnow = datetime.utcnow()
                howold = utcnow - report_time
                staled = True if howold > self.reap_interval else False
                LOG.debug('is report for event id %s type %s for host %s staled ? %s , reported at %s, now %s',
                         report_uuid, str(eventType), str(hostname), str(staled), str(report_time), str(utcnow))
                if staled:
                    LOG.debug('remove staled record for host %s from kv store, key : %s ,event id %s,time in record: %s'
                             'time parsed : %s, current : %s', hostname,
                             key, report_uuid, report_time_str, str(report_time),
                             str(datetime.now()))
                    self.kv_delete(key)
            elif valid_cluster_port(key):
                # Dealing with "<ip>:<port>" = <host_id>
                ip_addr = key.split(':')[0]
                members = self.cc.agent.members()
                addresses = [x['Addr'] for x in members]
                if ip_addr not in addresses:
                    LOG.debug('remove from kv store for unknown host %s '
                             'by key %s , current members : %s',
                             ip_addr, key, str(members))
                    self.kv_delete(key)

    def log_kvstore(self):
        try:
            _, kv_list = self.kv_fetch('', recurse=True)
            LOG.debug('kv store after join: %s', str(kv_list))
            # dump current kv store into file
            record = {
                'timestamp': str(datetime.utcnow()),
                'kvstore': str(kv_list)
            }
            # limit the history file to be at most 500M, if bigger than that
            # then empty it
            max_bytes = 500 * 1024 * 1024
            location = '/opt/pf9/consul-data-dir/kvstore_history.log'
            if os.path.exists(location):
                size = os.path.getsize(location)
                if size >= max_bytes:
                    os.remove(location)
            with open(location, 'a') as fp:
                fp.write(json.dumps(record))
                fp.write('\n')
        except Exception as e:
            LOG.error(str(e))
            pass
