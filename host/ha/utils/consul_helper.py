# Copyright 2016 Platform9 Systems Inc.
# All Rights Reserved

import json
import re

from datetime import datetime
from datetime import timedelta
from os import makedirs
from os.path import dirname
from os.path import exists
from uuid import uuid4

from ha.utils import log as logging
from netifaces import AF_INET
from netifaces import gateways
from netifaces import ifaddresses
from oslo_config import cfg

import consul

LOG = logging.getLogger(__name__)
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


class cluster(object):
    change_time = None
    change_info = {}

    def __init__(self, time, info):
        self.change_info = info
        self.change_time = time

    def __eq__(self, other):
        if not isinstance(other, cluster):
            return False
        if (self.change_info['hostname'] == other.change_info['hostname'] and
                self.change_info['eventType'] == other.change_info[
                    'eventType']):
            return True
        return False

    def __repr__(self):
        jobj = {
            'change_time': datetime.strftime(self.change_time,
                                             "%Y-%m-%d %H:%M:%S"),
            'change_info': json.dumps(self.change_info)
        }
        return json.dumps(jobj)

    @classmethod
    def from_str(cls, string):
        obj = json.loads(string)
        change_time = datetime.strptime(obj['change_time'],
                                        "%Y-%m-%d %H:%M:%S")
        change_info = json.loads(obj['change_info'])
        return cls(change_time, change_info)


class consul_status(object):
    last_status = {}
    last_status_update_time = None
    current_status = {}
    cluster = None
    cc = None
    dirty = False
    host_id = None
    changed_clusters = []
    leader = False

    def __init__(self, host_id):
        if not exists(dirname(LAST_STATUS_UPDATE_FILE)):
            makedirs(dirname(LAST_STATUS_UPDATE_FILE))
        LOG.debug('file %s exist : %s', str(LAST_STATUS_UPDATE_FILE),
                 str(exists(LAST_STATUS_UPDATE_FILE)))
        if exists(LAST_STATUS_UPDATE_FILE):
            with open(LAST_STATUS_UPDATE_FILE) as fptr:
                last_update_json = json.load(fptr)
                self.last_status = last_update_json.get('status', {})
                last_update_time = last_update_json.get('time')
                self.current_status = last_update_json.get('current_status')
                if last_update_time and last_update_time != 'None':
                    self.last_status_update_time = datetime.strptime(
                            last_update_time, "%Y-%m-%d %H:%M:%S")
        self.cc = consul.Consul()
        self.host_id = host_id
        reap_interval = CONF.consul.key_reap_interval
        self.reap_interval = timedelta(minutes=reap_interval)
        self.publish_hostid()

    def publish_hostid(self):
        '''This function updates the KV store with the

        <ip_addres>:8301=<host ID>. If such a key already exists with the same
        value then it is not updated
        '''
        key = '%s:%s' % (get_ip_address(), '8301')
        try:
            if self.cluster_alive():
                # KV store is not available when quorum is lost
                _, data = self.cc.kv.get(key)
                if not data:
                    LOG.info(
                            'Adding {key}={id}'.format(key=key,
                                                       id=self.host_id))
                    self.cc.kv.put(key, self.host_id)
                else:
                    if data['Value'] != self.host_id:
                        LOG.info('Updating {key} to {id}'.format(
                                key=key, id=self.host_id))
                        self.cc.kv.put(key, self.host_id)
            else:
                LOG.warn('Not adding {id} to KV since cluster is '
                         'unavailable'.format(id=self.host_id))
        except Exception as e:
            LOG.warn('failed to publish host id %s, error : %s', key, str(e))

    def _get_cluster_status(self, current_time=datetime.now()):
        cluster_report = {}
        for member in self.cc.agent.members():
            LOG.debug('member addr %s status %s', str(member.get('Addr')),
                      str(member.get('Status')))
            if member.get('Status', 4) == 1:
                # Node alive
                event_type = 1
                detail = 1
                event_id = 1
                start_time = datetime.strftime(current_time,
                                               '%Y-%m-%d %H:%M:%S')
                end_time = ""
            else:
                # Node failed
                event_type = 2
                detail = 2
                event_id = 1
                start_time = end_time = datetime.strftime(current_time,
                                                          '%Y-%m-%d %H:%M:%S')
            cluster_port = "%s:%s" % (member.get('Addr'), member.get('Port'))

            ignore, data = self.cc.kv.get(cluster_port)
            LOG.debug('get kv data for %s : %s', str(cluster_port), str(data))
            if not data:
                # Cannot get the host id, which means that ha-slave is not
                # running. We cannot be sure of the cluster state and reporting
                # without the host id does not work hence skip this host.
                continue
            cluster_id = data['Value']

            # extend report data with key consul info to be reported to ha mgr
            consul_info = {}
            consul_info['leader'] = self.cc.status.leader()
            consul_info['peers'] = self.cc.status.peers()
            consul_info['members'] = self.cc.agent.members()
            consul_info['timestamp'] = datetime.strftime(
                    current_time, '%Y-%m-%d %H:%M:%S')

            cluster_report[member['Addr']] = {
                'eventType': event_type,
                'cluster_port': "%s:%s" % (member.get('Addr'),
                                           member.get('Port')),
                'startTime': start_time,
                'endTime': end_time,
                'hostname': cluster_id,
                'uuid': cluster_id,
                'eventID': event_id,
                'detail': detail,
                'id': str(uuid4()),
                'reported': False,
                'reportedby': str(get_ip_address())
            }
        LOG.debug('cluster_report is : %s', str(cluster_report))
        return cluster_report

    def _should_report_change(self):
        report_interval = timedelta(seconds=CONF.consul.report_interval)
        retval = None
        reported_cls = None
        for cluster in self.changed_clusters:
            if datetime.now() - cluster.change_time > report_interval:
                current_state = self._get_cluster_status()
                addr = cluster.change_info['cluster_port'].split(':')[0]
                LOG.debug('checking cluster : %s', str(cluster))
                if addr in current_state and current_state[addr]['eventType'] \
                        == cluster.change_info['eventType']:
                    if cluster.change_info['reported']:
                        LOG.debug('ignore change that has been reported : %s', str(cluster))
                        continue
                    reported_cls = cluster
                    retval = cluster.change_info
                    LOG.info('found one change %s', str(cluster))
                    break
                elif addr not in current_state:
                    LOG.info('host %s was removed from consul cluster : %s',
                              str(addr), str(current_state))
                elif current_state[addr]['eventType'] != \
                        cluster.change_info['eventType']:
                    LOG.info('host %s status in consul has changed. ' \
                              'old : %s, current : %s',
                              str(addr),
                              str(cluster.change_info['eventType']),
                              str(current_state[addr]['eventType']))
            else:
                LOG.info('change has not exceed report grace period. '\
                          'now : %s , last change : %s, grace period : %s',
                        str(datetime.now()), str(cluster.change_time),
                        str(report_interval))
        if reported_cls:
            ignore, data = self.cc.kv.get(retval['hostname'])
            if not data:
                # only store host down report in kv store
                if retval['eventType'] == 2:
                    LOG.debug('report node down to kv store : %s ', str(retval))
                    self.report_node_down_to_kv(retval['hostname'],
                                                str(reported_cls))
            else:
                # two scenarios when report exist in kv store:
                # 1. eventType = 2
                #  (a) reported = False : when the above time check failed
                #  (b) reported = True  : already reported to hamgr
                # 2. eventType = 1
                #  host now alive from previous down state, should also report
                data_obj = json.loads(data['Value'])
                cls_obj = cluster.from_str(data_obj['node_info'])
                if retval['eventType'] == 1:
                    # node become alive from down, need to report
                    return retval
                if cls_obj.change_info['eventType'] == 2:
                    if cls_obj.change_info.get('reported'):
                        # Already reported once
                        retval = None
        if not retval:
            LOG.debug('change to report to hamgr : %s', str(retval))
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
        return report_change

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
        _, kv_list = self.cc.kv.get('', recurse=True)
        if kv_list is None:
            kv_list = []
        for kv in kv_list:
            key = kv['Key']
            value = kv['Value']
            if UUID_PATTERN.match(key):
                obj = json.loads(value)
                cls = cluster.from_str(obj['node_info'])
                if cls not in self.changed_clusters:
                    self.changed_clusters.append(cls)
        LOG.debug("cache is refreshed by using reports from kv store : %s ",
                  str(self.changed_clusters))
        # If the leader node goes down, then host down event is not
        # recorded in KV store. It is possible that leader was not able to
        # record some other failed nodes in KV store. Check the current status
        # and add any failed nodes in the changed_status array that are not
        # present in it already. Addresses bug: IAAS-7044
        current_status = self._get_cluster_status()
        for addr, data in current_status.items():
            # first check if new node is added
            if addr not in self.last_status:
                self.last_status[addr] = data
                LOG.debug('found alive node :%s', str(data))
            # if last record eventType is different than current
            # no matter what change it is, record it in cache if not already
            if self.last_status[addr].get('eventType') != data['eventType']:
                cls_obj = cluster(datetime.now(), data)
                if cls_obj not in self.changed_clusters:
                    self.changed_clusters.append(cls_obj)
                    LOG.debug("found status of node %s changed from %s to %s",
                              str(addr),
                              str(self.last_status[addr].get('eventType')),
                              str(data['eventType']))
            if data['eventType'] == 2:
                # Failed node
                cls_obj = cluster(datetime.now(), data)
                if cls_obj not in self.changed_clusters:
                    # check whether this down node has been reported before
                    reported_before = False
                    hostid = data['hostname']
                    _, existing_data = self.cc.kv.get(hostid)
                    # if report exist, check whether reported
                    if existing_data:
                        existing_json = json.loads(existing_data['Value'])
                        existing_report = existing_json['node_info']
                        if existing_report:
                            existing_cls = cluster.from_str(existing_report)
                            if existing_cls.change_info['reported']:
                                reported_before = True
                    # when host down, and not reported
                    if not reported_before:
                        self.changed_clusters.append(cls_obj)
            elif data['eventType'] == 1:
                # for each current alive node, see if it is in cache
                cached_alive_nodes = []
                for node in self.changed_clusters:
                    if data['hostname'] == node.change_info['hostname']:
                        cached_alive_nodes.append(node)
                # check whether the node previously was down and reported
                # it is possible the host went down, but before the down event
                # is reported, it goes alive , so no need to report.
                # if host is alive after the previous down event was reported,
                # just need to remove report from kv store
                for node in cached_alive_nodes:
                    hostid = node.change_info['hostname']
                    reported = node.change_info['reported']
                    if reported:
                        # no need to cache reported change
                        self.changed_clusters.remove(node)
                        # no need to store reported event in kv store
                        _, existing_data = self.cc.kv.get(hostid)
                        if existing_data:
                            self.cc.kv.delete(key)

        LOG.debug("cache is refreshed by using current consul status: %s ",
                  str(self.changed_clusters))
        # set current status, but the last status will be set to current status
        # after the report process is completed
        self.current_status = current_status
        LOG.debug("cache is updated : %s ", str(self.changed_clusters))

    def cluster_alive(self):
        try:
            return self.cc.status.leader() != ''
        except Exception:
            return False

    def report_node_down_to_kv(self, hostid, node_info, report_time=None,
                               report_id=None):
        data = {
            'notice_time': datetime.strftime(datetime.now(),
                                             '%Y-%m-%d %H:%M:%S'),
            'report_time': report_time,
            'id': report_id,
            'node_info': node_info
        }
        self.cc.kv.put(hostid, json.dumps(data))

    def update_kv(self, key, value):
        self.cc.kv.put(key, value)

    def delete_from_kv(self, key):
        try:
            self.cc.kv.delete(key)
        except Exception as e:
            LOG.warn('error when {id} tried to delete {key}. {error}'.format(
                    id=self.host_id, key=key, error=str(e)))

    def get_report_status(self, hostid):
        ignore, data = self.cc.kv.get(hostid)
        if data:
            return json.loads(data['Value'])
        LOG.warn('{id} tried to access report status for {host} which '
                 'did not exist'.format(id=self.host_id, host=hostid))
        return None

    def update_reported_status(self, cluster_status):
        temp_cls = cluster(datetime.now(), cluster_status)
        # after change is report to hamgr, remove it from cache
        # the report in kv store is cleaned when refresh cache
        self.changed_clusters.remove(temp_cls)
        old_status = self.get_report_status(cluster_status['hostname'])
        if not old_status:
            LOG.warn('status report for host %s does not exist in kv store',
                     str(cluster_status['hostname']))
            return
        old_status['report_time'] = datetime.strftime(datetime.now(),
                                                      '%Y-%m-%d %H:%M:%S')
        old_status['id'] = cluster_status['id']

        # mark this event as reported and store to kv store
        cls = cluster.from_str(old_status['node_info'])
        cls.change_info['reported'] = True
        old_status['node_info'] = str(cls)
        self.update_kv(cluster_status['hostname'], json.dumps(old_status))
        LOG.debug('update reported change for host %s in kv store : %s',
                  str(cluster_status['hostname']),
                  str(self.cc.kv.get(str(cluster_status['hostname']))))

    def cleanup_consul_kv_store(self):
        _, kv_list = self.cc.kv.get('', recurse=True)
        LOG.info('vk store to clean up : %s', str(kv_list))
        for kv in kv_list:
            key = kv['Key']
            value = kv['Value']
            if UUID_PATTERN.match(key):
                # Dealing with a status report key-value pair
                value = json.loads(value)
                report_time_str = value['report_time']
                report_uuid = value['id']
                if not report_uuid:
                    # This key value pair was not reported.Don't delete the key
                    continue
                report_time = datetime.strptime(report_time_str,
                                                "%Y-%m-%d %H:%M:%S")
                if datetime.now() - report_time > self.reap_interval:
                    self.cc.kv.delete(key)
            elif valid_cluster_port(key):
                # Dealing with "<ip>:<port>" = <host_id>
                ip_addr = key.split(':')[0]
                if ip_addr not in [x['Addr'] for x in self.cc.agent.members()]:
                    self.cc.kv.delete(key)

    def log_kvstore(self):
        try:
            _, kv_list = self.cc.kv.get('', recurse=True)
            LOG.info('kv store after join: %s', str(kv_list))
        except Exception as e:
            LOG.error(str(e))
            pass
