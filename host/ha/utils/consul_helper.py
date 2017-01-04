# Copyright 2016 Platform9 Systems Inc.
# All Rights Reserved

from datetime import datetime
from datetime import timedelta
from ha.utils import log as logging
from netifaces import gateways
from netifaces import ifaddresses
from netifaces import AF_INET
from os import makedirs
from os.path import dirname
from os.path import exists
from oslo_config import cfg
from subprocess import check_output
from subprocess import CalledProcessError
from uuid import uuid4

import json
import consul
import re


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
    cfg.IntOpt('key_reap_interval', default=72*60,
               help='Minutes before stale key value entries are deleted.')
]

node_grp = cfg.OptGroup('node', title='Options related to a consul node')

node_opts = [
    cfg.StrOpt('ip_address', help='IP Address that provides connectivity to'
               ' other hosts', default="")
]
CONF.register_group(consul_grp)
CONF.register_opts(consul_opts, consul_grp)
CONF.register_group(node_grp)
CONF.register_opts(node_opts, node_grp)

LAST_STATUS_UPDATE_FILE = CONF.consul.last_update_file
UUID_PATTERN = re.compile(r'^[\da-f]{8}-([\da-f]{4}-){3}[\da-f]{12}$', re.IGNORECASE)
CONSUL_PORTS = [8300, 8301, 8302, 8400, 8500, 8600]


def _valid_ip_address(string):
    octets = string.split('.')
    if len(octets) != 4:
        return False
    try:
        return all(0<=int(num)<256 for num in octets)
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
    except:
        return ''


class cluster:
    change_time = None
    change_info = {}
    def __init__(self, time, info):
        self.change_info = info
        self.change_time = time

    def __eq__(self, other):
        if not isinstance(other, cluster):
            return False
        if (self.change_info['hostname'] == other.change_info['hostname'] and
                self.change_info['eventType'] == other.change_info['eventType']):
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
        change_time = datetime.strptime(obj['change_time'], "%Y-%m-%d %H:%M:%S")
        change_info = json.loads(obj['change_info'])
        return cls(change_time, change_info)


class consul_status:
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
        '''
        This function updates the KV store with the <ip_addres>:8301=<host ID>.
        If such a key already exists with the same value then it is not updated
        '''
        key = '%s:%s' % (get_ip_address(), '8301')
        if self.cluster_alive():
            # KV store is not available when quorum is lost
            _, data = self.cc.kv.get(key)
            if not data:
                LOG.info('Adding {key}={id}'.format(key=key, id=self.host_id))
                self.cc.kv.put(key, self.host_id)
            else:
                if data['Value'] != self.host_id:
                    LOG.info('Updating {key} to {id}'.format(
                        key=key, id=self.host_id))
                    self.cc.kv.put(key, self.host_id)
        else:
            LOG.warn('Not adding {id} to KV since cluster is '
                     'unavailable'.format(id=self.host_id))

    def _get_cluster_status(self, current_time=datetime.now()):
        cluster_report = {}
        for member in self.cc.agent.members():
            if member.get('Status', 4) == 1:
                # Node alive
                event_type = 1
                detail = 1
                event_id = 1
                start_time = datetime.strftime(current_time, '%Y-%m-%d %H:%M:%S')
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
            if not data:
                # Cannot get the host id, which means that ha-slave is not
                # running. We cannot be sure of the cluster state and reporting
                # without the host id does not work hence skip this host.
                continue
            cluster_id = data['Value']

            cluster_report[member['Addr']] = {
                'eventType': event_type,
                'cluster_port': "%s:%s" % (member.get('Addr'), member.get('Port')),
                'startTime': start_time,
                'endTime': end_time,
                'hostname': cluster_id,
                'uuid': cluster_id,
                'eventID': event_id,
                'detail': detail,
                'id': str(uuid4())
            }
        return cluster_report

    def _should_report_change(self):
        report_interval = timedelta(seconds=CONF.consul.report_interval)
        retval = None
        reported_cls = None
        for cluster in self.changed_clusters:
            LOG.debug('Checking %s status before reporting',
                    cluster.change_info['cluster_port'])
            if datetime.now() - cluster.change_time > report_interval:
                current_state = self._get_cluster_status()
                addr = cluster.change_info['cluster_port'].split(':')[0]
                if addr in current_state and current_state[addr]['eventType'] \
                        == cluster.change_info['eventType']:
                    reported_cls = cluster
                    retval = cluster.change_info
                    break
        if reported_cls:
            ignore, data = self.cc.kv.get(retval['hostname'])
            if not data:
                self.report_node_down_to_kv(retval['hostname'], str(reported_cls))
            else:
                data_obj = json.loads(data['Value'])
                if data_obj.get('id'):
                    # Already reported once
                    retval = None
        return retval

    def update(self, current_status):
        for key, value in current_status.items():
            if key not in self.last_status:
                # New node was added
                self.last_status[key] = current_status[key]
                LOG.info('New node added %s', key)
            elif value.get('eventType') != \
                    self.last_status[key].get('eventType'):
                LOG.info('Status of {node} changed from {old} to {new}'.format(
                    node=key, old=self.last_status[key].get('eventType'),
                    new=value.get('eventType')))
                # Status of a node has changed
                cls_obj = cluster(datetime.now(), current_status[key])
                if cls_obj not in self.changed_clusters:
                    self.changed_clusters.append(cls_obj)
                self.dirty = True
        self.current_status = current_status
        with open(LAST_STATUS_UPDATE_FILE, 'w') as fptr:
            fptr.truncate()
            json.dump({
                'status': self.last_status,
                'time': datetime.strftime(self.last_status_update_time,
                                          '%Y-%m-%d %H:%M:%S'),
                'current_status': self.current_status
            }, fptr)

    def get_cluster_status(self):
        """
        This function will return a update the status file when called. If
        cluster status remains changed for x minutes then the changed status
        will be reported back else the older status is returned.
        x is fetched from the conf option - CONF.consul.report_interval and
        it defaults to 6 minutes.
        """
        result = {}
        current_time = datetime.now()
        report_change = self._should_report_change()
        current_status = self._get_cluster_status(current_time)
        self.last_status_update_time = current_time
        self.update(current_status)
        if report_change:
            self.last_status = self.current_status
            return report_change
        return None

    def am_i_cluster_leader(self):
        if not self.cluster_alive():
            # There is no cluster so no leader
            return False
        cluster_leader = self.cc.status.leader()
        leader_ip = cluster_leader.split(':')[0]
        my_ip = get_ip_address()
        am_i_leader = my_ip == leader_ip

        # Check if a node is becoming leader or giving up leader status
        if am_i_leader != self.leader:
            self.leader = am_i_leader
            if am_i_leader:
                # Node just became the leader
                self.populate_cache_from_kv()
            else:
                # Node gave up leadership
                # Clear the changed_clusters array so that there is nothing
                # to report
                self.changed_clusters = []
        return am_i_leader

    def populate_cache_from_kv(self):
        _, kv_list = self.cc.kv.get('', recurse=True)
        self.changed_clusters = []
        if kv_list is None:
            return
        for kv in kv_list:
            key = kv['Key']
            value = kv['Value']
            if UUID_PATTERN.match(key):
                obj = json.loads(value)
                cls = cluster.from_str(obj['node_info'])
                self.changed_clusters.append(cls)

    def cluster_alive(self):
        try:
            return self.cc.status.leader() != ''
        except:
            return False

    def report_node_down_to_kv(self, hostid, node_info, report_time=None,
                               report_id=None):
        data = {
            'notice_time': datetime.strftime(datetime.now(), '%Y-%m-%d %H:%M:%S'),
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
        except:
            LOG.warn('{id} tried to delete non-existent key - {key}'.format(
                id=self.host_id, key=key))

    def get_report_status(self, hostid):
        ignore, data = self.cc.kv.get(hostid)
        if data:
            return json.loads(data['Value'])
        else:
            LOG.warn('{id} tried to access report status for {host} which '
                     'did not exist'.format(id=self.host_id, host=hostid))
            return None

    def update_reported_status(self, cluster_status):
        temp_cls = cluster(datetime.now(), cluster_status)
        self.changed_clusters.remove(temp_cls)
        old_status = self.get_report_status(cluster_status['hostname'])
        if not old_status:
            return
        old_status['report_time'] = datetime.strftime(datetime.now(),
                                                      '%Y-%m-%d %H:%M:%S')
        old_status['id'] = cluster_status['id']
        self.update_kv(cluster_status['hostname'], json.dumps(old_status))

    def cleanup_consul_kv_store(self):
        _, kv_list = self.cc.kv.get('', recurse=True)
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

