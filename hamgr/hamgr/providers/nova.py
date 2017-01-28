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

import hamgr.db.api as db_api
import hamgr.exceptions as ha_exceptions
import eventlet
import logging
import requests
import threading
import time

from collections import defaultdict
from datetime import datetime
from datetime import timedelta
from hamgr import states
from hamgr import periodic_task
from hamgr.common import utils
from hamgr.common import masakari
from novaclient import client, exceptions
from provider import Provider
from urlparse import urlparse

LOG = logging.getLogger(__name__)
eventlet.monkey_patch()


class NovaProvider(Provider):

    def __init__(self, config):
        self._username = config.get('keystone_middleware', 'admin_user')
        self._passwd = config.get('keystone_middleware', 'admin_password')
        self._auth_uri = config.get('keystone_middleware', 'auth_uri')
        self._tenant = config.get('keystone_middleware', 'admin_tenant_name')
        self._region = config.get('nova', 'region')
        self._token = None
        periodic_task.add_task(self._check_host_aggregate_changes, 120,
                               run_now=True)
        self.hosts_down_per_cluster = defaultdict(dict)
        self.aggregate_task_lock = threading.Lock()
        self.aggregate_task_running = False
        self.host_down_dict_lock = threading.Lock()

    def _check_host_aggregate_changes(self):
        with self.aggregate_task_lock:
            if self.aggregate_task_running:
                LOG.info('Check host aggregates for changes task already running')
                return
            self.aggregate_task_running = True
        self._token = utils.get_token(self._tenant, self._username, self._passwd, self._token)
        clusters = db_api.get_all_active_clusters()
        client = self._get_client()
        for cluster in clusters:
            aggregate_id = cluster.name
            aggregate = self._get_aggregate(client, aggregate_id)
            current_host_ids = set(aggregate.hosts)
            new_host_ids = set()
            active_host_ids = set()
            inactive_host_ids = set()
            removed_host_ids = set()
            try:
                nodes = masakari.get_nodes_in_segment(self._token, aggregate_id)
                db_node_ids = set([node['name'] for node in nodes])
                for host in current_host_ids:
                    if self._is_nova_service_active(host, client=client):
                        if host not in db_node_ids:
                            new_host_ids.add(host)
                        else:
                            active_host_ids.add(host)
                    else:
                        if host in db_node_ids:
                            # Only the host currently part of cluster that are
                            # down are of interest
                            inactive_host_ids.add(host)
                        else:
                            LOG.info('Ignoring down host %s as it is not part'
                                     ' of the cluster', host)
                removed_host_ids = db_node_ids - current_host_ids

                LOG.info('Found %s active hosts', str(active_host_ids))
                LOG.info('Found %s new hosts', str(new_host_ids))
                LOG.info('Found %s inactive hosts', str(inactive_host_ids))
                LOG.info('Found %s removed hosts', str(removed_host_ids))

                if len(new_host_ids) == 0 and len(removed_host_ids) == 0:
                    # No new hosts to process
                    LOG.info('No new hosts to process in {clsid} '
                             'cluster'.format(clsid=cluster.name))
                    continue

                if inactive_host_ids or \
                        cluster.task_state not in [states.TASK_COMPLETED]:
                    # Host aggregate has changed but there are inactive hosts
                    # in the host aggregate or another thread is working on
                    # same cluster so do not reconfigure the cluster yet
                    LOG.warn('Skipping {clsid} because there are inactive '
                             'hosts or incomplete tasks'.format(
                                 clsid=cluster.name))
                    continue

                self._disable(aggregate_id, synchronize=True)
                self._enable(aggregate_id,
                             hosts=list(active_host_ids.union(new_host_ids)))
            except ha_exceptions.ClusterBusy:
                pass
            except ha_exceptions.InsufficientHosts:
                LOG.warn('Disabling HA since number of aggregate %s hosts is '
                         'insufficient', aggregate_id)
            except ha_exceptions.SegmentNotFound:
                LOG.warn('Failover segment for cluster: %s was not found',
                         cluster.name)
            except Exception as e:
                LOG.error('Exception while processing aggregate %s: %s',
                          aggregate_id, e)

        with self.aggregate_task_lock:
            LOG.debug('Aggregate changes task completed')
            self.aggregate_task_running = False

    def _is_nova_service_active(self, host_id, client=None):
        if not client:
            client = self._get_client()
        binary = 'nova-compute'
        services= client.services.list(binary=binary, host=host_id)
        if len(services) == 1:
            if services[0].state == 'up':
                if services[0].status != 'enabled' \
                        and services[0].disabled_reason == 'Host disabled by PF9 HA manager':
                    client.services.enable(binary=binary, host=host_id)
                return True
            return False
        else:
            LOG.error('Found %d nova compute services with %s host id'
                    % (len(services), host_id))
            raise ha_exceptions.HostNotFound(host_id)

    def _get_client(self):
        return client.Client(2,
                             self._username,
                             self._passwd,
                             self._tenant,
                             self._auth_uri + '/v2.0',
                             region_name=self._region,
                             insecure=True,
                             connection_pool=False)

    def _get_all(self, client):
        aggregates = client.aggregates.list()
        result = []
        for aggr in aggregates:
            result.append(self._get_one(client, aggr.id))
        return result

    def _get_one(self, client, aggregate_id):
        _ = self._get_aggregate(client, aggregate_id)
        cluster = None
        try:
            cluster = db_api.get_cluster(str(aggregate_id))
        except ha_exceptions.ClusterNotFound:
            pass

        enabled = cluster.enabled if cluster is not None else False
        if enabled is True:
            task_state = 'completed' if cluster.task_state is None else \
                cluster.task_state
        else:
            task_state = None
        return dict(id=aggregate_id, enabled=enabled, task_state=task_state)

    def get(self, aggregate_id):
        client = self._get_client()

        return [self._get_one(client, aggregate_id)] if aggregate_id is not None \
            else self._get_all(client)

    def _get_aggregate(self, client, aggregate_id):
        try:
            return client.aggregates.get(aggregate_id)
        except exceptions.NotFound:
            raise ha_exceptions.AggregateNotFound(aggregate_id)

    def _validate_hosts(self, hosts):
        # TODO Make this check configurable
        if len(hosts) < 3:
            raise ha_exceptions.InsufficientHosts()
        # TODO check if host is part of any other aggregates

        # Check host state and role status in resmgr before proceeding
        self._token = utils.get_token(self._tenant, self._username, self._passwd, self._token)
        headers = {'X-Auth-Token': self._token['id'],
                   'Content-Type': 'application/json'}
        resmgr_url = 'http://localhost:8080/resmgr/v1/hosts/'
        current_roles = {}
        for host in hosts:
            host_url = '/'.join([resmgr_url, host])
            resp = requests.get(host_url, headers=headers)
            resp.raise_for_status()
            json_resp = resp.json()
            if json_resp['role_status'] != 'ok':
                LOG.warn('Role status of host %s is not ok, not enabling HA at the moment.', host)
                raise ha_exceptions.InvalidHostRoleStatus(host)
            elif json_resp['info']['responding'] == False:
                LOG.warn('Host %s is not responding, not enabling HA at the moment.', host)
                raise ha_exceptions.HostOffline(host)
            current_roles[host] = json_resp['roles']
        return current_roles

    def _auth(self, ip_lookup, token, nodes, role, ip=None):
        assert role in ['server', 'agent']
        url = 'http://localhost:8080/resmgr/v1/hosts/'
        headers = {'X-Auth-Token': token['id'],
                   'Content-Type': 'application/json'}
        for node in nodes:
            LOG.info('Authorizing pf9-ha-slave role on node %s using IP %s',
                     node, ip_lookup[node])
            data = dict(join=ip, ip_address=ip_lookup[node])
            data['bootstrap_expect'] = 3 if role == 'server' else 0
            auth_url = '/'.join([url, node, 'roles', 'pf9-ha-slave'])
            resp = requests.put(auth_url, headers=headers,
                                json=data, verify=False)
            if resp.status_code == requests.codes.not_found and \
                resp.content.find('HostDown'):
                raise ha_exceptions.HostOffline(node)
            # Retry auth if resmgr throws conflict error for upto 2 minutes
            while resp.status_code == requests.codes.conflict:
                LOG.info('Role conflict error for node %s, retrying after 5 sec', node)
                time.sleep(5)
                resp = requests.put(auth_url, headers=headers,
                                    json=data, verify=False)
                if datetime.now() - start_time > timedelta(minutes=2):
                    break
            resp.raise_for_status()

    def _query_resmgr_consul_ip(self, host_id, host_roles):
        # Get ostackhost role name
        roles = filter(lambda x: x.startswith('pf9-ostackhost'), host_roles)
        if len(roles) != 1:
            raise ha_exceptions.InvalidHypervisorRoleStatus(host_id)
        rolename = roles[0]
        # Query consul_ip from resmgr ostackhost role settings
        self._token = utils.get_token(self._tenant, self._username, self._passwd, self._token)
        headers = {'X-Auth-Token': self._token['id'],
                   'Content-Type': 'application/json'}
        resmgr_url = 'http://localhost:8080/resmgr/v1/hosts/'
        host_url = '/'.join([resmgr_url, host_id, 'roles', rolename])
        resp = requests.get(host_url, headers=headers)
        resp.raise_for_status()
        json_resp = resp.json()
        if 'consul_ip' in json_resp and json_resp['consul_ip']:
            return str(json_resp['consul_ip'])
        elif 'novncproxy_base_url' in json_resp and json_resp['novncproxy_base_url']:
            vnc_url = json_resp['novncproxy_base_url']
            LOG.info('vnc url %s', vnc_url)
            return urlparse(vnc_url).hostname
        else:
            return None

    def _get_ips(self, client, nodes, current_roles):
        all_hypervisors = client.hypervisors.list()
        lookup = set(nodes)
        ip_lookup = dict()
        for hyp in all_hypervisors:
            host_id = hyp.service['host']
            if host_id in lookup:
                ip_lookup[host_id] = hyp.host_ip
                # Overwrite host_ip value with consul_ip or novncproxy_base_url IP from ostackhost role
                consul_ip = self._query_resmgr_consul_ip(host_id, current_roles[host_id])
                if consul_ip:
                    LOG.debug('Using consul ip %s from ostackhost role', consul_ip)
                    ip_lookup[host_id] = consul_ip
        return ip_lookup

    def _assign_roles(self, client, hosts, current_roles):
        hosts = sorted(hosts)
        leader = hosts[0]
        servers = hosts[1:3]

        agents = []
        if len(hosts) >= 5:
            servers += hosts[3:5]
            agents = hosts[5:]
        elif len(hosts) >= 4:
            agents = hosts[3:]

        ip_lookup = self._get_ips(client, hosts, current_roles)
        if leader not in ip_lookup:
            LOG.error('Leader %s not found in nova', leader)
            raise ha_exceptions.HostNotFound(leader)

        leader_ip = ip_lookup[leader]
        self._token = utils.get_token(self._tenant, self._username,
                                      self._passwd, self._token)
        self._auth(ip_lookup, self._token, [leader] + servers,
                   'server', ip=leader_ip)
        self._auth(ip_lookup, self._token, agents, 'agent', ip=leader_ip)

    def _enable(self, aggregate_id, hosts=None, next_state=states.TASK_COMPLETED):
        """
        :params aggregate_id: Aggregate ID on which HA is being enabled
        :params hosts: Hosts under the aggregate on which HA is to be enabled
                       This option should only be used when enabling HA on few
                       hosts under an aggregate. By default, HA is enabled on
                       all hosts under a host aggregate.
        :params next_state: state in which the cluster should be if enable
                            completes. This is used in case of cluster is
                            migrating i.e. enable operation is being performed
                            as part of a cluster migration operation.
        """
        client = self._get_client()
        str_aggregate_id = str(aggregate_id)
        cluster = None
        cluster_id = None
        try:
            cluster = db_api.get_cluster(str_aggregate_id)
            cluster_id = cluster.id
        except ha_exceptions.ClusterNotFound:
            pass
        else:
            if cluster.task_state not in [states.TASK_COMPLETED]:
                if cluster.task_state == states.TASK_MIGRATING and \
                        next_state == states.TASK_MIGRATING:
                    LOG.info('Enabling HA has part of cluster migration')
                else:
                    LOG.info('Cluster %s is running task %s, cannot enable',
                            str_aggregate_id, cluster.task_state)
                    raise ha_exceptions.ClusterBusy(str_aggregate_id,
                            cluster.task_state)

        aggregate = self._get_aggregate(client, aggregate_id)
        if not hosts:
            hosts = aggregate.hosts
        else:
            LOG.info('Enabling HA on some of the hosts %s of the %s aggregate',
                     str(hosts), aggregate_id)
        current_roles = self._validate_hosts(hosts)

        self._token = utils.get_token(self._tenant, self._username,
                                      self._passwd, self._token)

        try:
            # 1. Push roles
            self._assign_roles(client, hosts, current_roles)

            # 2. Create cluster
            cluster = db_api.create_cluster_if_needed(str_aggregate_id, states.TASK_CREATING)
            cluster_id = cluster.id
            LOG.info('Creating cluster with id %d', cluster_id)

            # 3. Create fail-over segment
            masakari.create_failover_segment(self._token, str_aggregate_id, hosts)

            LOG.info('Enabling cluster %d', cluster_id)
            db_api.update_cluster(cluster_id, True)
        except Exception as e:
            LOG.error('Cannot enable HA on %s: %s, performing cleanup by disabling', str_aggregate_id, e)

            if cluster_id is not None:
                db_api.update_cluster_task_state(cluster_id, states.TASK_COMPLETED)
            self._disable(aggregate_id)

            if cluster_id is not None:
                try:
                    db_api.update_cluster(cluster_id, False)
                except exceptions.ClusterNotFound:
                    pass
            raise
        else:
            if cluster_id is not None:
                db_api.update_cluster_task_state(cluster_id, next_state)

    def _wait_for_role_removal(self, nodes, rolename='pf9-ha-slave'):
        self._token = utils.get_token(self._tenant, self._username, self._passwd, self._token)
        headers = {'X-Auth-Token': self._token['id'],
                   'Content-Type': 'application/json'}
        url = 'http://localhost:8080/resmgr/v1/hosts/'
        for node in nodes:
            start_time = datetime.now()
            auth_url = '/'.join([url, node])
            resp = requests.get(auth_url, headers=headers)
            resp.raise_for_status()
            json_resp = resp.json()
            while json_resp['role_status'] != 'ok' or \
                    rolename in json_resp['roles']:
                time.sleep(5)
                resp = requests.get(auth_url, headers=headers)
                resp.raise_for_status()
                json_resp = resp.json()
                if datetime.now() - start_time > timedelta(minutes=5):
                    raise ha_exceptions.RoleConvergeFailed(node)

    def _deauth(self, nodes):
        self._token = utils.get_token(self._tenant, self._username,
                                      self._passwd, self._token)
        headers = {'X-Auth-Token': self._token['id'],
                   'Content-Type': 'application/json'}
        url = 'http://localhost:8080/resmgr/v1/hosts/'

        for node in nodes:
            LOG.info('De-authorizing pf9-ha-slave role on node %s', node)
            start_time = datetime.now()
            auth_url = '/'.join([url, node, 'roles', 'pf9-ha-slave'])
            resp = requests.delete(auth_url, headers=headers)
            # Retry deauth if resmgr throws conflict error for upto 2 minutes
            while resp.status_code == requests.codes.conflict:
                LOG.info('Role removal conflict error for node %s, retrying'
                         'after 5 sec', node)
                time.sleep(5)
                resp = requests.delete(auth_url, headers=headers)
                if datetime.now() - start_time > timedelta(minutes=2):
                    break
            resp.raise_for_status()

    def _disable(self, aggregate_id, synchronize=False,
                 next_state=states.TASK_COMPLETED):
        """
        :params aggregate_id: Host aggregate ID on which HA is being disabled
        :params synchronize: when set to True, function blocks till ha-slave
                             role is removed from all the hosts
        :params next_state: state in which the cluster should be if disable
                            completes. This is used in case of cluster is
                            migrating i.e. disable operation is being performed
                            as part of a cluster migration operation. If there
                            is an error during the disable operation cluster
                            will be put in "error removing" state
        """
        str_aggregate_id = str(aggregate_id)
        cluster = None
        try:
            cluster = db_api.get_cluster(str_aggregate_id)
        except ha_exceptions.ClusterNotFound:
            pass

        if cluster:
            if cluster.task_state not in [states.TASK_COMPLETED,
                                          states.TASK_ERROR_REMOVING]:
                if cluster.task_state == states.TASK_MIGRATING and \
                        next_state == states.TASK_MIGRATING:
                    LOG.info('disabling HA as part of cluster migration')
                else:
                    LOG.info('Cluster %s is busy in %s state', cluster.name,
                            cluster.task_state)
                    raise ha_exceptions.ClusterBusy(cluster.name,
                                                    cluster.task_state)

            db_api.update_cluster_task_state(cluster.id, states.TASK_REMOVING)

        try:
            self._token = utils.get_token(self._tenant, self._username,
                                          self._passwd, self._token)
            hosts = None
            try:
                if cluster:
                    # If aggregate has changed, we need to remove role from previous segment's nodes
                    nodes = masakari.get_nodes_in_segment(self._token, cluster.name)
                    hosts = [n['name'] for n in nodes]
                else:
                    # If cluster was not even created, but we are disabling HA to rollback enablement
                    client = self._get_client()
                    aggregate = self._get_aggregate(client, aggregate_id)
                    hosts = set(aggregate.hosts)
            except ha_exceptions.SegmentNotFound:
                LOG.warn('Failover segment for cluster: %s was not found, skipping deauth', cluster.name)

            if hosts:
                self._deauth(hosts)
                if synchronize:
                    self._wait_for_role_removal(hosts)

            masakari.delete_failover_segment(self._token, str_aggregate_id)
        except:
            if cluster:
                db_api.update_cluster_task_state(cluster.id, states.TASK_ERROR_REMOVING)
            raise
        else:
            if cluster:
                db_api.update_cluster(cluster.id, False)
                db_api.update_cluster_task_state(cluster.id, next_state)

    def put(self, aggregate_id, method):
        if method == 'enable':
            self._enable(aggregate_id)
        else:
            self._disable(aggregate_id)

    def _get_cluster_for_host(self, host_id, client=None):
        if not client:
            client = self._get_client()
        clusters = db_api.get_all_active_clusters()
        for cluster in clusters:
            aggregate_id = cluster.name
            aggregate = self._get_aggregate(client, aggregate_id)
            if host_id in aggregate.hosts:
                return cluster
        raise ha_exceptions.HostNotFound(host=host_id)

    def _remove_host_from_cluster(self, cluster, host, client=None):
        if not client:
            client = self._get_client()
        aggregate_id = cluster.name
        aggregate = self._get_aggregate(client, aggregate_id)
        current_host_ids = set(aggregate.hosts)

        try:
            nodes = masakari.get_nodes_in_segment(self._token, aggregate_id)
            db_node_ids = set([node['name'] for node in nodes])
            for current_host in current_host_ids:
                if not self._is_nova_service_active(current_host, client=client):
                    if current_host in db_node_ids and \
                            current_host not in self.hosts_down_per_cluster[cluster.id]:
                        self.hosts_down_per_cluster[cluster.id][current_host] = False
                else:
                    if current_host in self.hosts_down_per_cluster[cluster.id]:
                        self.hosts_down_per_cluster.pop(current_host)

            self.hosts_down_per_cluster[cluster.id][host] = True
            if all([v for k, v in self.hosts_down_per_cluster[cluster.id].items()]):
                host_list = current_host_ids - \
                        set(self.hosts_down_per_cluster[cluster.id].keys())
                # Task state will be managed by this function
                self._disable(aggregate_id, synchronize=True,
                              next_state=states.TASK_MIGRATING)
                self._enable(aggregate_id, hosts=list(host_list),
                             next_state=states.TASK_MIGRATING)
                self.hosts_down_per_cluster.pop(cluster.id)
            else:
                # TODO: Addtional tests to verify that multiple host failures
                #       does not have other side effects
                LOG.info('There are still down hosts that need to be reported'
                         ' before reconfiguring the cluster')
        except:
            LOG.exception('Could not process {host} host down'.format(host=host))
        db_api.update_cluster_task_state(cluster.id, states.TASK_COMPLETED)

    def host_down(self, event_details):
        host = event_details['hostname']
        time = event_details['time']
        event = 'STOPPED'
        host_status = 'NORMAL'
        cluster_status = 'OFFLINE'
        notification_type = 'COMPUTE_HOST'
        payload = {
            "event": event,
            "host_status": host_status,
            "cluster_status": cluster_status
        }

        try:
            cluster = self._get_cluster_for_host(host)
            db_api.update_cluster_task_state(cluster.id, states.TASK_MIGRATING)
            cluster = db_api.get_cluster(cluster.id)
            self._token = utils.get_token(self._tenant, self._username,
                                          self._passwd, self._token)
            masakari.create_notification(self._token, notification_type,
                                         host, time, payload)
            def _remove_host_task():
                self._remove_host_from_cluster(cluster, host)

            periodic_task.add_task(_remove_host_task, 0, run_now=True, run_once=True)
            retval = True
        except:
            LOG.exception('Error processing {host} host down'.format(host=host))
            retval = False
        return retval

    def host_up(self, event_details):
        host = event_details['hostname']
        try:
            if self._is_nova_service_active(host):
                # When the cluster was reconfigured for host down event this
                # node is removed from masakari. Hence generating a host up
                # notification will result in 404. The node will be added back
                # in the cluster with the next periodic task run.
                return True
            # No point in adding the node back if nova-compute is down
            return False
        except:
            return False
        return True

def get_provider(config):
    db_api.init(config)
    return NovaProvider(config)

