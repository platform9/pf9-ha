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

import json
import logging
import threading
import time

from collections import defaultdict
from datetime import datetime
from datetime import timedelta
from urlparse import urlparse

import eventlet
import requests

from hamgr.common import masakari
from hamgr.common import utils
from hamgr.db import api as db_api
from hamgr import exceptions as ha_exceptions
from hamgr import periodic_task
from hamgr.providers.provider import Provider
from hamgr import states
from hamgr import constants
from novaclient import client
from novaclient import exceptions
from keystoneauth1 import loading
from keystoneauth1 import session

from hamgr.notification import publish
from hamgr.notification import notification as ha_notification

LOG = logging.getLogger(__name__)
eventlet.monkey_patch()


class NovaProvider(Provider):

    def __init__(self, config):
        self._username = config.get('keystone_middleware', 'admin_user')
        self._passwd = config.get('keystone_middleware', 'admin_password')
        self._auth_uri = config.get('keystone_middleware', 'auth_uri')
        self._auth_uri_v3 = "%s/v3" % self._auth_uri
        self._tenant = config.get('keystone_middleware', 'admin_tenant_name')
        self._region = config.get('nova', 'region')
        self._token = None
        # make sure the waiting timeout for masakari to process notification
        # falls in reasonable range. normally it should finish in 5 minutes
        # in a cluster of 5 nodes, if 2 hosts are down at same time, normally
        # masakari takes 2 times of processing time for single host (also
        # dependents on how many vms hosted on it by nova)
        self._notification_stale_minutes = 30
        if config.has_section('masakari'):
            setting_timeout = config.getint('masakari',
                                            'notification_waiting_minutes')
            if setting_timeout < 5:
                raise ha_exceptions.ConfigException(
                    'invalid setting in '
                    'configuration file : notification_waiting_minutes , '
                    'should be equal or bigger than 5')

            self._notification_stale_minutes = setting_timeout
        LOG.info('masakari notification waiting timeout is set to %s '
                 'minutes', str(self._notification_stale_minutes))

        # to suppress possible too much host events in the case where
        # salve failed to report the event to hamgr du to network issue,
        # use the configured threshold seconds to check whether similar event
        # has been reported, if not then record down, otherwise ignore it
        self._event_report_threshold_seconds = 30
        setting_seconds = config.getint('DEFAULT',
                                        'event_report_threshold_seconds')
        if setting_seconds <= 0:
            raise ha_exceptions.ConfigException('invalid setting in ' \
                                                'configuration file : '
                                                'event_report_threshold_seconds ' \
                                                'should be bigger than 0')
        else:
            self._event_report_threshold_seconds = setting_seconds
        LOG.info('event report threshold seconds is %s',
                 str(self._event_report_threshold_seconds))
        self.hosts_down_per_cluster = defaultdict(dict)
        self.aggregate_task_lock = threading.Lock()
        self.aggregate_task_running = False
        self.host_down_dict_lock = threading.Lock()
        # thread lock
        self.events_processing_lock = threading.Lock()
        self.events_processing_running = False
        # ha status thread lock
        self.ha_status_processing_lock = threading.Lock()
        self.ha_status_processing_running = False

    def _toggle_host_maintenance_state(self, host_id, segment_name,
                                       on_maintenance):
        LOG.debug(
            'toggle masakari host %s in segment %s to maintenance state %s',
            str(host_id), str(segment_name), str(on_maintenance))
        try:
            masakari.update_host_maintenance(self._token, host_id, segment_name,
                                             on_maintenance)

        except Exception as e:
            LOG.error('failed to toggle maintenance for host %s to %s , '
                      'error : %s', host_id, str(on_maintenance), str(e))

    # thread is dedicated to handle recorded host events
    def host_events_processing(self):
        # wait until current task complete
        with self.events_processing_lock:
            if self.events_processing_running:
                LOG.debug('events processing task is already running')
                return
            self.events_processing_running = True

        try:
            LOG.debug('handle host events now at %s', str(datetime.utcnow()))

            # ---------------------------------------------------------------------
            # because masakari only be able to handle one notification each time
            # for the same segment, so for the same segment need to
            # create/handle
            # notification in sequential manner
            #
            # for all unhandled processing events (both host-down and host-up) ,
            #  1 sort the records by event_time
            #  2 for each record
            #    2.1 if host-down event
            #      a) if host still down in nova
            #        if notification_id is empty, then create notification
            #        if notification_status is not 'running' or 'finished', then
            #           track notification progress
            #      b) if host is up in nova
            #         mark the event as aborted
            #    2.2 if host-up event
            #      c) if host still up in nova
            #         mark event as finished
            #      d) if host is down in nova
            #         check there is unhandled host-down processing event exist
            #         if such event exist, just mark current host-up event as
            #         finished
            #         if no such event, then check timestamp , if this
            # host-up event
            #         is too old, just delete it, otherwise ignore it until it
            #         become too old to be deleted
            # ---------------------------------------------------------------------
            unhandled_events = db_api.get_all_unhandled_processing_events()
            if not unhandled_events:
                LOG.debug('no unhandled processing events found')
                return
            events = sorted(unhandled_events, key=lambda x: x.event_time)
            if not events or len(events) <= 0:
                LOG.debug('no unhandled processing events to process')
                return
            LOG.info('found unhandled events : %s', str(events))
            # TODO : https://platform9.atlassian.net/browse/IAAS-9060
            self._token = utils.get_token(self._auth_uri_v3,
                                          self._tenant, self._username,
                                          self._passwd, self._token)
            nova_client = self._get_client()
            time_out = timedelta(minutes=self._notification_stale_minutes)
            for event in events:
                LOG.debug('start of handling event %s', str(event))
                event_uuid = event.event_uuid
                event_type = event.event_type
                event_time = event.event_time
                host_name = event.host_name
                cluster = db_api.get_cluster(int(event.cluster_id))
                # if cluster of this event is disabled, no need to handle it
                if not cluster or not cluster.enabled:
                    LOG.debug('cluster %s in event %s is not enabled nor exist',
                              event.cluster_id, str(event))
                    continue
                LOG.debug('found ha cluster with id %s : %s', event.cluster_id,
                          str(cluster))
                nova_aggregate = self._get_aggregate(nova_client, cluster.name)
                # if no such host aggregate, no need to handle it
                if not nova_aggregate:
                    LOG.debug('no nova host aggregation found with name %s',
                              str(cluster.name))
                    continue
                # if host in the event not exist in host aggregate, no need to
                # handle the event
                if host_name not in nova_aggregate.hosts:
                    LOG.debug('host %s in event does not exist in nova %s',
                              host_name, str(nova_aggregate.hosts))
                    continue
                # if the event happened long time ago, just abort
                if datetime.utcnow() - event_time > time_out:
                    db_api.update_processing_event_with_notification(
                        event_uuid, None, None,
                        constants.STATE_ABORTED)
                    LOG.warn('event %s is aborted as it is too stale',
                             str(event_uuid))
                    continue

                # check whether host is active in nova
                is_active = self._is_nova_service_active(host_name,
                                                         client=nova_client)
                LOG.debug('is host %s active in nova ? %s', host_name,
                          str(is_active))

                # for 'host-down' event
                if event_type == constants.EVENT_HOST_DOWN:
                    if is_active:
                        # since host is alive, mark it as aborted
                        db_api.update_processing_event_with_notification(
                            event_uuid, None, None, constants.STATE_ABORTED)
                        LOG.info('event %s is marked as aborted, '
                                 'because host is alive', event_uuid)
                    else:

                        # host still dead, see whether reported to masakari
                        if not event.notification_uuid:
                            notification_obj = self._report_event_to_masakari(
                                event)
                            if notification_obj:
                                LOG.info('event %s is reported to masakari : '
                                          '%s', event_uuid,
                                          str(notification_obj))
                                event.notification_uuid = \
                                    notification_obj['notification'][
                                        'notification_uuid']
                                state = self._tracking_masakari_notification(
                                    event)
                                LOG.info('event %s is updated with '
                                          'notification state %s', event_uuid,
                                          state)
                        else:
                            state = self._tracking_masakari_notification(event)
                            LOG.info('event %s is updated with '
                                      'notification state %s', event_uuid,
                                      state)
                elif event_type == constants.EVENT_HOST_UP:
                    if is_active:
                        db_api.update_processing_event_with_notification(
                            event_uuid, None, None, constants.STATE_FINISHED)
                        LOG.info('event %s is marked as %s',
                                 event_uuid, constants.STATE_FINISHED)
                    else:
                        # when there is host-up, but actual nova status is down
                        # there should be a host-down event soon, sine the
                        # pf9-slave has a grace period of time to report events
                        # so here just wait the new host-down event, or just
                        # wait until this event become stale
                        LOG.warn('event %s is host-up event at %s but host '
                                 'is not active in nova now %s', event_uuid,
                                 event_time, datetime.utcnow())
                LOG.debug('end of handling event %s', str(event))
        finally:
            # release thread lock
            with self.events_processing_lock:
                self.events_processing_running = False

    def _report_event_to_masakari(self, event):
        if not event:
            return
        try:
            data = {
                "event": 'STOPPED',
                "host_status": 'NORMAL',
                "cluster_status": 'OFFLINE'
            }
            notification_obj = \
                masakari.create_notification(self._token,
                                             'COMPUTE_HOST',
                                             event.host_name,
                                             str(event.event_time),
                                             data)
            if notification_obj and notification_obj.get('notification',
                                                         None):
                return notification_obj
        except Exception:
            LOG.error('failed to create masakari notification, error',
                      exc_info=True)
        return None

    def _tracking_masakari_notification(self, event):
        if not event:
            return None
        notification_uuid = event.notification_uuid
        state = masakari.get_notification_status(self._token, notification_uuid)
        LOG.debug('masakari notification %s status : %s',
                  str(notification_uuid), str(state))
        # save the state to event
        db_api.update_processing_event_with_notification(event.event_uuid,
                                                         event.notification_uuid,
                                                         event.notification_created,
                                                         state)
        return state

    def check_host_aggregate_changes(self):
        with self.aggregate_task_lock:
            if self.aggregate_task_running:
                LOG.info('Check host aggregates for changes task already '
                         'running')
                return
            self.aggregate_task_running = True
        self._token = utils.get_token(self._auth_uri_v3,
                                      self._tenant, self._username,
                                      self._passwd, self._token)
        clusters = db_api.get_all_active_clusters()
        LOG.info('checking active clusters : %s', str([x.name for x in clusters]))
        client = self._get_client()
        for cluster in clusters:
            aggregate_id = cluster.name
            segment_name = str(cluster.id)
            aggregate = self._get_aggregate(client, aggregate_id)
            current_host_ids = set(aggregate.hosts)
            new_host_ids = set()
            active_host_ids = set()
            inactive_host_ids = set()
            removed_host_ids = set()
            remaining_host_ids = set()
            try:
                nodes = masakari.get_nodes_in_segment(self._token,
                                                      segment_name)
                db_node_ids = set([node['name'] for node in nodes])

                LOG.debug("hosts records from nova : %s",
                          ','.join(list(current_host_ids)))
                LOG.debug("hosts records from masakari : %s",
                          ','.join(list(db_node_ids)))

                for host in current_host_ids:
                    if self._is_nova_service_active(host, client=client):
                        remaining_host_ids.add(host)

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

                # only when the cluster state is completed
                if cluster.task_state in [states.TASK_COMPLETED]:
                    for hid in remaining_host_ids:
                        # already know the host is active in nova
                        xhosts = filter(lambda x: str(x['name']) == str(hid),
                                        nodes)
                        if len(xhosts) == 1 and xhosts[0]['on_maintenance']:
                            LOG.info('toggle maintenance for host %s',
                                     str(hid))
                            # need to unlock the host
                            self._toggle_host_maintenance_state(hid,
                                                                segment_name,
                                                                False)

                if len(new_host_ids) == 0 and len(removed_host_ids) == 0:
                    # No new hosts to process
                    LOG.info('No new hosts to process in {clsid} '
                             'cluster'.format(clsid=cluster.name))

                    # make sure ha-slave is enabled on existing active hosts
                    current_roles = self._validate_hosts(remaining_host_ids,
                                                         check_cluster=False)
                    LOG.debug("found roles : %s", str(current_roles))
                    tmp_ids = set()
                    for r_host in remaining_host_ids:
                        h_roles = current_roles[r_host]
                        LOG.debug("host %s  has roles : %s", str(r_host),
                                  ','.join(h_roles))
                        if "pf9-ha-slave" not in h_roles:
                            tmp_ids.add(r_host)

                    if len(tmp_ids) > 0:
                        txt_ids = ','.join(list(tmp_ids))
                        LOG.info("assign ha-slave role for hosts : %s",
                                  txt_ids)
                        self._assign_roles(client, remaining_host_ids,
                                           current_roles)
                        LOG.debug("ha-slave roles are assigned to : %s",
                                  txt_ids)
                    else:
                        LOG.info("all hosts already have pf9-ha-slave role")

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
                LOG.info('trying to disable HA aggregate %s', str(aggregate_id))
                self._disable(aggregate_id, synchronize=True)
                target = list(active_host_ids.union(new_host_ids))
                LOG.info('trying to enable HA aggregate %s on hosts %s',
                         str(aggregate_id), str(target))
                self._enable(aggregate_id, hosts=target)
            except ha_exceptions.ClusterBusy:
                pass
            except ha_exceptions.InsufficientHosts:
                LOG.warn('Detected number of aggregate %s hosts is '
                         'insufficient', aggregate_id)
            except ha_exceptions.SegmentNotFound:
                LOG.warn('Masakari segment for cluster: %s was not found',
                         cluster.name)
            except ha_exceptions.HostPartOfCluster:
                LOG.error("Not enabling cluster as cluster hosts are part of"
                          "another cluster")
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
        services = client.services.list(binary=binary, host=host_id)
        if len(services) == 1:
            if services[0].state == 'up':
                disabled_reason = 'Host disabled by PF9 HA manager'
                if services[0].status != 'enabled' and \
                        services[0].disabled_reason == disabled_reason:
                    LOG.info('host %s state is up, status is %s, '
                              'disabled reason %s, so enable it',
                              str(host_id), str(services[0].status),
                              str(services[0].disabled_reason))
                    client.services.enable(binary=binary, host=host_id)
                LOG.debug("nova host %s is up and enabled", str(host_id))
                return True
            LOG.debug("nova host %s state is down", str(host_id))
            return False
        else:
            LOG.error('Found %d nova compute services with %s host id'
                      % (len(services), host_id))
            raise ha_exceptions.HostNotFound(host_id)

    def _get_client(self):
        # reference:
        # https://docs.openstack.org/python-novaclient/latest/reference/api/index.html
        loader = loading.get_plugin_loader('password')
        auth = loader.load_from_options(auth_url=self._auth_uri,
                                        username=self._username,
                                        password=self._passwd,
                                        project_name=self._tenant)
        sess = session.Session(auth=auth)
        nova = client.Client(2, session=sess)
        return nova

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

        return [self._get_one(client, aggregate_id)] \
            if aggregate_id is not None else self._get_all(client)

    def _get_aggregate(self, client, aggregate_id):
        try:
            return client.aggregates.get(aggregate_id)
        except exceptions.NotFound:
            raise ha_exceptions.AggregateNotFound(aggregate_id)

    def _check_if_host_in_other_cluster(self, hosts):
        clusters = db_api.get_all_active_clusters()
        invalid_hosts = set()
        valid_hosts = set()
        client = self._get_client()
        for cluster in clusters:
            aggregate = self._get_aggregate(client, cluster.name)
            cluster_hosts = set(aggregate.hosts)
            for host in hosts:
                if host in cluster_hosts:
                    invalid_hosts.add(host)
                else:
                    valid_hosts.add(host)
        LOG.debug("Valid Hosts: %s", str(valid_hosts))
        LOG.debug("Invalid Hosts: %s", str(invalid_hosts))
        if invalid_hosts:
            LOG.error("Host(s) %s are part of another cluster",
                      str(invalid_hosts))
            raise ha_exceptions.HostPartOfCluster(str(invalid_hosts))

    def _validate_hosts(self, hosts, check_cluster=True):
        # TODO Make this check configurable
        # Since the consul needs 3 to 5 servers for bootstrapping, it is safe
        # to enable HA only if 4 hosts are present. So that even if 1 host goes
        # down after cluster is created, we can reconfigure it.
        if len(hosts) < 4:
            raise ha_exceptions.InsufficientHosts()

        LOG.debug("check host in other cluster : %s", str(check_cluster))
        if check_cluster is True:
            self._check_if_host_in_other_cluster(hosts)

        # Check host state and role status in resmgr before proceeding
        current_roles = {}
        json_resp = self._wait_for_role_to_ok(hosts)
        for host in json_resp:
            if host.get('role_status', '') != 'ok':
                LOG.warn('Role status of host %s is not ok, not enabling HA '
                         'at the moment.', host['id'])
                raise ha_exceptions.InvalidHostRoleStatus(host['id'])
            if host['info']['responding'] is False:
                LOG.warn('Host %s is not responding, not enabling HA at the '
                         'moment.', host['id'])
                raise ha_exceptions.HostOffline(host['id'])
            current_roles[host['id']] = host['roles']
        LOG.debug("current roles : %s , from resp : %s", str(current_roles),
                  json.dumps(json_resp))
        return current_roles

    def _auth(self, ip_lookup, cluster_ip_lookup, token, nodes, role, ip=None):
        assert role in ['server', 'agent']
        url = 'http://localhost:8080/resmgr/v1/hosts/'
        headers = {'X-Auth-Token': token['id'],
                   'Content-Type': 'application/json'}
        for node in nodes:
            start_time = datetime.now()
            LOG.info('Authorizing pf9-ha-slave role on node %s using IP %s',
                     node, ip_lookup[node])
            ips = ','.join([str(v) for v in cluster_ip_lookup.values()])
            LOG.info('ips for consul members to join : %s', ips)
            data = dict(join=ips, ip_address=ip_lookup[node],
                        cluster_ip=cluster_ip_lookup[node])
            data['bootstrap_expect'] = 3 if role == 'server' else 0
            auth_url = '/'.join([url, node, 'roles', 'pf9-ha-slave'])
            resp = requests.put(auth_url, headers=headers,
                                json=data, verify=False)
            if resp.status_code == requests.codes.not_found and \
                    resp.content.find('HostDown'):
                raise ha_exceptions.HostOffline(node)
            # Retry auth if resmgr throws conflict error for upto 2 minutes
            while resp.status_code == requests.codes.conflict:
                LOG.info('Role conflict error for node %s, retrying after 5 '
                         'sec', node)
                time.sleep(5)
                resp = requests.put(auth_url, headers=headers,
                                    json=data, verify=False)
                if datetime.now() - start_time > timedelta(minutes=2):
                    break
            resp.raise_for_status()

    def _fetch_role_details_for_host(self, host_id, host_roles):
        roles = filter(lambda x: x.startswith('pf9-ostackhost'), host_roles)
        if len(roles) != 1:
            raise ha_exceptions.InvalidHypervisorRoleStatus(host_id)
        rolename = roles[0]
        # Query consul_ip from resmgr ostackhost role settings
        self._token = utils.get_token(self._auth_uri_v3,
                                      self._tenant, self._username,
                                      self._passwd, self._token)
        headers = {'X-Auth-Token': self._token['id'],
                   'Content-Type': 'application/json'}
        resmgr_url = 'http://localhost:8080/resmgr/v1/hosts/'
        host_url = '/'.join([resmgr_url, host_id, 'roles', rolename])
        resp = requests.get(host_url, headers=headers)
        resp.raise_for_status()
        json_resp = resp.json()
        LOG.debug("role details for host %s : %s", str(host_id),
                  json.dumps(json_resp))
        return json_resp

    def _get_cluster_ip(self, host_id, json_resp):
        if 'cluster_ip' not in json_resp:
            raise ha_exceptions.ClusterIpNotFound(host_id)
        return str(json_resp['cluster_ip'])

    def _get_consul_ip(self, host_id, json_resp):
        if 'consul_ip' in json_resp and json_resp['consul_ip']:
            return str(json_resp['consul_ip'])
        return self._get_cluster_ip(host_id, json_resp)

    def _get_ips(self, client, nodes, current_roles):
        all_hypervisors = client.hypervisors.list()
        lookup = set(nodes)
        ip_lookup = dict()
        cluster_ip_lookup = dict()
        for hyp in all_hypervisors:
            host_id = hyp.service['host']
            if host_id in lookup:
                ip_lookup[host_id] = hyp.host_ip
                # Overwrite host_ip value with consul_ip or cluster_ip
                # from ostackhost role
                json_resp = self._fetch_role_details_for_host(
                    host_id, current_roles[host_id])
                consul_ip = self._get_consul_ip(host_id, json_resp)
                if consul_ip:
                    LOG.info('Using consul ip %s from ostackhost role',
                              consul_ip)
                    ip_lookup[host_id] = consul_ip
                cluster_ip = self._get_cluster_ip(host_id, json_resp)
                LOG.info('Using cluster ip %s from ostackhost role',
                          cluster_ip)
                cluster_ip_lookup[host_id] = cluster_ip
        return ip_lookup, cluster_ip_lookup

    def _assign_roles(self, client, hosts, current_roles):
        hosts = sorted(hosts)
        leader = hosts[0]
        servers = hosts[1:4]

        agents = []
        if len(hosts) >= 5:
            servers += hosts[4:5]
            agents = hosts[5:]

        ip_lookup, cluster_ip_lookup = self._get_ips(client, hosts,
                                                     current_roles)
        if leader not in ip_lookup:
            LOG.error('Leader %s not found in nova', leader)
            raise ha_exceptions.HostNotFound(leader)

        leader_ip = ip_lookup[leader]

        self._token = utils.get_token(self._auth_uri_v3,
                                      self._tenant, self._username,
                                      self._passwd, self._token)
        self._auth(ip_lookup, cluster_ip_lookup, self._token,
                   [leader] + servers, 'server', ip=leader_ip)
        self._auth(ip_lookup, cluster_ip_lookup, self._token, agents, 'agent',
                   ip=leader_ip)

    def __perf_meter(self, method, time_start):
        time_end = datetime.utcnow()
        LOG.debug('[metric] "%s" call : %s (start : %s , '
                  'end : %s)', str(method), str(time_end - time_start),
                  str(time_start), str(time_end))

    def _enable(self, aggregate_id, hosts=None,
                next_state=states.TASK_COMPLETED):
        """Enable HA

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
        cluster_id = None
        try:
            cluster = db_api.get_cluster(str_aggregate_id)
            cluster_id = cluster.id
        except ha_exceptions.ClusterNotFound:
            pass
        except Exception:
            LOG.exception('error when get cluster %s from db',
                          str(str_aggregate_id))
            raise
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

        # make sure no host exists in multiple host aggregation
        aggregate = self._get_aggregate(client, aggregate_id)
        if not hosts:
            hosts = aggregate.hosts
        self._validate_hosts(hosts)

        try:
            cluster = db_api.create_cluster_if_needed(str_aggregate_id,
                                                      states.TASK_CREATING)
            # set the status to 'request-enable'
            db_api.update_request_status(cluster.id, constants.HA_STATE_REQUEST_ENABLE)
            # publish status
            self._notify_status(constants.HA_STATE_REQUEST_ENABLE, "cluster", cluster.id)
        except Exception as e:
            print str(e)
            db_api.update_cluster_task_state(cluster_id, next_state)
            LOG.error('unhandled exception : %s' , str(e))


    def _handle_enable_request(self, request, hosts=None, next_state=states.TASK_COMPLETED):
        if not request:
            return
        client = self._get_client()
        str_aggregate_id = request.name
        cluster_id = request.id
        time_begin = datetime.utcnow()
        self.__perf_meter('db_api.get_cluster', time_begin),
        time_begin = datetime.utcnow()
        aggregate = self._get_aggregate(client, str_aggregate_id)
        self.__perf_meter('_get_aggregate', time_begin)
        if not hosts:
            hosts = aggregate.hosts
        else:
            LOG.info('Enabling HA on some of the hosts %s of the %s aggregate',
                     str(hosts), str_aggregate_id)
        time_begin = datetime.utcnow()
        current_roles = self._validate_hosts(hosts)
        self.__perf_meter('_validate_hosts', time_begin)
        time_begin = datetime.utcnow()
        self._token = utils.get_token(self._auth_uri_v3,
                                      self._tenant, self._username,
                                      self._passwd, self._token)
        self.__perf_meter('utils.get_token', time_begin)
        try:
            # 1. mark request as 'enabling' in status
            LOG.info('updating status of cluster %d to %s', cluster_id, constants.HA_STATE_ENABLING)
            time_begin = datetime.utcnow()
            db_api.update_request_status(cluster_id, constants.HA_STATE_ENABLING)
            self._notify_status(constants.HA_STATE_ENABLING, "cluster", cluster_id)
            self.__perf_meter('db_api.update_enable_or_disable_request_status', time_begin)

            # 2. Push roles
            time_begin = datetime.utcnow()
            self._assign_roles(client, hosts, current_roles)
            self.__perf_meter('_assign_roles', time_begin)

            # 3. Create masakari fail-over segment
            time_begin = datetime.utcnow()
            masakari.create_failover_segment(self._token, str(cluster_id),
                                             hosts)
            self.__perf_meter('masakari.create_failover_segment', time_begin)

            # 4. mark request as 'enabled' in status
            LOG.info('updating status of cluster %d to %s', cluster_id,
                     constants.HA_STATE_ENABLED)
            time_begin = datetime.utcnow()
            db_api.update_request_status(cluster_id,
                                         constants.HA_STATE_ENABLED)
            self._notify_status(constants.HA_STATE_ENABLED, "cluster", cluster_id)
            self.__perf_meter('db_api.update_enable_or_disable_request_status', time_begin)

            # 5. finally mark the cluster as enabled
            LOG.debug('update flag enabled to : %s', str(True))
            time_begin = datetime.utcnow()
            db_api.update_cluster(cluster_id, True)
            self.__perf_meter('db_api.update_cluster', time_begin)
        except Exception as e:
            time_begin = datetime.utcnow()
            LOG.exception('Cannot enable HA on %s: %s, performing cleanup by '
                      'disabling', cluster_id, e)

            if cluster_id is not None:
                # mark the request status to 'error'
                db_api.update_request_status(cluster_id,
                                             constants.HA_STATE_ERROR)
                self._notify_status(constants.HA_STATE_ERROR, "cluster", cluster_id)
                # mark task as completed
                db_api.update_cluster_task_state(cluster_id,
                                                 states.TASK_COMPLETED)

            # rollback above steps
            self._handle_disable_request(request)

            # if rollbacks succeeded , then flag 'enabled' as false
            if cluster_id is not None:
                try:
                    db_api.update_cluster(cluster_id, False)
                except exceptions.ClusterNotFound:
                    pass
            raise
        else:
            if cluster_id is not None:
                time_begin = datetime.utcnow()
                db_api.update_cluster_task_state(cluster_id, next_state)
                self.__perf_meter('db_api.update_cluster_task_state', time_begin)

    def _wait_for_role_to_ok(self, nodes):
        self._token = utils.get_token(self._auth_uri_v3,
                                      self._tenant, self._username,
                                      self._passwd, self._token)
        headers = {'X-Auth-Token': self._token['id'],
                   'Content-Type': 'application/json'}
        url = 'http://localhost:8080/resmgr/v1/hosts/'
        json_resp = []
        for node in nodes:
            start_time = datetime.now()
            auth_url = '/'.join([url, node])
            resp = requests.get(auth_url, headers=headers)
            resp.raise_for_status()
            while resp.json().get('role_status', '') != 'ok':
                time.sleep(30)
                resp = requests.get(auth_url, headers=headers)
                resp.raise_for_status()
                if datetime.now() - start_time > timedelta(minutes=15):
                    LOG.info(
                        "host %s is not converged, starting from %s to %s ",
                        str(node), str(start_time), str(datetime.now()))
                    raise ha_exceptions.RoleConvergeFailed(node)
            json_resp.append(resp.json())
        return json_resp

    def _deauth(self, nodes):
        self._token = utils.get_token(self._auth_uri_v3,
                                      self._tenant, self._username,
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
        """Disale HA

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
            LOG.warn('cluster %s can not be found for disable',
                     str_aggregate_id)
            pass

        if not cluster:
            LOG.warn('no cluster with id %s', str_aggregate_id)
            return

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

        try:
            db_api.update_cluster_task_state(cluster.id, states.TASK_REMOVING)
            # mark the status as 'request-disable' for processing
            db_api.update_request_status(cluster.id, constants.HA_STATE_REQUEST_DISABLE)
            self._notify_status(constants.HA_STATE_REQUEST_DISABLE, "cluster", cluster.id)
        except Exception as e:
            LOG.error('unhandled exceptions : %s ', str(e))

    def ha_enable_disable_request_processing(self):
        with self.ha_status_processing_lock:
            if self.ha_status_processing_running:
                LOG.debug('ha status processing task is already running')
                return
            self.ha_status_processing_running = True

        try:
            LOG.debug('ha status thread is checking status')
            requests = db_api.get_all_unhandled_enable_or_disable_requests()
            for request in requests:
                if request.status == constants.HA_STATE_REQUEST_ENABLE:
                    self._handle_enable_request(request)
                if request.status == constants.HA_STATE_REQUEST_DISABLE:
                    self._handle_disable_request(request)
        finally:
            with self.ha_status_processing_lock:
                self.ha_status_processing_running = False

    def _handle_disable_request(self, request, next_state=states.TASK_COMPLETED):
        LOG.info('handling disable request : %s', str(request))
        cluster = request
        # the name used to query db needs to be string, not int
        aggregate_name = str(cluster.id)
        try:
            self._token = utils.get_token(self._auth_uri_v3,
                                          self._tenant, self._username,
                                          self._passwd, self._token)
            hosts = None
            try:
                if cluster:
                    # If aggregate has changed, we need to remove role from
                    # previous segment's nodes
                    nodes = masakari.get_nodes_in_segment(self._token,
                                                          aggregate_name)
                    hosts = [n['name'] for n in nodes]
                else:
                    # If cluster was not even created, but we are disabling HA
                    # to rollback enablement
                    client = self._get_client()
                    aggregate = self._get_aggregate(client, aggregate_name)
                    hosts = set(aggregate.hosts)
            except ha_exceptions.SegmentNotFound:
                LOG.warn('Masakari segment for cluster: %s was not found, '
                         'skipping deauth', aggregate_name)

            # change status from 'request-disable' to 'disabling'
            db_api.update_request_status(cluster.id
                                         , constants.HA_STATE_DISABLING)
            self._notify_status(constants.HA_STATE_DISABLING, "cluster", cluster.id)
            if hosts:
                self._deauth(hosts)
                self._wait_for_role_to_ok(hosts)
            else:
                LOG.info('no hosts found in segment %s', str(aggregate_name))

            masakari.delete_failover_segment(self._token, str(aggregate_name))
            # change status from 'disabling' to 'disabled'
            db_api.update_request_status(cluster.id
                                         , constants.HA_STATE_DISABLED)
            self._notify_status(constants.HA_STATE_DISABLED, "cluster", cluster.id)
        except Exception:
            if cluster:
                # mark the status as 'error'
                db_api.update_request_status(cluster.id
                                             , constants.HA_STATE_ERROR)
                self._notify_status(constants.HA_STATE_ERROR, "cluster", cluster.id)
                db_api.update_cluster_task_state(cluster.id,
                                                 states.TASK_ERROR_REMOVING)
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
                if not self._is_nova_service_active(current_host,
                                                    client=client):
                    if current_host in db_node_ids and \
                            current_host not in \
                            self.hosts_down_per_cluster[cluster.id]:
                        self.hosts_down_per_cluster[cluster.id][
                            current_host] = False
                else:
                    if current_host in self.hosts_down_per_cluster[cluster.id]:
                        self.hosts_down_per_cluster.pop(current_host)

            self.hosts_down_per_cluster[cluster.id][host] = True
            if all([v for k, v in self.hosts_down_per_cluster[
                cluster.id].items()]):
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
        except Exception as e:
            LOG.exception(
                'Could not process {host} host down. HA is not enabled '
                'because of this error : {error}'.format(
                    host=host, error=e))
        db_api.update_cluster_task_state(cluster.id, states.TASK_COMPLETED)

    def host_down(self, event_details):
        host = event_details['hostname']
        try:
            # report host down event
            retval = self._report_event(constants.EVENT_HOST_DOWN,
                                        event_details)
        except Exception:
            LOG.exception('Error processing %s host down', host)
            retval = False
        return retval

    def host_up(self, event_details):
        host = event_details['hostname']
        try:
            # also report host up event
            self._report_event(constants.EVENT_HOST_UP, event_details)

            if self._is_nova_service_active(host):
                # When the cluster was reconfigured for host down event this
                # node is removed from masakari. Hence generating a host up
                # notification will result in 404. The node will be added back
                # in the cluster with the next periodic task run.
                return True
            # No point in adding the node back if nova-compute is down
            return False
        except Exception:
            LOG.exception('failed to process host up event')
            return False
        return True

    def _report_event(self, event_type, event_details):
        if not event_type or not event_details or 'host_id' not in \
                event_details:
            return False

        host_name = event_details['host_id']

        try:
            LOG.debug('prepare to write event, host %s, event %s , '
                      'details %s',
                      host_name, event_type, event_details)
            # find the cluster id from given host id
            clusters = db_api.get_all_active_clusters()
            LOG.debug('all active clusters : %s', str(clusters))
            if clusters is None:
                LOG.warn('unable to get active clusters to write event : %s',
                         str(event_details))
                return False
            client = self._get_client()
            target_cluster_id = None
            for cluster in clusters:
                aggregate_id = cluster.name
                aggregate = self._get_aggregate(client, aggregate_id)
                hosts = set(aggregate.hosts)
                if str(host_name) in hosts:
                    target_cluster_id = cluster.id
                    break
            if not target_cluster_id:
                LOG.info('unable to find cluster for host %s to write event',
                          host_name)
                return False

            # suppress same events if it has been reported before within
            # given threshold seconds
            end_time = datetime.utcnow()
            start_time = end_time - timedelta(
                seconds=self._event_report_threshold_seconds)
            existing_changes = db_api.get_change_events_between_times(
                target_cluster_id,
                host_name,
                event_type,
                start_time,
                end_time)
            if existing_changes and len(existing_changes) > 0:
                LOG.warn('ignore current event, as same change event has been '
                         'reported '
                         'within % '
                         'seconds', self._event_report_threshold_seconds)
                return

            # write change event into db
            change_record = db_api.create_change_event(target_cluster_id,
                                                       event_details)
            LOG.info('change event record is created for host %s in cluster '
                      '%s', host_name, target_cluster_id)

            # similarly, suppress events that has been recorded within given
            # threshold seconds to avoid flooding
            end_time = datetime.utcnow()
            start_time = end_time - timedelta(
                seconds=self._event_report_threshold_seconds)
            existing_events = db_api.get_processing_events_between_times(
                event_type,
                host_name,
                target_cluster_id,
                start_time,
                end_time)
            if existing_events and len(existing_events) > 0:
                LOG.warn('ignore current event, as same processing event has '
                         'been reported within % seconds',
                         self._event_report_threshold_seconds)
                return

            # write events processing record
            event_uuid = change_record.uuid
            processing_record = db_api.create_processing_event(event_uuid,
                                                               event_type,
                                                               host_name,
                                                               target_cluster_id)
            LOG.info('event processing record is created for host %s on '
                      'event %s in cluster %s', host_name, event_type,
                      target_cluster_id)
            return True
        except Exception:
            LOG.exception('failed to record event')
        return False

    def _notify_status(self, action, target, identifier):
        obj = ha_notification.Notification(action, target, identifier)
        publish(obj)

def get_provider(config):
    db_api.init(config)
    return NovaProvider(config)
