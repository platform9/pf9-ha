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
import threading
import time
from random import randint
from itertools import combinations
from collections import defaultdict
from datetime import datetime
from datetime import timedelta
from uuid import uuid4

import requests
from keystoneauth1 import loading
from keystoneauth1 import session
from novaclient import client
from novaclient import exceptions

import hamgr
from hamgr.common import masakari
from hamgr.common import utils
from hamgr.db import api as db_api
from hamgr.notification import get_notification_manager
from hamgr.providers.provider import Provider
from hamgr.rebalance import get_rebalance_controller
from shared import constants
from shared.exceptions import ha_exceptions
from shared.messages.rebalance_request import ConsulRoleRebalanceRequest
from shared.messages.cluster_event import ClusterEvent
from shared.messages.consul_request import ConsulRefreshRequest
from hamgr.common import key_helper as keyhelper
from hamgr.resmgr_client import ResmgrClient
from shared.constants import LOGGER_PREFIX
from six.moves.configparser import ConfigParser

LOG = logging.getLogger(LOGGER_PREFIX + __name__)

AMQP_HOST_QUEUE_PREFIX = 'queue-receiving-for-host'
AMQP_HTTP_PORT = 15672

VMHA_MAX_FANOUT = 3

NOVA_REQ_TIMEOUT = 5

class NovaProvider(Provider):
    def __init__(self, config):
        self._username = config.get('keystone_middleware', 'username')
        self._passwd = config.get('keystone_middleware', 'password')
        self._auth_url = config.get('keystone_middleware', 'auth_url')
        self._tenant = config.get('keystone_middleware', 'project_name')
        self._region = config.get('nova', 'region')
        self._bbmaster_host = config.get('DEFAULT', 'bbmaster_host')
        self._resmgr_endpoint = config.get('DEFAULT', 'resmgr_endpoint')

        self._resmgr_client = ResmgrClient(self._resmgr_endpoint)
        self._max_auth_wait_seconds = 300
        self._consul_bootstrap_expect = 3
        self._amqp_user = config.get("amqp", "username")
        self._amqp_password = config.get("amqp", "password")
        self._amqp_host = config.get("amqp", "host")
        self._amqp_mgmt_host = config.get("amqp", "mgmt_host")
        self._mdb_uri = config.get("masakari", "sqlconnectURI")
        self._db_uri = config.get("database", "sqlconnectURI")
        self._db_pwd = self._db_uri
        self._hamgr_config = '/etc/pf9/hamgr/hamgr.conf'
        self._hypervisor_details = ""
        parser = ConfigParser()
        with open(self._hamgr_config) as fp: 
            parser.readfp(fp)
        self._du_name = parser.get('DEFAULT', 'customer_shortname')
        if self._db_uri:
            idx = self._db_uri.find('@')
            if idx > 0:
                self._db_pwd = self._db_uri[0:idx]
                idx = self._db_pwd.rfind(':')
                if idx > 0:
                    self._db_pwd = self._db_pwd[idx+1:]
        try:
            self._max_auth_wait_seconds = config.getint('DEFAULT', 'max_role_auth_wait_seconds')
        except:
            LOG.warning("'max_role_auth_wait_seconds' not exist in config file " \
                     "or its value not integer, use default 300")
        if self._max_auth_wait_seconds <= 0:
            self._max_auth_wait_seconds = 300
        self._max_role_converged_wait_seconds = 900
        try:
            self._max_role_converged_wait_seconds = config.getint('DEFAULT', 'max_role_converged_wait_seconds')
        except:
            LOG.warning("'max_role_converged_wait_seconds' not exist in config file " \
                     "or its value not integer, use default 900")
        if self._max_role_converged_wait_seconds <= 0:
            self._max_role_converged_wait_seconds = 900
        self._token = None
        self._config = config
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
        LOG.debug('masakari notification waiting timeout is set to %s '
                 'minutes', str(self._notification_stale_minutes))

        # to suppress possible too much host events in the case where
        # slave failed to report the event to hamgr du to network issue,
        # use the configured threshold seconds to check whether similar event
        # has been reported, if not then record down, otherwise ignore it
        self._event_report_threshold_seconds = 30
        setting_seconds = config.getint('DEFAULT',
                                        'event_report_threshold_seconds')
        if setting_seconds <= 0:
            raise ha_exceptions.ConfigException('invalid setting in configuration file : '
                                                'event_report_threshold_seconds '
                                                'should be bigger than 0')
        else:
            self._event_report_threshold_seconds = setting_seconds
        LOG.debug('event report threshold seconds is %s',
                 str(self._event_report_threshold_seconds))
        self.hosts_down_per_cluster = defaultdict(dict)
        self.availability_zone_task_lock = threading.Lock()
        self.availability_zone_task_running = False
        self.host_down_dict_lock = threading.Lock()
        # thread lock
        self.events_processing_lock = threading.Lock()
        self.events_processing_running = False
        # ha status thread lock
        self.ha_status_processing_lock = threading.Lock()
        self.ha_status_processing_running = False
        # lock for consul role rebalance processing
        self.consul_role_rebalance_processing_lock = threading.Lock()
        self.consul_role_rebalance_processing_running = False
        # lock for consul encryption change processing
        self.consul_encryption_processing_lock = threading.Lock()
        self.consul_encryption_processing_running = False
        self.queue_processing_lock = threading.Lock()
        self.queue_processing_running = False

    def _get_v3_token(self):
        self._token = utils.get_token(self._auth_url,
                                      self._tenant, self._username,
                                      self._passwd, self._token, self._region)
        return self._token

    def _is_consul_encryption_enabled(self):
        enable = self._config.getboolean('DEFAULT',
                                         'enable_consul_encryption') \
            if self._config.has_option("DEFAULT", "enable_consul_encryption") \
            else False
        return enable

    def _get_active_azs(self, token):
        nova_active_azs = []
        azInfo = {}
        headers = {"X-AUTH-TOKEN": token}
        url = 'http://nova-api.' + self._du_name + '.svc.cluster.local:8774/v2.1/os-availability-zone/detail'
        response = requests.get(url, headers=headers)
        if response.status_code == 200:
            azInfo = response.json()
            for az in azInfo['availabilityZoneInfo']:
                if az['zoneName'] == 'internal':
                    continue
                nova_active_azs.append(az['zoneName'])
        return nova_active_azs, azInfo
   
    def _get_nova_hosts(self, token, az):
        nova_hosts = []
        headers = {"X-AUTH-TOKEN": token}
        url = 'http://nova-api.' + self._du_name + '.svc.cluster.local:8774/v2.1/os-services'
        response = requests.get(url, headers=headers)
        if response.status_code == 200:
            data = response.json()
            for svc in data['services']:
                if svc['zone'] == az:
                    nova_hosts.append(svc['host'])
        return nova_hosts
 
    # thread is dedicated to handle recorded host events
    def process_host_events(self):
        # wait until current task complete
        with self.events_processing_lock:
            if self.events_processing_running:
                LOG.debug('events processing task is already running')
                return
            self.events_processing_running = True
        LOG.debug('host events processing task starts to run at %s',
                  str(datetime.utcnow()))
        try:
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
                LOG.debug('no host events found')
                return
            events = sorted(unhandled_events, key=lambda x: x.event_time)
            if not events:
                LOG.debug('no host events to process')
                return
            LOG.info('found host events : %s', str(events))

            self._token = self._get_v3_token()
            nova_client = self._get_nova_client()
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
                    LOG.warning('cluster %s in event %s is not enabled or does not exist',
                                event.cluster_id, str(event_uuid))
                    db_api.update_processing_event_with_notification(
                        event_uuid, None, None,
                        constants.STATE_ABORTED,
                        'cluster is no longer enabled', update_all = True)
                    continue
                LOG.debug('event %s: found ha cluster with id %s : %s', event_uuid,
                          event.cluster_id, str(cluster))
                nova_active_azs, azInfo = self._get_active_azs(self._token['id'])
                if not nova_active_azs:
                    LOG.error('Error getting availability zone info, will retry')
                    return
                # if no such availability zone, no need to handle it
                if not cluster.name in nova_active_azs:
                    LOG.warning('no availability zone found with name %s for event %s',
                                str(cluster.name), str(event_uuid))
                    continue
                # if host in the event not exist in availability zone, no need to
                # handle the event
                nova_hosts = self._get_nova_hosts(self._token['id'], cluster.name)
                if not nova_hosts:
                    LOG.error('Error getting hosts in availability zone %s, will retry', cluster.name)
                    return
                if host_name not in nova_hosts:
                    LOG.warning('host %s in event %s does not exist in availability zone %s',
                                host_name, str(event_uuid), str(nova_hosts))
                    db_api.update_processing_event_with_notification(
                        event_uuid, None, None,
                        constants.STATE_ABORTED,
                        'event host is no longer part of cluster')
                    continue

                # when same event type for the same host was reported
                # within short time, means they are duplicated, this
                # could happen when consul cluster switch leader, already
                # threshold them when receive them, but if this process happens
                # within very short time, because of this hamgr tries to check
                # whether similar ones exist will fail, as db has not flushed.
                # so we do similar check to skip duplicated report.
                end_time = datetime.utcnow()
                start_time = end_time - timedelta(seconds=self._event_report_threshold_seconds)
                existing_events = db_api.get_processing_events_between_times(
                        event_type,
                        host_name,
                        event.cluster_id,
                        start_time,
                        end_time)

                if existing_events:
                    ids = [x.event_uuid for x in existing_events]
                    past_events = list(set(ids).difference(set([event_uuid])))
                    msg = 'similar events have happened within %s seconds : %s' % (
                        str(self._event_report_threshold_seconds),
                        str(past_events))
                    if past_events:
                        LOG.debug('event %s: %s', str(event_uuid), msg)
                        db_api.update_processing_event_with_notification(
                                event_uuid, None, None,
                                constants.STATE_DUPLICATED,
                                msg
                        )
                        LOG.warning('event %s is aborted, as %s', str(event_uuid), str(msg))
                        continue

                # if the event happened long time ago, just abort
                if datetime.utcnow() - event_time > time_out:
                    if event.notification_uuid and event.notification_status != constants.STATE_FINISHED:
                        state = masakari.get_notification_status(self._token, event.notification_uuid)
                        LOG.info('event %s is in state : %s , corresponding masakari notification state : %s',
                                 str(event_uuid), str(event.notification_status), str(state))
                    db_api.update_processing_event_with_notification(
                        event_uuid, None, None,
                        constants.STATE_ABORTED,
                        'event happened long time ago : %s' % str(event_time)
                    )
                    LOG.warning('event %s is aborted from state %s, as it is too stale',
                             str(event_uuid), str(event.notification_status))
                    continue

                # check whether host is active in nova
                LOG.debug('try to check whether host %s in nova is active ', str(host_name))
                is_active = self._is_nova_service_active(host_name, nova_client=nova_client)
                LOG.debug('is host %s active in nova ? %s', host_name, str(is_active))

                # for 'host-down' event
                if event_type == constants.EVENT_HOST_DOWN:
                    if not event.notification_uuid:
                        if is_active:
                            LOG.info('Still reporting host_down event to masakari, host %s is alive in nova',
                                     host_name)
                            #continue

                        # when masakari received host-down notification, the host in masakari will be set on maintenance
                        # if a host in masakari is marked as on maintenance, it will give 409 error if you try to
                        # create another host-down notification for it. we have another task to reset the maintenance
                        # status when the masakari failover segment is not under recovery. so we can wait that complete
                        # here before trying to create another notification. this will avoid the 409 error.
                        #
                        if not masakari.is_failover_segment_under_recovery(self._token, cluster.name):
                            LOG.info('in event %s %s for host %s, try to reset maintenance state in masakari '
                                     'which is active in nova', event_type, event_uuid, host_name)
                            self._toggle_masakari_hosts_maintenance_status(segment_name=cluster.name,
                                                                           host_ids=[host_name],
                                                                           ignore_status_in_nova=True)
                            masakari_host_on_maintenance = masakari.is_host_on_maintenance(self._token, host_name,
                                                                                           cluster.name)
                            if masakari_host_on_maintenance:
                                LOG.warning('%s event %s for host %s, which is in masakari segment %s '
                                            'is on maintenance', event_type, event_uuid,
                                            host_name, cluster.name)
                        LOG.debug('Reporting event %s id %s for host %s to masakari',
                                  str(event_type), str(event_uuid), host_name)
                        notification_obj = self._report_event_to_masakari(event)
                        if notification_obj:
                            LOG.info('Host-down event %s for host %s is reported to masakari, notification id %s',
                                     event_uuid, host_name, str(notification_obj))
                            event.notification_uuid = notification_obj['notification']['notification_uuid']
                            state = self._tracking_masakari_notification(event)
                            LOG.info('event %s for host %s is updated with notification state %s',
                                     event_uuid, host_name, state)
                        else:
                            LOG.error('failed to report %s event %s for host %s to masakari : %s',
                                      event_type, event_uuid, host_name, str(event))
                    else:
                        state = self._tracking_masakari_notification(event)
                        LOG.info('Host-down event %s for host %s is updated with notification state %s',
                                 event_uuid, host_name, state)
                elif event_type == constants.EVENT_HOST_UP:
                    if is_active:
                        if not masakari.is_failover_segment_under_recovery(self._token, cluster.name):
                            LOG.info('in event %s %s for host %s, try to reset maintenance state in masakari '
                                     'which is active in nova', event_type, event_uuid, host_name)
                            self._toggle_masakari_hosts_maintenance_status(segment_name=cluster.name,
                                                                           host_ids=[host_name])
                        else:
                            LOG.info('retry toggle maintence state for host %s as cluster %s is in recovery',
                                     str(host_name), str(cluster.name))

                        db_api.update_processing_event_with_notification(event_uuid,
                                                                         None,
                                                                         None,
                                                                         constants.STATE_FINISHED)
                        LOG.info('Host-up event %s for host %s is marked as %s', event_uuid,
                                 host_name, constants.STATE_FINISHED)
                    else:
                        # when there is host-up, but actual nova status is down
                        # there should be a host-down event soon, since the
                        # pf9-slave has a grace period of time to report events
                        # so here just wait the new host-down event, or just
                        # wait until this event becomes stale
                        LOG.info('event %s is host-up event at %s but host '
                                 'is not active in nova now %s', event_uuid,
                                 event_time, datetime.utcnow())
                LOG.debug('end of handling event %s', event_uuid)
        except Exception:
            LOG.exception('unhandled exception when processing host events')
        finally:
            # release thread lock
            with self.events_processing_lock:
                self.events_processing_running = False
        LOG.debug('host events processing task has finished at %s', str(datetime.utcnow()))

    def _report_event_to_masakari(self, event):
        if not event:
            return
        notification_obj = None
        try:
            self._token = self._get_v3_token()
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
            if notification_obj and notification_obj.get('notification', None):
                return notification_obj
        except Exception:
            LOG.error('failed to create masakari notification, error', exc_info=True)
        LOG.warning('no valid masakari notification was created for event %s %s for host %s: %s',
                    event.event_type, event.event_uuid, event.host_name, str(notification_obj))
        return None

    def _tracking_masakari_notification(self, event):
        if not event:
            return None
        self._token = self._get_v3_token()
        notification_uuid = event.notification_uuid
        state = masakari.get_notification_status(self._token, notification_uuid)
        # save the state to event
        LOG.info('updating event %s %s with masakari notification %s status : %s',
                 event.event_type, event.event_uuid, notification_uuid, str(state))
        db_api.update_processing_event_with_notification(event.event_uuid,
                                                         event.notification_uuid,
                                                         event.notification_created,
                                                         state)
        return state

    def process_queue_for_unauthed_hosts(self):
        with self.queue_processing_lock:
            if self.queue_processing_running:
                LOG.debug('Queue processing is already running')
                return
            self.queue_processing_running = True

        LOG.debug('queue processing task starts to run at %s',
                  str(datetime.utcnow()))
        self._token = self._get_v3_token()
        resmgr_hosts = self._resmgr_client.get_hosts_info(self._token['id'])
        for host in resmgr_hosts:
            if (not self.availability_zone_task_running and
                    not self.consul_role_rebalance_processing_running and
                    "pf9-ha-slave" not in host["roles"]):
                req_url = 'http://{0}:{1}@{2}:{3}/api/queues/%2f/{4}-{5}' \
                    .format(self._amqp_user, self._amqp_password,
                            self._amqp_mgmt_host, AMQP_HTTP_PORT,
                            AMQP_HOST_QUEUE_PREFIX, host["id"])
                resp = requests.get(req_url)
                if resp.status_code == requests.codes.ok:
                    resp = requests.delete(req_url)
                    if resp.status_code not in (requests.codes.no_content,
                                                requests.codes.not_found):
                        LOG.warning('Failed to delete rabbitmq queue for host '
                                    '%s on role deauth: %s', host["id"],
                                    resp.status_code)

    def process_availability_zone_changes(self):
        try:
            with self.availability_zone_task_lock:
                if self.availability_zone_task_running:
                    LOG.debug('Check host availability_zones for changes task already running')
                    return
                self.availability_zone_task_running = True

            LOG.debug('host availability_zone change processing task starts to run at %s', str(datetime.utcnow()))

            # ---------------------------------------------------------------
            # scenarios
            #  1) nova availability_zone exists   [ sync from nova availability_zone to ha system ]
            #     1.1) corresponding ha cluster exists
            #        1.1.1) masakari (failover segment, hosts) does not match hosts from nova availability_zone
            #             X = {hosts in masakari} , Y = {host in nova availability_zone}, Z = { common hosts in X and Y}
            #            1.1.1.1) X and Y have no common
            #                 remove all masakari hosts, then add nova availability_zone hosts to masakari
            #            1.1.1.2) X and Y have some in common
            #                 remove additional hosts (X-Z) from masakari, add additional host (Y-Z) to masakari
            #                 this also works for
            #                   - Y is subset of X : just add additional hosts to masakari
            #                   - X is subset of Y : just remove additional hosts from masakari
            #        1.1.2) masakari (failover segment, hosts) matches hosts from nova availability_zone
            #           no action needed
            #    1.2) corresponding ha cluster does not exist
            #        need to create a disabled ha cluster , and add masakari failover segment and hosts
            #  2) nova availability_zone does not exists  [ build nova availability_zones from ha system ]
            #     [ not sure whether this is valid scenario, because create nova availability_zones needs to know
            #      availability zone infomation , especially when there is no availability zone . ]
            # ----------------------------------------------------------
            # first reconcile hosts between nova availability_zones and masakari
            # to make sure real world settings on nova are synced to
            # masakari for vm evacuation management
            # ----------------------------------------------------------
            # find out all host availability_zones from nova

            self._token = self._get_v3_token()
            nova_client = self._get_nova_client()
            nova_active_azs, azInfo = self._get_active_azs(self._token['id'])
            if not nova_active_azs:
                LOG.error('Error getting availability zone info, will retry')
                return
            LOG.debug('active nova availability_zones : %s', nova_active_azs)
            hamgr_all_clusters = db_api.get_all_clusters()
            LOG.debug('all vmha clusters : %s', str(hamgr_all_clusters))

            # because hosts in availability zone reflect the real world settings for HA, so need to reconcile hosts in
            # each availability zone from nova to hamgr and masakari
            # for each active availability zone
            #  if there is corresponding ha cluster, then update hosts in masakari with hosts from this availability zone
            #  else create ha cluster in hamgr , and in masakari , but put the cluster in disabled state
            
            for az in azInfo['availabilityZoneInfo']:
                az_name = az['zoneName']
                if az['zoneName'] == 'internal':
                    continue
                az_hosts = self._get_nova_hosts(self._token['id'], az_name)
                if not az_hosts:
                    LOG.error('Error getting hosts in availability zone %s, will retry', az_name)
                    return
                # is there a ha cluster for az_name ?
                hamgr_clusters_for_az = [x for x in hamgr_all_clusters if x.name == az_name]
                if not hamgr_clusters_for_az:
                    LOG.debug('there is no matched vmha cluster with name matches to availability zone name %s',
                              az_name)
                    continue
                elif len(hamgr_clusters_for_az) > 1:
                    LOG.warning('there are more than 1 vmha clusters for availability zone %s : %s',
                                az_name, str(hamgr_clusters_for_az))
                LOG.info('%s host ids from availability zone %s: %s', str(len(az_hosts)),
                         str(az_name), str(az_hosts))

                # we only support one-to-one mapping between vmha cluster to availability zone
                current_cluster = hamgr_clusters_for_az[0]
                LOG.info('pick the first matched vmha cluster %s for availability zone name %s',
                         current_cluster.name, az_name)
                if current_cluster.status == 'request-enable': 
                   LOG.info('VMHA is not yet enabled for availability zone: %s not precessing it', current_cluster.name)                                 
                   continue  
                # reconcile hosts to hamgr and masakari
                cluster_name = current_cluster.name
                cluster_enabled = current_cluster.enabled
                cluster_status = current_cluster.status
                cluster_task_state = current_cluster.task_state

                # ideally the hosts from masakari is the same as hosts in availability zones
                # if more hosts in availability zone, means they exist in availability zone but not in masakari
                # if more hosts in masakari, means some of they are removed from availability zone

                masakari_hosts = []

                # remove host found in masakari segment but not in availability zone
                if masakari.is_failover_segment_exist(self._token, str(cluster_name)):
                    masakari_hosts = masakari.get_nodes_in_segment(self._token, str(cluster_name))
                masakari_hids = [x['name'] for x in masakari_hosts]
                common_ids = set(az_hosts).intersection(set(masakari_hids))
                masakari_delta_ids = set(masakari_hids) - set(common_ids)
                LOG.debug('cluster name  %s, masakari hids : %s, az name %s hids %s, common ids %s, '
                          'masakari additional hids %s, size:%s', str(cluster_name), str(masakari_hids),
                          str(az_name), str(az_hosts), str(common_ids),
                          str(masakari_delta_ids), str(len(masakari_delta_ids)))
                if len(masakari_delta_ids) > 0:
                    LOG.info('found hosts removed from availability zone name %s : %s',
                             str(az_name), str(masakari_delta_ids))
                    # need to remove those additional hosts found in masakari
                    if masakari.is_failover_segment_under_recovery(self._token, str(cluster_name)):
                        LOG.info('will retry to remove hosts %s from masakari segment %s, '
                                 'as the segment is under recovery',
                                 str(masakari_delta_ids), str(cluster_name))
                    else:
                        LOG.info('remove additional hosts %s found in masakari from segment %s ',
                                 str(masakari_delta_ids), str(cluster_name))
                        masakari.delete_hosts_from_failover_segment(self._token,
                                                                    str(cluster_name),
                                                                    masakari_delta_ids)
                        if cluster_enabled:
                            LOG.info('remove ha slave role from removed hosts : %s', str(masakari_delta_ids))
                            self._remove_ha_slave_if_exist(cluster_name, masakari_delta_ids, common_ids)
                            LOG.info('try to rebalance consul roles after removed hosts : %s',
                                     str(masakari_delta_ids))
                            time.sleep(30)
                            self._rebalance_consul_roles_if_needed(cluster_name, constants.EVENT_HOST_REMOVED)

                masakari_hosts = []

                # add hosts found in availability zone but not in masakari segment
                if masakari.is_failover_segment_exist(self._token, cluster_name):
                    masakari_hosts = masakari.get_nodes_in_segment(self._token, cluster_name)
                masakari_hids = [x['name'] for x in masakari_hosts]
                common_ids = set(az_hosts).intersection(set(masakari_hids))
                nova_delta_ids = set(az_hosts) - set(common_ids)
                if len(nova_delta_ids) > 0:
                    #Check if the host is already a part of Masakari Database before adding it to avoid any conflicts
                    db_api.connect_masakari(masakari,self._mdb_uri)
                    result = db_api.get_hosts_by_name(nova_delta_ids)

                    LOG.info('found new hosts added into availability zone %s : %s', az_name, str(nova_delta_ids))
                    # need to add those additional hosts into masakari
                    if masakari.is_failover_segment_under_recovery(self._token, cluster_name):
                        LOG.info('will retry to add hosts %s to masakari segment %s, '
                                 'as the segment is under recovery',
                                 str(nova_delta_ids), cluster_name)
                    elif result != None:
                        # host is already a part of other segment so not adding to avoid conflicts
                        LOG.info('Not adding hosts as its already a part of masakari segment %s ', result)
                    else:
                        # hosts already in availability zone so only need to add to masakari
                        LOG.info('add new hosts %s found in availability zone %s to masakari segment %s',
                                 str(nova_delta_ids), az_name, cluster_name)
                        masakari.add_hosts_to_failover_segment(self._token,
                                                               cluster_name,
                                                               nova_delta_ids)
                        # if cluster_enabled:
                        #     # pick consul role for new host based on other cluster members
                        #     consul_role = constants.CONSUL_ROLE_SERVER
                        #     c_report = self._get_latest_consul_status(cluster_name)
                        #     if c_report and c_report.get('members', []):
                        #         members = c_report.get('members')
                        #         c_servers = [x for x in members if x['Tags']['role'] == 'consul']
                        #         c_servers_alive = [x for x in c_servers if x['Status'] == 1]
                        #         if len(c_servers_alive) >= constants.SERVER_THRESHOLD:
                        #             consul_role = constants.CONSUL_ROLE_CLIENT
                        #     LOG.info('add ha slave role (%s) to added hosts: %s', consul_role, str(nova_delta_ids))
                        #     self._add_ha_slave_if_not_exist(cluster_name, nova_delta_ids, consul_role, common_ids)
                        #     # trigger a consul role rebalance request
                        #     time.sleep(30)
                        #     self._rebalance_consul_roles_if_needed(cluster_name, constants.EVENT_HOST_ADDED)
                # when cluster is enabled, make sure pf9-ha-slave role is on all active hosts
                # ----------------------------------------------------------
                # then reconcile masakari hosts and pf9-ha-slave on those
                # hosts for active ha clusters to make sure the
                # pf9-ha-slave role is installed on hosts in all active
                # clusters to provide monitoring ability
                # ----------------------------------------------------------
                if (not cluster_enabled) and \
                        (cluster_status in [constants.HA_STATE_DISABLED, constants.HA_STATE_ERROR]) and \
                        (cluster_task_state in [constants.TASK_COMPLETED, constants.TASK_ERROR_REMOVING]):
                    # if availability zone exist but vmha cluster is disabled, try to remove pf9-ha-slave
                    # in case previous action(disable vmha, or hostagent failed) failed to remove the pf9-ha-slave
                    # this will make sure pf9-ha-slave not left on hosts when vmha disabled for a availability zone
                    try:
                        LOG.info('cluster (name %s, status %s, task state %s) is not enabled, '
                                 'trying to remove pf9-ha-slave role from hosts',
                                 str(cluster_name), str(cluster_status), str(cluster_task_state))
                        self._deauth(az_hosts)
                    except Exception:
                        LOG.warning('remove pf9-ha-slave on hosts %s failed, will retry later',
                                    str(az_hosts))
                    continue

                LOG.debug('cluster %s is enabled, now checking slave role', cluster_name)
                availability_zone = az_name
                segment_name = str(cluster_name)
                availability_zone_host_ids = set(az_hosts)
                new_active_host_ids = set()
                new_inactive_host_ids = set()
                existing_active_host_ids = set()
                existing_inactive_host_ids = set()
                removed_host_ids = set()
                nova_active_host_ids = set()
                try:
                    nodes = masakari.get_nodes_in_segment(self._token, segment_name)
                    masakari_host_ids = set([node['name'] for node in nodes])

                    LOG.debug("hosts records from nova : %s",
                              ','.join(list(availability_zone_host_ids)))
                    LOG.debug("hosts records from masakari : %s",
                              ','.join(list(masakari_host_ids)))

                    for host in availability_zone_host_ids:
                        if self._is_nova_service_active(host, nova_client=nova_client):
                            nova_active_host_ids.add(host)

                            if host not in masakari_host_ids:
                                new_active_host_ids.add(host)
                            else:
                                existing_active_host_ids.add(host)
                        else:
                            if host in masakari_host_ids:
                                # Only the host currently part of cluster that are
                                # down are of interest
                                existing_inactive_host_ids.add(host)
                            else:
                                new_inactive_host_ids.add(host)
                                LOG.info('Ignoring down host %s as it is not part'
                                         ' of the cluster', host)

                    removed_host_ids = masakari_host_ids - availability_zone_host_ids

                    LOG.info('Found existing active hosts : %s ', str(existing_active_host_ids))
                    LOG.info('Found existing inactive hosts : %s ', str(existing_inactive_host_ids))
                    LOG.info('Found new active hosts : %s ', str(new_active_host_ids))
                    LOG.info('Found new inactive hosts : %s ', str(new_inactive_host_ids))
                    LOG.info('Found removed hosts : %s ', str(removed_host_ids))
                    LOG.info('Found nova active hosts : %s ', str(nova_active_host_ids))

                    # do this first than other steps because this is more important, if the maintance
                    # state is not reseted after host become active, it will block sending notification
                    # to masakari if the same host become offline again
                    # resume on maintenance hosts, and enforce pf9-ha-slave role on active hosts
                    LOG.debug('try to reset hosts maintenance state in masakari for active hosts in nova %s',
                              str(nova_active_host_ids))
                    self._toggle_masakari_hosts_maintenance_status(segment_name=segment_name,
                                                                   host_ids=nova_active_host_ids)

                    # only when the cluster state is completed
                    if current_cluster.task_state not in [constants.TASK_COMPLETED]:
                        continue

                    found_hosts_added = len(new_active_host_ids) > 0 or len(new_inactive_host_ids) > 0
                    found_hosts_removed = len(removed_host_ids) > 0
                    found_hosts_changes = found_hosts_added or found_hosts_removed
                    if not found_hosts_changes:
                        LOG.info('no hosts added or removed, now make sure pf9-ha-slave is assigned on hosts %s',
                                 str(nova_active_host_ids))
                        # make sure ha-slave is enabled on existing active hosts
                        hosts_info = self._resmgr_client.fetch_hosts_details(nova_active_host_ids, self._token['id'])
                        current_roles= dict(zip(hosts_info.keys(), [hosts_info.get(x,{}).get('roles', []) for x in hosts_info.keys()]))

                        role_missed_host_ids = set()
                        ip_mismatch_host_ids = set()
                        self._token = self._get_v3_token()
                        join_ips, cluster_ip_lookup, cluster_details = self._get_join_info(cluster_name)
                        LOG.info('expected cluster join_ips: %s', join_ips)

                        for hid in nova_active_host_ids:
                            h_roles = current_roles[hid]
                            LOG.debug("host %s  has roles : %s", str(hid), ','.join(h_roles))
                            if "pf9-ha-slave" not in h_roles:
                                role_missed_host_ids.add(hid)
                            else:
                                rs = hosts_info.get(hid).get('role_settings', {}).get("pf9-ha-slave", {})
                                if 'join' not in rs or rs['join'] != join_ips:
                                    ip_mismatch_host_ids.add(hid)

                        if len(role_missed_host_ids) > 0:
                            txt_ids = ','.join(list(role_missed_host_ids))
                            LOG.info("assign ha-slave role for hosts which is missing pf9-ha-slave: %s", txt_ids)
                            # hosts are not changed but just missing pf9-ha-slave, it is ok to just re-assign roles
                            # TODO: only assign to hosts in role_missed_host_ids??
                            #self._assign_roles(nova_client, availability_zone, nova_active_host_ids)
                            self._add_ha_slave_if_not_exist(cluster_name, role_missed_host_ids, 'server',
                                                            masakari_host_ids)
                            LOG.debug("ha-slave roles are assigned to : %s", txt_ids)
                        elif len(ip_mismatch_host_ids) > 0:
                            LOG.info('updating join ips for availability_zone %s, hosts %s',
                                     str(availability_zone), ip_mismatch_host_ids)
                            #self._auth(availability_zone, cluster_ip_lookup, cluster_ip_lookup, cluster_details,
                            #           self._token, ip_mismatch_host_ids)
                        else:
                            LOG.info("all hosts already have pf9-ha-slave role")

                    if len(new_active_host_ids) > 0:
                        # need to put pf-ha-slave role on them
                        LOG.info('new active hosts are added into availability_zone %s but not in masakari : %s ',
                                 str(availability_zone),
                                 str(new_active_host_ids))
                        # same here, since we don't know the consul roles on existing hosts, so just
                        # make these hosts as consul server, and trigger a role rebalance request
                        self._add_ha_slave_if_not_exist(cluster_name, new_active_host_ids, 'server',
                                                        masakari_host_ids)
                        # trigger consul role rebalance request
                        #time.sleep(30)
                        #self._rebalance_consul_roles_if_needed(cluster_name, constants.EVENT_HOST_ADDED)

                    if len(new_inactive_host_ids) > 0:
                        # need to wait for them become active so can put pf9-ha-slave role on them
                        LOG.info('new inactive hosts are added into availability_zone %s but '
                                 'not in masakari : %s, wait for hosts to become active',
                                 segment_name,
                                 str(new_inactive_host_ids))

                    if len(removed_host_ids) > 0:
                        LOG.info('hosts are removed from nova availability_zone %s : %s, now remove pf9-ha-slave role',
                                 availability_zone, str(removed_host_ids))
                        self._remove_ha_slave_if_exist(cluster_name, removed_host_ids,
                            list( set(existing_active_host_ids).difference(set(removed_host_ids)) ))
                        LOG.info('try to rebalance consul roles after removing hosts: %s', str(removed_host_ids))
                        #time.sleep(30)
                        #self._rebalance_consul_roles_if_needed(cluster_name, constants.EVENT_HOST_REMOVED)

                    if len(existing_inactive_host_ids) > 0:
                        LOG.warning('existing inactive hosts in segment %s: %s, wait for hosts to become active',
                                    segment_name, str(existing_inactive_host_ids))

                except ha_exceptions.ClusterBusy:
                    pass
                except ha_exceptions.InsufficientHosts:
                    LOG.warning('Detected number of availability_zone %s hosts is insufficient', availability_zone)
                except ha_exceptions.SegmentNotFound:
                    LOG.warning('Masakari segment for cluster: %s was not found', current_cluster.name)
                except ha_exceptions.HostPartOfCluster:
                    LOG.error("Not enabling cluster as cluster hosts are part of another cluster")
                except Exception:
                    LOG.exception('Exception while processing availability_zone %s', availability_zone)
        except Exception as e:
            LOG.exception('unhandled exception in host availability_zone change processing task : %s', str(e))
        finally:
            with self.availability_zone_task_lock:
                LOG.debug('Availability Zone changes task completed')
                self.availability_zone_task_running = False
        LOG.debug('host availability zone change processing task has finished at %s', str(datetime.utcnow()))

    def _toggle_masakari_hosts_maintenance_status(self, segment_name="", host_ids=[], ignore_status_in_nova=False, on_maintenance=False):
        try:
            self._token = self._get_v3_token()
            nova_client = self._get_nova_client()

            # figure out the target failover segments for reset
            segment_hosts_pairs = []
            if segment_name:
                if not masakari.is_failover_segment_exist(self._token, str(segment_name)):
                    LOG.warning('vmha cluster with name %s does not exist, will not toggle hosts maintenance status',
                                str(segment_name))
                    return

                masakari_hosts = masakari.get_nodes_in_segment(self._token, str(segment_name))
                # always use the hosts found for the given segment, in case the hosts passed in
                # not belong to the given segment
                valid_hosts = [x['name'] for x in masakari_hosts]

                # when pass in host ids, just use the intersection, because data from masakari is the trusted source
                if host_ids and len(host_ids) > 0 :
                    common = list(set(valid_hosts).intersection(set(host_ids)))
                    if len(common) > 0 :
                        valid_hosts = common

                segment_hosts_pairs.append({
                    'segment_name': segment_name,
                    'host_ids': valid_hosts
                })
            else:
                # find all active ha clusters
                nova_active_azs, azInfo = self._get_active_azs(self._token['id'])
                if not nova_active_azs:
                    LOG.error('Error getting availability zone info, will retry')
                    return
                hamgr_clusters = db_api.get_all_clusters()
                for az in nova_active_azs:
                    # make sure vmha group for this availability_zone actually exists
                    matched_clusters = [x for x in hamgr_clusters if x.name == az['zoneName']]
                    if not matched_clusters:
                        LOG.warning('vmha cluster for host availability_zone %s does not exist or disabled, '
                                    'will not toggle hosts maintenance status', az['zoneName'])
                        continue

                    # make sure masakari failover segment exists
                    if not masakari.is_failover_segment_exist(self._token, az['zoneName']):
                        LOG.error('vmha cluster for host availability zone %s exists, but masakari segment does not exist, '
                                  'will not toggle hosts maintenance status', az['zoneName'])
                        continue

                    masakari_hosts = masakari.get_nodes_in_segment(self._token, az['zoneName'])
                    valid_hosts = [x['name'] for x in masakari_hosts]

                    # if not pass in availability_zone id but just host ids, then use those host id to figure which
                    # availability_zone they belong to, or just ignore host ids and check for every availability_zones
                    if host_ids and len(host_ids) > 0 :
                        common = list(set(valid_hosts).intersection(set(host_ids)))
                        if len(common) > 0:
                            valid_hosts = common

                    # otherwise will check all hosts
                    segment_hosts_pairs.append({
                        'segment_name': str(agg.id),
                        'host_ids': valid_hosts
                    })

            # reset hosts maintenance status
            for entry in segment_hosts_pairs:
                segment_name = entry['segment_name']

                # don't reset host maintenance state if there is vm migration task running
                if masakari.is_failover_segment_under_recovery(self._token, segment_name):
                    LOG.info('segment %s is under recovery, will not toggle hosts maintenance status.',
                             segment_name)
                    continue

                hosts_in_segment = entry['host_ids']
                for hostid in hosts_in_segment:
                    # is this host up and active in nova ?
                    host_up_in_nova = self._is_nova_service_active(hostid, nova_client)

                    # if request to ignore status in nova, then set maintenance to whatever required
                    # but if not to ignore status in nova, then can only reset maintenance when host is up in nova
                    if not ignore_status_in_nova:
                        if not host_up_in_nova and not on_maintenance:
                            LOG.debug('require to not ignore status in nova and toggle maintenance status to %s, '
                                      'but host %s in segment %s is not up in nova. '
                                      'will not toggle hosts maintenance status.',
                                      str(on_maintenance), str(hostid), str(segment_name))
                            continue

                    if not masakari.is_host_on_maintenance(self._token, hostid, segment_name):
                        continue

                    LOG.info('toggle maintenance status to %s for host %s in segment %s, with status in nova %s)',
                              str(on_maintenance), str(hostid), str(segment_name), str(host_up_in_nova))
                    try:
                        masakari.update_host_maintenance(self._token, hostid, segment_name, on_maintenance)
                        LOG.info('maintenance status for host %s in masakari segment %s has been toggled to %s',
                                 str(hostid), str(segment_name), str(on_maintenance))
                    except Exception as e:
                        LOG.warning('failed to toggle maintenance for host %s to %s , error : %s',
                                    hostid, str(on_maintenance), str(e))
        except Exception:
            LOG.exception('unhandled exception when toggle masakari host maintenance status')

    def _cleanup_vmha_and_masakari(self):
        """
        this method is used to remove any additional vmha clusters and
        masakari segments which there is no mapping nova host availability_zones
        :return: none
        """
        self._token = self._get_v3_token()
        nova_active_azs, azInfo = self._get_active_azs(self._token['id'])
        if not nova_active_azs:
            LOG.error('Error getting availability zone info, will retry')
            return
        LOG.debug('names of host availability_zones found : %s', nova_active_azs)
        hamgr_all_clusters = db_api.get_all_clusters()
        hamgr_cluster_names = [x.name for x in hamgr_all_clusters]
        LOG.debug('names of vmha clusters found : %s', str(hamgr_cluster_names))
        masakari_active_segments = masakari.get_all_failover_segments(self._token)
        masakari_segment_names = [x['name'] for x in masakari_active_segments]
        LOG.debug('names of masakari segments found : %s', str(masakari_segment_names))
        common_between_nova_and_hamgr = list(set(nova_active_azs).intersection(set(hamgr_cluster_names)))
        LOG.debug('common set between host availability_zones and vmha cluster : %s', str(common_between_nova_and_hamgr))
        hamgr_clusters_to_remove = list(set(hamgr_cluster_names).difference(common_between_nova_and_hamgr))
        LOG.debug('additional vmha clusters for removal : %s', str(hamgr_clusters_to_remove))

        # try to remove additional ha clusters
        for cluster_name in hamgr_clusters_to_remove:
            try:
                if self.check_vmha_enabled_on_resmgr(cluster_name=cluster_name):
                    LOG.debug('the cluster %s seems to be enabled from resmgr side. Skipping disabling call for this', cluster_name)
                    continue
                LOG.debug('disable additional vmha cluster : %s', cluster_name)
                self._disable(cluster_name, synchronize=True)
            except Exception as exp:
                LOG.warning('failed to remove additional vmha cluster with name %s : %s', cluster_name, str(exp))

        common_between_nova_and_masakari = list(set(nova_active_azs).intersection(set(masakari_segment_names)))
        LOG.debug('common set between host availability_zones and masakari segments : %s', str(common_between_nova_and_masakari))
        masakari_segments_to_remove = list(set(masakari_segment_names).difference(set(common_between_nova_and_masakari)))
        LOG.debug('additional masakari segments for removal : %s', str(masakari_segments_to_remove))
        # try to remove additional masakari cluster if it was not removed during vmha cluster removal
        for segment_name in masakari_segments_to_remove:
            try:
                masakari.delete_failover_segment(self._token, segment_name)
            except Exception as exp:
                LOG.warning('failed to remove additional masakari segment with name %s : %s', segment_name, str(exp))

    def _get_latest_consul_status(self, cluster_name):
        controller = get_rebalance_controller(self._config)
        request = ConsulRefreshRequest(cluster=cluster_name, cmd='refresh')
        status = controller.ask_for_consul_cluster_status(request)
        LOG.debug('latest consul status : %s', str(status))
        if not status or status['status'] != constants.RPC_TASK_STATE_FINISHED:
            LOG.warning('failed to get latest consul status for cluster %s : %s', cluster_name, str(status))
            return None

        report = json.loads(status['report'])
        return report

    def _rebalance_consul_roles_if_needed(self, cluster_name, event_type, event_uuid=None):
        LOG.debug('check if consul cluster %s needs rebalance on event type %s, uuid %s',
                  str(cluster_name), str(event_type), str(event_uuid))
        if event_type not in constants.HOST_EVENTS:
            LOG.warning('not inspect consul role rebalance, as type of event %s is invalid : %s',
                        str(event_uuid), event_type)
            return
        # need the most recent consul info to decide the candidates
        report = self._get_latest_consul_status(cluster_name)
        if not report:
            LOG.warning('not inspect consul role rebalance, as failed to '
                        'refresh consul status for cluster %s', cluster_name)
            return

        LOG.info('consul status refresh report : %s', str(report))
        # check num of hosts match what the cluster should be
        members = report.get('members', [])
        _, _, cluster_details = self._get_join_info(cluster_name)
        if len(members) != len(cluster_details):
            LOG.warning('consul role rebalance: num of hosts in consul '
                        'status does not match expected cluster. '
                        'status : %s, expected : %s',
                        str(members), str(cluster_details))
           # return

        # rather than directly rebalance, just create the rebalance requests
        # and let rebalance processor drive the whole process
        candidates = self._get_consul_rebalance_candidates(members)
        if not candidates:
            LOG.warning("no candidates available for role rebalance for cluster %s", cluster_name)
            return

        LOG.info("report consul rebalance for event %s type %s with candidates : %s",
                 str(event_uuid), event_type, str(candidates))
        self._report_consul_rebalance(cluster_name=cluster_name,
                                      event_type=event_type,
                                      event_uuid=event_uuid,
                                      candidates=candidates,
                                      consul_report=report)

        # we can also record the consul status
        # to avoid too much consul status in db, only record the consul status
        # when there is a real event (host-up/host-down)
        if not event_uuid:
            # use debug level to avoid too much logs that are less value,
            # but will be helpful when debug issues
            LOG.debug('not store consul status as event_uuid is empty, '
                      'current consul status : %s', str(report))
            return
        # add consul status record if event_uuid exist
        try:
            reportedBy = report.get('reportedBy', None)
            leader = report.get('leader', None)
            peers = report.get('peers', None)
            members = report.get('members', None)
            kv = report.get('kv', None)
            joins = report.get('joins', None)
            LOG.debug('refreshed consul status : reportedBy %s , leader %s, '
                      'peers %s, members %s, kv %s, joins %s',
                      str(reportedBy), str(leader), str(peers), str(members),
                      str(kv), str(joins))
            if not reportedBy:
                LOG.warning('consul refresh report has no info for reportedBy')
                return

            nova_client = self._get_nova_client()
            clusters = db_api.get_all_active_clusters()
            if not clusters:
                LOG.debug('no active ha clusters')
                return
            target_cluster_id = None
            target_cluster_name = None
            for cluster in clusters:
                availability_zone = cluster.name
                availability_zone = self._get_availability_zone(nova_client, availability_zone)
                hosts = set(availability_zone.hosts)
                if str(reportedBy) in hosts:
                    target_cluster_id = cluster.id
                    target_cluster_name = cluster.name
                    break
            if not target_cluster_id:
                LOG.debug('no host availability_zones with id %s', target_cluster_name)
                return

            LOG.debug('add new consul status from report %s', str(report))
            db_api.add_consul_status(target_cluster_id,
                                     target_cluster_name,
                                     str(leader),
                                     json.dumps(peers),
                                     json.dumps(members),
                                     json.dumps(kv),
                                     str(joins),
                                     event_uuid)
        except Exception:
            LOG.exception('unhandled exception when try to add new consul status')

    def _execute_consul_role_rebalance(self, rebalance_request, cluster_name, host_ip, join_ips):
        req_id = rebalance_request['id']
        target_host_id = rebalance_request['host_id']
        target_old_role = rebalance_request['old_role']
        target_new_role = rebalance_request['new_role']
        LOG.info('execute consul role rebalance request %s for host %s from old role %s to new role %s',
                  req_id, target_host_id, target_old_role, target_new_role)
        # because the resmgr keeps the customerized settings for consul role, for pf9-ha-slave role
        # the difference between consul server and client is the 'bootstrap_expect' settings
        # for consul server role, the value is now 3, but for consul slave, the value is 0
        # after updated the settings for pf9-ha-slave role,  the consul config file won't be changed
        # until restart the pf9-ha-slave service, or manually change the config file, then run
        # consul join command
        # so need two steps
        # 1) change settings in resmgr
        # 2) send request to host
        #   on host, once received the request, it will
        #   a) when change from 'server' to 'client'
        #      - delete /opt/pf9/etc/pf9-consul/conf.d/server.json
        #      - create /opt/pf9/etc/pf9-consul/conf.d/client.json
        #      - run 'consul join xxx' command
        #   or
        #   b) just restart pf9-ha-slave service
        #
        self._token = self._get_v3_token()
        data = self._resmgr_client.get_host_info(target_host_id, self._token['id'])
        conclusion = {
            'status': constants.RPC_TASK_STATE_FINISHED,
            'message': ''
        }

        if 'pf9-ha-slave' not in data['roles']:
            error = 'host %s does not have pf9-ha-slave role' % target_host_id
            LOG.warning('abort consul role rebalance request, as %s', error)
            conclusion.update({'status': constants.RPC_TASK_STATE_ABORTED, 'message': error})
            return conclusion

        _, _, nodes_details = self._get_ips_for_hosts_v3(cluster_name)
        data = self._customize_pf9_ha_slave_config(cluster_name, join_ips, host_ip, host_ip, nodes_details)
        # step 1 : modify 'bootstrap_expect' base on the new role
        if target_new_role == constants.CONSUL_ROLE_SERVER:
            data['bootstrap_expect'] = self._consul_bootstrap_expect
        else:
            data['bootstrap_expect'] = 0
        LOG.debug('update resmgr for consul role rebalance for host %s with data %s',
                  target_host_id, str(data))
        # update resmgr with the new settings
        result = self._resmgr_client.update_role(target_host_id, "pf9-ha-slave", data, self._token['id'])
        LOG.debug('rebalance by updating settings for host %s on role '
                  'pf9-ha-slave with : %s, returns : %s',
                  str(target_host_id), str(data), str(result))
        succeeced = (result.status_code == requests.codes.ok)
        resp = None
        if not succeeced:
            LOG.warning('failed to update resmgr for consul role rebalance host %s with data %s, resp %s',
                     target_host_id,
                     str(data),
                     str(result))
            conclusion.update({'status': constants.RPC_TASK_STATE_ABORTED, 'message': 'failed to update resmgr : %s' % result.text})
            return conclusion
        else:
            # step 2 : notify pf9-ha-slave with RPC message contains the rebalance request
            LOG.info('sending consul role rebalance request %s ', str(rebalance_request))
            resp = get_rebalance_controller(self._config).rebalance_and_wait_for_result(rebalance_request)

        if resp:
            LOG.info('response of consul role rebalance request : %s', str(resp))
            consul_status = resp.get('consul_status', {})
            resp_msg = resp.get('message', {})
            conclusion.update({'status': resp['status'], 'message': resp_msg, 'consul_status': consul_status})
            return conclusion
        else:
            msg = 'empty consul role rebalance response for request : %s' % str(rebalance_request)
            LOG.warning('consul role rebalance failed due to %s', msg)
            conclusion.update({'status': constants.RPC_TASK_STATE_ERROR, 'message': msg})
            return conclusion

    def _add_ha_slave_if_not_exist(self, cluster_name, host_ids_for_adding, consul_role, peer_host_ids):
        if consul_role not in ['server', 'agent']:
            consul_role = 'server'
        nova_client = self._get_nova_client()
        self._token = self._get_v3_token()
        hosts_info = self._resmgr_client.fetch_hosts_details(host_ids_for_adding, self._token['id'])
        current_roles = dict(zip(hosts_info.keys(), [hosts_info.get(x, {}).get('roles', []) for x in hosts_info.keys()]))

        role_missed_host_ids = []
        for hid in host_ids_for_adding:
            h_roles = current_roles[hid]
            if "pf9-ha-slave" not in h_roles:
                role_missed_host_ids.append(hid)
        if len(role_missed_host_ids) > 0:
            LOG.info('authorize pf9-ha-slave during adding new hosts: %s ', str(role_missed_host_ids))
            hypervisors = nova_client.hypervisors.list()
            #all_hosts = list(set(host_ids_for_adding).union(set(peer_host_ids)))
            # Need join ips for offline hosts in availability_zone as well
            _ , azInfo = self._get_active_azs(self._token['id'])
            if not azInfo:
                LOG.error('Error getting availability zone info, will retry')
                return
            az_hosts = []
            for az in azInfo['availabilityZoneInfo']:
                if az['zoneName'] == cluster_name:
                    for x in az['hosts'].keys():
                        az_hosts.append(x)
                    break
            cluster_hosts = set(az_hosts)
            hosts_info = self._resmgr_client.fetch_hosts_details(cluster_hosts, self._token['id'])

            ip_lookup, cluster_ip_lookup, nodes_details = self._get_ips_for_hosts_v2(hypervisors,
                                                                                     cluster_hosts,
                                                                                     hosts_info)
            LOG.debug('peer_host_ids len %s', len(peer_host_ids))
            LOG.debug('ip_lookup: %s', len(ip_lookup))
            LOG.debug('cluster_ip_lookup: %s', len(cluster_ip_lookup))
            LOG.debug('nodes_details: %s', len(nodes_details))

            self._auth(cluster_name, ip_lookup, cluster_ip_lookup, nodes_details,
                       self._token, role_missed_host_ids, consul_role)
            self._wait_for_role_to_ok_v2(role_missed_host_ids)
            # Update join_ips for existing hosts
            LOG.info('When adding new hosts: updating pf9-ha-slave for existing %d hosts: %s',
                     len(set(peer_host_ids)), str(peer_host_ids))
            self._auth(cluster_name, ip_lookup, cluster_ip_lookup, nodes_details,
                       self._token, peer_host_ids)
            self._wait_for_role_to_ok_v2(peer_host_ids)

    def _remove_ha_slave_if_exist(self, cluster_name, host_ids, peer_host_ids):
        nova_client = self._get_nova_client()
        hypervisors = nova_client.hypervisors.list()
        self._token = self._get_v3_token()
        # Need join ips for offline hosts in availability_zone as well
        _ , azInfo = self._get_active_azs(self._token['id'])
        if not azInfo:
            LOG.error('Error getting availability zone info, will retry')
            return
        az_hosts = []
        for az in azInfo['availabilityZoneInfo']:
            if az['zoneName'] == cluster_name:
                for x in az['hosts'].keys():
                    az_hosts.append(x)
                break
        cluster_hosts = set(az_hosts)
        #all_hosts = list(set(host_ids).union(set(peer_host_ids)))
        all_hosts = list(set(host_ids).union(set(cluster_hosts)))
        hosts_info = self._resmgr_client.fetch_hosts_details(all_hosts, self._token['id'])

        current_roles = dict(zip(hosts_info.keys(), [hosts_info.get(x, {}).get('roles', []) for x in hosts_info.keys()]))
        role_assigned_host_ids = []
        for hid in host_ids:
            h_roles = current_roles[hid]
            if "pf9-ha-slave" in h_roles:
                role_assigned_host_ids.append(hid)
        if len(role_assigned_host_ids) > 0:
            LOG.info('remove pf9-ha-slave role from hosts: %s ', str(host_ids))
            self._deauth(host_ids)
            self._wait_for_role_to_ok_v2(host_ids)
            # Update join_ips for existing hosts
            ip_lookup, cluster_ip_lookup, nodes_details = self._get_ips_for_hosts_v2(hypervisors,
                                                                                     cluster_hosts,
                                                                                     hosts_info)
            LOG.debug('peer_host_ids len %s', len(peer_host_ids))
            LOG.debug('ip_lookup: %s', len(ip_lookup))
            LOG.debug('cluster_ip_lookup: %s', len(cluster_ip_lookup))
            LOG.debug('nodes_details: %s', len(nodes_details))

            LOG.info('When removing hosts: updating pf9-ha-slave for existing %d hosts: %s',
                     len(set(peer_host_ids)), str(peer_host_ids))
            self._auth(cluster_name, ip_lookup, cluster_ip_lookup, nodes_details,
                       self._token, peer_host_ids)
            self._wait_for_role_to_ok_v2(peer_host_ids)

    def _is_nova_service_active(self, host_id, nova_client=None):
        if not nova_client:
            nova_client = self._get_nova_client()
        binary = 'nova-compute'
        services = nova_client.services.list(binary=binary, host=host_id)
        if len(services) == 1:
            if services[0].state == 'up':
                if services[0].status != 'enabled' and \
                        services[0].disabled_reason == constants.PF9_DISABLED_REASON:
                    LOG.info('host %s state is up, status is %s, '
                             'disabled reason %s, so enabling it',
                             str(host_id), str(services[0].status),
                             str(services[0].disabled_reason))
                    nova_client.services.enable(binary=binary, host=host_id)
                LOG.debug("nova host %s is up and enabled", str(host_id))
                return True
            LOG.debug("nova host %s state is down", str(host_id))
            return False
        else:
            LOG.error('Found %s nova compute services with host id %s'
                      % (str(len(services)), host_id))
            raise ha_exceptions.HostNotFound(host_id)

    def _get_nova_client(self):
        # reference:
        # https://docs.openstack.org/python-novaclient/latest/reference/api/index.html
        loader = loading.get_plugin_loader('password')
        auth = loader.load_from_options(auth_url=self._auth_url,
                                        username=self._username,
                                        password=self._passwd,
                                        project_name=self._tenant,
                                        user_domain_id="default",
                                        project_domain_id="default")
        sess = session.Session(auth=auth)
        nova = client.Client(2, session=sess, region_name=self._region, interface='internal')
        return nova

    def _get_az_ha_status(self, availability_zone):
        # availability_zone is the 'name' field in hamgr cluster table
        name = availability_zone
        cluster = db_api.get_cluster(name)
        if cluster is None:
            LOG.warning('no hamgr cluster record for availability zone %s', availability_zone)
            return {}
        enabled = cluster.enabled if cluster is not None else False
        if enabled is True:
            task_state = 'completed' if cluster.task_state is None else \
                cluster.task_state
        else:
            task_state = cluster.task_state
        return dict(name=availability_zone, enabled=enabled, task_state=task_state)
  
    def _get_ha_status(self):
        self._token = self._get_v3_token()
        nova_active_azs, _ = self._get_active_azs(self._token['id'])
        if not nova_active_azs:
            LOG.error('Error getting availability zone info, will retry')
            return
        result = []
        for az in nova_active_azs:
            obj = self._get_az_ha_status(az)
            if obj:
                result.append(obj)
            else:
               LOG.warning('no hamgr cluster db record for availability zone %s', az) 
        return result  

    def get(self, availability_zone):
        if availability_zone is not None:
            obj = self._get_az_ha_status(availability_zone)
            return [obj] if obj else []
        else:
            return self._get_ha_status()

    def _validate_hosts(self, hosts, check_cluster=True, check_host_insufficient=True):
        # [FOR CONSUL]
        # Since the consul needs 3 to 5 servers for bootstrapping, it is safe
        # to enable HA only if 4 hosts are present. So that even if 1 host goes
        # down after cluster is created, we can reconfigure it.
        # [FOR VMHA-AGENT]
        # Just check if there are atleast 2 hosts in the cluster
        if check_host_insufficient and len(hosts) < 2:
            raise ha_exceptions.InsufficientHosts()

        # Check host state and role status in resmgr before proceeding
        # make one call to resmgr , not for each host
        self._token = self._get_v3_token()
        hosts_info = self._resmgr_client.fetch_hosts_details(hosts, self._token['id'])
        for host in hosts:
            json_resp = hosts_info.get(host, {})
            if json_resp:
                LOG.debug('role status for host %s : %s', host, str(json_resp))
                if json_resp.get('role_status', '') != 'ok':
                    LOG.warning('Role status of host %s is not ok : %s, not enabling HA '
                             'at the moment.', host, json_resp.get('role_status', ''))
                    raise ha_exceptions.InvalidHostRoleStatus(host)
                if json_resp['info']['responding'] is False:
                    LOG.warning('Host %s is not responding : %s, not enabling HA at the '
                             'moment.', host, json_resp['info']['responding'])
                    raise ha_exceptions.HostOffline(host)

    def _customize_pf9_ha_slave_config(self, cluster_name, consul_join_ips, consul_host_ip, consul_cluster_ip, nodes_details):
        # since the pf9-ha-slave role is controlled by hamgr, all the customizable settings for pf9-ha-slave role
        # that need to be synced with hamgr should be list here to be passed down to resmgr for updating
        # the settings for pf9-ha-slave on host.
        # the keys needs to match the keys which are customizable in pf9-ha-slave config file
        # first the basic settings
        customize_cfg = dict(cluster_name=str(cluster_name),
                             join=consul_join_ips,
                             ip_address=consul_host_ip,
                             cluster_ip=consul_cluster_ip,
                             cluster_details=json.dumps(nodes_details))
        # then other settings
        is_enabled = self._config.getboolean("DEFAULT", "enable_consul_role_rebalance") \
            if self._config.has_option("DEFAULT", "enable_consul_role_rebalance") else False

        customize_cfg['role_rebalance_enabled'] = str(is_enabled)
        section = 'consul_rebalance'
        if is_enabled and self._config.has_section(section):
            exchange = self._config.get(section, 'exchange_name')
            exchange_type = self._config.get(section, 'exchange_type')
            routingkey_for_sending = self._config.get(section, 'routingkey_for_sending') \
                if self._config.has_option(section, 'routingkey_for_sending') else 'sending'
            routingkey_for_receiving = self._config.get(section, 'routingkey_for_receiving') \
                if self._config.has_option(section, 'routingkey_for_receiving') else 'receiving'
            # here no matter what the settings are used by hamgr, the pf9-ha-slave needs to matched settings
            # in order to send and receive message on the same exchange
            customize_cfg['amqp_exchange_name'] = exchange
            customize_cfg['amqp_exchange_type'] = exchange_type
            # the routing key for sending on controller is the routing key for receiving on hosts
            customize_cfg['amqp_routingkey_sending'] = routingkey_for_receiving
            # the routing key for receiving on controller is the routing key for sending on hosts
            customize_cfg['amqp_routingkey_receiving'] = routingkey_for_sending

        if self._is_consul_encryption_enabled():
            gossip_key = keyhelper.get_consul_gossip_encryption_key(cluster_name=str(cluster_name), seed=self._db_pwd)
            _, ca_cert_content = keyhelper.read_consul_ca_key_cert_pair()
            svc_key_content, svc_cert_content = keyhelper.read_consul_svc_key_cert_pair(cluster_name)
            customize_cfg['encrypt'] = gossip_key
            customize_cfg['verify_incoming'] = "true"
            customize_cfg['verify_outgoing'] = 'true'
            customize_cfg['verify_server_hostname'] = 'false'
            customize_cfg['ca_file_content'] = ca_cert_content
            customize_cfg['cert_file_content'] = svc_cert_content
            customize_cfg['key_file_content'] = svc_key_content

        # use same logging level for pf9-ha-slave
        use_same_level = False
        if self._config.has_option('DEFAULT', 'same_logging_level_for_hosts') :
            use_same_level = self._config.get('DEFAULT', 'same_logging_level_for_hosts')
        if use_same_level:
            log_level = self._config.get("log", "level") \
                if self._config.has_option("log","level") \
                else hamgr.DEFAULT_LOG_LEVEL
            customize_cfg['level'] = log_level

        LOG.debug('customized role configuration : %s', str(customize_cfg))
        return customize_cfg

    def _auth(self, availability_zone, ip_lookup, cluster_ip_lookup, nodes_details, token, nodes, role=None):
        valid_ips = [x for x in cluster_ip_lookup.values() if x != '']
        ips = ','.join([str(v) for v in sorted(valid_ips)])
        LOG.info('ips for consul members to join : %s', ips)
        for node in nodes:
            start_time = datetime.now()
            data = {}
            # data = self._customize_pf9_ha_slave_config(availability_zone, ips, ip_lookup[node], cluster_ip_lookup[node], nodes_details)
            # if role:
            #     data['bootstrap_expect'] = 3 if role == 'server' else 0
            #     LOG.info('Authorizing pf9-ha-slave role on node %s using IP %s',
            #              node, ip_lookup[node])
            # else:
            #     LOG.info('Updating pf9-ha-slave role on node %s using IP %s',
            #              node, ip_lookup[node])
            # LOG.debug('authorize by updating settings for host %s for pf9-ha-slave with : %s', node, str(data))
            resp = self._resmgr_client.update_role(node, 'pf9-ha-slave', data, self._token['id'])
            if resp.status_code == requests.codes.not_found and \
                    resp.content.find(b'HostDown'):
                continue
                #raise ha_exceptions.HostOffline(node)
            # Retry auth if resmgr throws conflict error for upto 2 minutes
            while resp.status_code == requests.codes.conflict:
                LOG.info('Role conflict error for node %s, retrying after 5 sec',
                         node)
                time.sleep(5)
                resp = self._resmgr_client.update_role(node, 'pf9-ha-slave', data, self._token['id'])
                if datetime.now() - start_time > timedelta(seconds=self._max_auth_wait_seconds):
                    break
            if resp.status_code != requests.codes.ok:
                LOG.error('authorize pf9-ha-slave on host %s failed, data %s, resp %s',
                          node, str(data), str(resp))
            #resp.raise_for_status()

    def _get_cluster_ip(self, host_id, json_resp):
        LOG.debug('try to get cluster_ip for host %s from resp %s', str(host_id), str(json_resp))
        if not json_resp:
            LOG.warning('_get_cluster_ip : response object is none, cannot parse cluster ip')
            return None
        if 'cluster_ip' not in json_resp:
            raise ha_exceptions.ClusterIpNotFound(host_id)
        return str(json_resp['cluster_ip'])

    def _get_ips_for_hosts_v3(self, availability_zone):
        cluster_name = str(availability_zone)
        # get hosts ids from masakari for current cluster
        hosts = masakari.get_nodes_in_segment(self._token, str(cluster_name))
        host_ids = [x['name'] for x in hosts]
        LOG.debug('checking configs for %s hosts in availability_zone %s : %s',
                  str(len(host_ids)), cluster_name, str(host_ids))
        nova_client = self._get_nova_client()
        hypervisors = nova_client.hypervisors.list()
        hosts_info = self._resmgr_client.fetch_hosts_details(host_ids, self._token['id'])
        ip_lookup, cluster_ip_lookup, nodes_details = self._get_ips_for_hosts_v2(hypervisors,
                                                                                 host_ids,
                                                                                 hosts_info)
        return ip_lookup, cluster_ip_lookup, nodes_details

    def _get_ips_for_hosts_v2(self, all_hypervisors, host_ids, hosts_info):
        lookup = set(host_ids)
        ip_lookup = dict()
        cluster_ip_lookup = dict()
        nodes_details = []
        for hyp in all_hypervisors:
            host_id = hyp.service['host']
            cluster_ip = ''
            if host_id in lookup:
                # each lookup table has ip for each host
                ip_lookup[host_id] = cluster_ip_lookup[host_id] = hyp.host_ip
                # Overwrite host_ip value with cluster_ip from ostackhost role
                roles_for_host = hosts_info.get(host_id, {}).get('roles', {})
                if roles_for_host:
                    json_resp = hosts_info.get(host_id, {}).get("role_settings", {}).get("pf9-ostackhost-neutron", {})
                    LOG.debug('role details for host %s : %s', host_id, str(json_resp))
                    cluster_ip = self._get_cluster_ip(host_id, json_resp)
                    if cluster_ip:
                        LOG.debug('Using cluster ip %s from ostackhost role for host %s', cluster_ip, host_id)
                        cluster_ip_lookup[host_id] = cluster_ip
                        ip_lookup[host_id] = cluster_ip

                    # only need host which has ostackhost role
                    nodes_details.append({'name': host_id, 'addr': cluster_ip})
                else:
                    LOG.warning('no role map for hypervisor %s', str(host_id))

        # throw exception if num of items in both ip_lookup and cluster_ip_lookup
        # not equals to num of hosts, this will fail the caller earlier
        if len(ip_lookup.keys()) != len(host_ids):
            ids = set(host_ids).difference(set(ip_lookup.keys()))
            LOG.warning('size not match : ip_lookup : %s, hosts : %s,  found no consul ip for hosts %s',
                     str(ip_lookup), str(host_ids), str(ids))
            raise ha_exceptions.HostsIpNotFound(list(ids))
        if len(cluster_ip_lookup.keys()) != len(host_ids):
            ids = set(host_ids).difference(set(cluster_ip_lookup.keys()))
            LOG.warning('size not match : cluster_ip_lookup : %s, hosts : %s, found no cluster ip for hosts %s',
                     str(cluster_ip_lookup), str(host_ids), str(ids))
            raise ha_exceptions.HostsIpNotFound(list(ids))
        return ip_lookup, cluster_ip_lookup, nodes_details

    def _get_join_info(self, cluster_name):
        # get hosts ids from masakari for current cluster
        self._token = self._get_v3_token()
        hosts = masakari.get_nodes_in_segment(self._token, str(cluster_name))
        host_ids = [x['name'] for x in hosts]
        LOG.debug('checking configs for %s hosts in availability_zone %s : %s',
                  str(len(host_ids)), cluster_name, str(host_ids))
        nova_client = self._get_nova_client()
        hypervisors = nova_client.hypervisors.list()
        hosts_info = self._resmgr_client.fetch_hosts_details(host_ids,
                                                             self._token['id'])
        LOG.debug('host details for %s : %s', str(host_ids), str(hosts_info))
        _, cluster_ip_lookup, cluster_details = self._get_ips_for_hosts_v2(
            hypervisors,
            host_ids,
            hosts_info)
        valid_ips = [x for x in cluster_ip_lookup.values() if x != '']
        join_ips = ','.join([str(v) for v in sorted(valid_ips)])
        return join_ips, cluster_ip_lookup, cluster_details

    def _assign_roles(self, nova_client, availability_zone, hosts):
        hosts = sorted(hosts)
        # First 4 hosts act as servers, at minimum
        servers = hosts[0:4]
        agents = []
        if len(hosts) >= 5:
            servers += hosts[4:5]
            agents = hosts[5:]

        self._token = self._get_v3_token()
        hypervisors = nova_client.hypervisors.list()
        # need join ips for offline hosts in availability_zone as well
        cluster_hosts = set(hosts) 
        hosts_info = self._resmgr_client.fetch_hosts_details(cluster_hosts, self._token['id'])
        ip_lookup, cluster_ip_lookup, nodes_details = self._get_ips_for_hosts_v2(hypervisors,
                                                                                 cluster_hosts,
                                                                                 hosts_info)
        LOG.info('authorize pf9-ha-slave during assign roles')
        self._auth(availability_zone, ip_lookup, cluster_ip_lookup, nodes_details, self._token, servers, 'server')
        self._auth(availability_zone, ip_lookup, cluster_ip_lookup, nodes_details, self._token, agents, 'agent')

    def __perf_meter(self, method, time_start):
        time_end = datetime.utcnow()
        LOG.debug('[metric] "%s" call : %s (start : %s , '
                  'end : %s)', str(method), str(time_end - time_start),
                  str(time_start), str(time_end))

    def _enable(self, availability_zone, hosts=None,
                next_state=constants.TASK_COMPLETED):
        """Enable HA

        :params availability_zone: Availability Zone on which HA is being enabled
        :params hosts: Hosts under the availability zone on which HA is to be enabled
                       This option should only be used when enabling HA on few
                       hosts under an availability zone. By default, HA is enabled on
                       all hosts under a availability zone.
        :params next_state: state in which the cluster should be if enable
                            completes. This is used in case of cluster is
                            migrating i.e. enable operation is being performed
                            as part of a cluster migration operation.
        """
        nova_client = self._get_nova_client()
        cluster = None
        cluster_id = None
        try:
            cluster = db_api.get_cluster(availability_zone)
        except ha_exceptions.ClusterNotFound:
            pass
        except Exception:
            LOG.exception('error when get cluster %s from db', availability_zone)
            raise

        if cluster:
            cluster_id = cluster.id
            if cluster.task_state not in [constants.TASK_COMPLETED]:
                if cluster.task_state == constants.TASK_MIGRATING and \
                        next_state == constants.TASK_MIGRATING:
                    LOG.info('Enabling HA has part of cluster migration')
                else:
                    LOG.warning('Cluster %s is running task %s, cannot enable',
                               availability_zone, cluster.task_state)
                    raise ha_exceptions.ClusterBusy(availability_zone,
                                                    cluster.task_state)
        
        #PCD-217 moved to az level hence below check is not needed
        # make sure no host exists in multiple host aggregation
        #availability_zone = self._get_availability_zone(nova_client, availability_zone)
        #if not hosts:
        #    hosts = availability_zone.hosts
            # validate only when enabling HA on all availability_zone hosts
        #     self._validate_hosts(hosts)

        # observed that hosts sometimes need longer time to get to converged state
        # so here don't validate hosts, let the _handle_enable_request processor retry
        # this method's role is to save the request to db
        # this will avoid the REST API return 409
        try:
            LOG.debug('create ha cluster for enabling request')
            cluster = db_api.create_cluster_if_needed(availability_zone, constants.TASK_WAITING)
            db_api.update_cluster_task_state(availability_zone, constants.TASK_WAITING)
            # set the status to 'request-enable'
            db_api.update_request_status(availability_zone, constants.HA_STATE_REQUEST_ENABLE)
            # publish status
            self._notify_status(constants.HA_STATE_REQUEST_ENABLE, "cluster", availability_zone)
        except Exception as e:
            if cluster_id:
                db_api.update_cluster_task_state(cluster_id, next_state)
            LOG.error('unhandled exception : %s', str(e))

    def _handle_enable_request(self, request, hosts=None, next_state=constants.TASK_COMPLETED):
        LOG.info('Handling enable request : %s', str(request))
        if not request:
            return
        nova_client = self._get_nova_client()
        str_availability_zone = request.name
        cluster_id = request.id
        cluster_name = str(request.name)  
        LOG.info('Enabling HA on some of the hosts %s of the %s availability_zone',
                     str(hosts), str_availability_zone)
        try:
            # observed that hosts sometimes need longer time to get to converged state
            # if this happens, _validate_hosts will throw exceptions, then the request will
            # not be processed until all requirements are met
            self._validate_hosts(hosts)
        except ha_exceptions.HostPartOfCluster:
            # if because host in two clusters, just report the request in error status
            db_api.update_request_status(cluster_id, constants.HA_STATE_ERROR)
            LOG.warning('found hosts exist in other clusters')
            raise
        except (ha_exceptions.HostOffline,
                ha_exceptions.InvalidHostRoleStatus,
                ha_exceptions.InvalidHypervisorRoleStatus) as ex:
            LOG.warning('exception when handle enable request : %s, will retry the request', str(ex))
            return
        except Exception:
            LOG.exception('unhandled exception in validate hosts when handle enable request')
            return
        time_begin = datetime.utcnow()
        self._token = self._get_v3_token()
        self.__perf_meter('utils.get_token', time_begin)
        try:
            # when request to enable a cluster, first create CA if not exist, and svc key and certs
            # if self._is_consul_encryption_enabled():
            #     ca_changed = False
            #     svc_changed = False
            #     if not keyhelper.are_consul_ca_key_cert_pair_exist() or \
            #             keyhelper.is_consul_ca_cert_expired():
            #         ca_changed = keyhelper.create_consul_ca_key_cert_pairs()
            #         LOG.info('CA key and cert are generated ? %s', str(ca_changed))
            #     else:
            #         LOG.debug('CA key and cert are good for now')

            #     if not keyhelper.are_consul_svc_key_cert_pair_exist(cluster_name) or \
            #             keyhelper.is_consul_svc_cert_expired(cluster_name):
            #         svc_changed = keyhelper.create_consul_svc_key_cert_pairs(cluster_name)
            #         LOG.info('svc key and cert for cluster name %s are generated ? %s', cluster_name, str(svc_changed))
            #     else:
            #         LOG.debug('svc key and cert for cluster name %s are good for now', cluster_name)

            # 1. update task_state to 'creating' and mark request as 'enabling' in status
            cluster = db_api.get_cluster(cluster_id)
            if cluster.task_state != constants.TASK_CREATING:
                db_api.update_cluster_task_state(str_availability_zone, constants.TASK_CREATING)
            LOG.info('updating status of cluster %s to %s', str(cluster_id), constants.HA_STATE_ENABLING)
            time_begin = datetime.utcnow()
            db_api.update_request_status(cluster_id, constants.HA_STATE_ENABLING)
            self._notify_status(constants.HA_STATE_ENABLING, "cluster", cluster_id)
            self.__perf_meter('db_api.update_enable_or_disable_request_status', time_begin)

            # 2. Push roles
            time_begin = datetime.utcnow()
            LOG.info('assign roles for hosts during handling enabling request')
            self._assign_roles(nova_client, str_availability_zone, hosts)
            self.__perf_meter('_assign_roles', time_begin)

            # 3. Create masakari fail-over segment
            time_begin = datetime.utcnow()
            if not masakari.is_failover_segment_exist(self._token, str(cluster_name)):
                LOG.info('create masakari masakari segment during handling enabling request, '
                         'segment id %s, name %s, hosts %s', str(cluster_id), cluster_name, str(hosts))
                # masakari failover segment must be created with vmha cluster name, not id
                masakari.create_failover_segment(self._token, cluster_name, hosts)
            self.__perf_meter('masakari.create_segment', time_begin)

            # 4. mark request as 'enabled' in status
            LOG.info('updating status of cluster with id %s to %s', str(cluster_id),
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
                                                 constants.TASK_COMPLETED)

            # rollback above steps
            self._handle_disable_request(request)

            # if rollbacks succeeded , then flag 'enabled' as false
            if cluster_id is not None:
                try:
                    db_api.update_cluster(cluster_id, False)
                except ha_exceptions.ClusterNotFound:
                    pass
            raise
        else:
            if cluster_id is not None:
                time_begin = datetime.utcnow()
                db_api.update_cluster_task_state(cluster_id, next_state)
                self.__perf_meter('db_api.update_cluster_task_state', time_begin)

    def _wait_for_role_to_ok_v2(self, nodes):

        timeout = self._max_role_converged_wait_seconds
        start_time = datetime.now()
        converged=set()

        self._token = self._get_v3_token()

        while True:
            if datetime.now() - start_time > timedelta(seconds=timeout):
                LOG.info("not all hosts are converged, starting from %s to %s",
                         str(start_time), str(datetime.now()))
                break

            app_info = self._resmgr_client.fetch_app_details(nodes, self._token['id'], self._bbmaster_host)

            all_ok = True
            for node in nodes:
                resp = app_info.get(node, {})
                if resp:
                    if resp.get('converged', False) == True:
                        converged.add(node)
                        all_ok &= True
                    else:
                        all_ok &= False
                else:
                    all_ok &= False

            if all_ok:
                break

            time.sleep(30)

        not_converged = list(set(nodes).difference(converged))
        if len(not_converged) > 0:
            LOG.error('_wait_for_role_to_ok_v2: %d hosts not converged out of %d',
                      len(not_converged), len(nodes))
            raise ha_exceptions.RoleConvergeFailed(','.join(not_converged))
        LOG.debug('End _wait_for_role_to_ok_v2')

    def _deauth(self, nodes):
        self._token = self._get_v3_token()
        for node in nodes:
            LOG.info('De-authorizing pf9-ha-slave role on node %s', node)
            start_time = datetime.now()
            resp = self._resmgr_client.delete_role(node, "pf9-ha-slave", self._token['id'])
            # Retry deauth if resmgr throws conflict error for upto 2 minutes
            while resp.status_code == requests.codes.conflict:
                LOG.info('Role removal conflict error for node %s, retrying '
                         'after 5 sec', node)
                time.sleep(5)
                resp = self._resmgr_client.delete_role(node, "pf9-ha-slave", self._token['id'])
                if datetime.now() - start_time > timedelta(seconds=self._max_auth_wait_seconds):
                    break

            if resp.status_code != requests.codes.ok:
                LOG.error('De-authorizing pf9-ha-slave role on host %s failed, error: %s',
                          node, str(resp))

            # Explicitly delete rabbitmq queue for the host being deauthed
            req_url = 'http://{0}:{1}@{2}:{3}/api/queues/%2f/{4}-{5}' \
                      .format(self._amqp_user, self._amqp_password, self._amqp_mgmt_host,
                              AMQP_HTTP_PORT, AMQP_HOST_QUEUE_PREFIX, node)
            resp = requests.delete(req_url)
            if resp.status_code not in (requests.codes.no_content, requests.codes.not_found):
                LOG.warning('Failed to delete rabbitmq queue for host %s on role deauth: %s',
                            node, resp.status_code)
        return resp


    def _disable(self, availability_zone, synchronize=False,
                 next_state=constants.TASK_COMPLETED):
        """Disable HA

        :params availability_zone: Host availability_zone ID on which HA is being disabled
        :params synchronize: when set to True, function blocks till ha-slave
                             role is removed from all the hosts
        :params next_state: state in which the cluster should be if disable
                            completes. This is used in case of cluster is
                            migrating i.e. disable operation is being performed
                            as part of a cluster migration operation. If there
                            is an error during the disable operation cluster
                            will be put in "error removing" state
        """
        str_availability_zone = str(availability_zone)
        cluster = db_api.get_cluster(str_availability_zone)
        if not cluster:
            LOG.warning('no cluster with id for disable %s', str_availability_zone)
            return

        if cluster.task_state not in [constants.TASK_COMPLETED, constants.TASK_WAITING,
                                      constants.TASK_ERROR_REMOVING]:
            if cluster.task_state == constants.TASK_MIGRATING and \
                    next_state == constants.TASK_MIGRATING:
                LOG.info('disabling HA as part of cluster migration')
            else:
                LOG.warning('Cluster %s is busy in %s state', cluster.name,
                         cluster.task_state)
                raise ha_exceptions.ClusterBusy(cluster.name,
                                                cluster.task_state)

        try:
            LOG.info('set cluster id %s task state to %s', str(cluster.id), constants.TASK_REMOVING)
            db_api.update_cluster_task_state(cluster.id, constants.TASK_REMOVING)
            # mark the status as 'request-disable' for processing
            LOG.info('set cluster id %s status to %s', str(cluster.id),constants.HA_STATE_REQUEST_DISABLE )
            db_api.update_request_status(cluster.id, constants.HA_STATE_REQUEST_DISABLE)
            self._notify_status(constants.HA_STATE_REQUEST_DISABLE, "cluster", cluster.id)
        except Exception as e:
            LOG.error('unhandled exceptions when disable cluster with name %s : %s ', str_availability_zone, str(e))

    def process_ha_enable_disable_requests(self):
        with self.ha_status_processing_lock:
            if self.ha_status_processing_running:
                LOG.debug('ha status processing task is already running')
                return
            self.ha_status_processing_running = True
        LOG.debug('HA status processing task starts to run at %s', str(datetime.utcnow()))
        try:
            self._token = self._get_v3_token()
            requests = db_api.get_all_unhandled_enable_or_disable_requests()
            if requests:
                for request in requests:
                    if request.status == constants.HA_STATE_REQUEST_ENABLE:
                        # cleanup before enable request is processed to avoid masakari conflict
                        self._cleanup_vmha_and_masakari()
                        with self.ha_status_processing_lock:
                            str_availability_zone = request.name
                            _ , azInfo = self._get_active_azs(self._token['id'])
                            if not azInfo:
                                # azInfo can be null if no AZ is created or 
                                #an error connecting to nova-api (invalid token, nova-api not available..etc)
                                LOG.info('No AZ created or error getting availability zone info, will retry')
                                continue
                            az_hosts = []
                            for az in azInfo['availabilityZoneInfo']:
                                if az['zoneName'] == str_availability_zone:
                                    for x in az['hosts'].keys():
                                        az_hosts.append(x)
                                    break
                            if len(az_hosts) < 2:
                                LOG.info('less than 2 hosts in availability zone %s waiting till it has >=2 hosts', str_availability_zone)
                                continue
                        self._handle_enable_request(request, hosts=az_hosts)
                    if request.status == constants.HA_STATE_REQUEST_DISABLE:
                        self._handle_disable_request(request)
                        # cleanup after disable request is processed to keep things synced
                        self._cleanup_vmha_and_masakari()
        finally:
            with self.ha_status_processing_lock:
                self.ha_status_processing_running = False
        LOG.debug('HA status processing task has finished at %s', str(datetime.utcnow()))

    def _handle_disable_request(self, request, next_state=constants.TASK_COMPLETED):
        LOG.info('Handling disable request : %s', str(request))
        cluster = request
        # the name used to query db needs to be string, not int
        # name in failover segment matches the name used in vmha cluster
        cluster_name = str(cluster.name)
        try:
            self._token = self._get_v3_token()
            hosts = None
            try:
                if cluster:
                    # If availability_zone has changed, we need to remove role from
                    # previous segment's nodes
                    nodes = masakari.get_nodes_in_segment(self._token,
                                                          cluster_name)
                    hosts = [n['name'] for n in nodes]
                else:
                    # If cluster was not even created, but we are disabling HA
                    # to rollback enablement
                    _ , azInfo = self._get_active_azs(self._token['id'])
                    if not azInfo:
                        LOG.error('Error getting availability zone info, will retry')
                        return
                    az_hosts = []
                    for az in azInfo['availabilityZoneInfo']:
                        if az['zoneName'] == 'internal':
                            continue
                        for x in az['hosts'].keys():
                            az_hosts.append(x)
                    hosts = set(az_hosts)
            except ha_exceptions.SegmentNotFound:
                LOG.warning('Masakari segment for cluster: %s was not found, '
                         'skipping deauth', cluster_name)

            # need to check whether masakari is working on this failover
            # segment for any notifications. if there is such notification
            # in 'new', 'running', 'error' state, then it is locked down
            # until the notification is not in those state, then we can
            # delete the failover segment, otherwise there will be
            # 409 conflict error
            need_to_wait = masakari.is_failover_segment_under_recovery(
                self._token, cluster_name)
            if need_to_wait:
                LOG.info('Not disabling cluster %s for now, as masakari segment is under recovery, '
                         'will retry disabling request later', cluster_name)
                return

            # since the failover segment is not under recovery , so force to reset host maintenance status
            self._toggle_masakari_hosts_maintenance_status(segment_name=cluster_name, ignore_status_in_nova=True)

            # change status from 'request-disable' to 'disabling'
            LOG.info('set cluster id %s task state to %s', str(cluster.id), str(constants.HA_STATE_DISABLING))
            db_api.update_request_status(cluster.id, constants.HA_STATE_DISABLING)
            self._notify_status(constants.HA_STATE_DISABLING, "cluster", cluster.id)
            if hosts:
                LOG.debug('De-authorize roles from hosts and wait for completion')
                self._deauth(hosts)
                LOG.info('Waiting for role deauth to converge for cluster %s', cluster_name)
                self._wait_for_role_to_ok_v2(hosts)
                LOG.info('De-authorize process complete for availability_zone %s hosts', cluster_name)
            else:
                LOG.info('no hosts found in availability_zone for deauth %s', cluster_name)
            LOG.info('Deleting failover segment %s from masakari', cluster_name)
            masakari.delete_failover_segment(self._token, cluster_name)
            # change status from 'disabling' to 'disabled'
            db_api.update_request_status(cluster.id, constants.HA_STATE_DISABLED)
            self._notify_status(constants.HA_STATE_DISABLED, "cluster", cluster.id)
            LOG.info('Successfully processed disable request for availability_zone %s', cluster_name)
        except Exception:
            if cluster:
                LOG.exception('Disable HA request failed for cluster %s', cluster_name)
                # mark the status as 'error'
                db_api.update_request_status(cluster.id, constants.HA_STATE_ERROR)
                self._notify_status(constants.HA_STATE_ERROR, "cluster", cluster.id)
                db_api.update_cluster_task_state(cluster.id, constants.TASK_ERROR_REMOVING)
            raise
        else:
            if cluster:
                db_api.update_cluster(cluster.id, False)
                db_api.update_cluster_task_state(cluster.id, next_state)

    def put(self, availability_zone, method):
        LOG.info('process %s request for Availability zone %s', method, availability_zone)
        if method == 'enable':
            self._enable(availability_zone)
        else:
            self._disable(availability_zone)

    def _get_cluster_for_host(self, host_id, nova_client=None):
        if not nova_client:
            nova_client = self._get_nova_client()
        clusters = db_api.get_all_active_clusters()
        for cluster in clusters:
            availability_zone = cluster.name
            availability_zone = self._get_availability_zone(nova_client, availability_zone)
            if host_id in availability_zone.hosts:
                return cluster
        raise ha_exceptions.HostNotFound(host=host_id)

    def _remove_host_from_cluster(self, cluster, host, nova_client=None):
        LOG.info('Removing host %s from availability_zone %s', host, cluster.name)
        if not nova_client:
            nova_client = self._get_nova_client()
        self._token = self._get_v3_token()
        az_hosts = self._get_nova_hosts(self._token['id'], cluster.name)
        if not az_hosts:
            LOG.error('Error getting hosts in availability zone %s, will retry', cluster.name)
            return
        current_host_ids = set(az_hosts)

        try:
            self._token = self._get_v3_token()
            nodes = masakari.get_nodes_in_segment(self._token, availability_zone)
            db_node_ids = set([node['name'] for node in nodes])
            for current_host in current_host_ids:
                if not self._is_nova_service_active(current_host,
                                                    nova_client=nova_client):
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
                self._disable(availability_zone, synchronize=True,
                              next_state=constants.TASK_MIGRATING)
                self._enable(availability_zone, hosts=list(host_list),
                             next_state=constants.TASK_MIGRATING)
                self.hosts_down_per_cluster.pop(cluster.id)
            else:
                # TODO: Addtional tests to verify that multiple host failures
                #       does not have other side effects
                LOG.info('There are still down hosts that need to be reported'
                         ' before reconfiguring the cluster')
        except Exception as e:
            LOG.exception(
                'Could not process {host} host removal. HA is not enabled '
                'because of this error : {error}'.format(host=host, error=e))
        db_api.update_cluster_task_state(cluster.id, constants.TASK_COMPLETED)

    def host_down(self, event_details):
        host = event_details['event']['hostName']
        try:
            # report host down event
            retval = self._report_event(constants.EVENT_HOST_DOWN,
                                        event_details)
        except Exception:
            LOG.exception('Error processing %s host down', host)
            retval = False
        return retval

    def host_up(self, event_details):
        host = event_details['event']['hostName']
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
            nova_client = self._get_nova_client()
            self._token = self._get_v3_token()
            LOG.debug('prepare to write event, host %s, event %s ,  details %s',
                      host_name, event_type, event_details)
            # find the cluster id from given host id
            clusters = db_api.get_all_active_clusters()
            if clusters:
                target_cluster_id = None
                target_cluster_name = None
                for cluster in clusters:
                    availability_zone = cluster.name
                    _ , azInfo = self._get_active_azs(self._token['id'])
                    if not azInfo:
                        LOG.error('Error getting availability zone info, will retry')
                        return
                    az_hosts = []
                    for az in azInfo['availabilityZoneInfo']:
                        if az['zoneName'] == availability_zone:
                            for x in az['hosts'].keys():
                                az_hosts.append(x)
                            break
                    hosts = set(az_hosts)
                    if str(host_name) in hosts:
                        target_cluster_id = cluster.id
                        target_cluster_name = cluster.name
                        break

                if target_cluster_id and target_cluster_name:
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

                    if not existing_changes:
                        # write change event into db
                        db_api.create_change_event(target_cluster_id,
                                                   { 'host_id': event_details['host_id'],
                                                     'event': event_details['event'] },
                                                   event_details['event']['eventId'])
                        LOG.info('change event record is created for host %s in cluster %s',
                                 host_name, target_cluster_id)
                    else:
                        ids = [x.uuid for x in existing_changes]
                        LOG.warning('ignore reporting change event %s for host %s, as it is already '
                                    'reported within %s seconds : %s', event_type, host_name,
                                    self._event_report_threshold_seconds, str(ids))

                    # similarly, suppress events that has been recorded within given
                    # threshold seconds to avoid flooding
                    end_time = datetime.utcnow()
                    start_time = end_time - timedelta(seconds=self._event_report_threshold_seconds)
                    existing_events = db_api.get_processing_events_between_times(
                        event_type,
                        host_name,
                        target_cluster_id,
                        start_time,
                        end_time)

                    event_uuid = event_details['event']['eventId']
                    if not existing_events:
                        # write events processing record
                        db_api.create_processing_event(event_uuid,
                                                       event_type,
                                                       host_name,
                                                       target_cluster_id)

                        # record the consul status
                        leader = event_details['consul'].get('leader', '')
                        peers = event_details['consul'].get('peers', '')
                        members = event_details['consul'].get('members', '')
                        kv = event_details['consul'].get('kv', '')
                        joins = event_details['consul'].get('joins', '')
                        db_api.add_consul_status(target_cluster_id,
                                                 target_cluster_name,
                                                 str(leader),
                                                 json.dumps(peers),
                                                 json.dumps(members),
                                                 json.dumps(kv),
                                                 str(joins),
                                                 event_uuid)

                        LOG.info('event processing record is created for host %s on '
                                 'event %s in cluster %s', host_name, event_type,
                                 target_cluster_id)

                        candidates = self._get_consul_rebalance_candidates(members)
                        if not candidates:
                            LOG.warning("No candidates available for role rebalance for cluster %s", target_cluster_name)
                        else:
                            LOG.info('reporting consul rebalance for event type %s happened to host %s',
                                     event_type, host_name)
                            self._report_consul_rebalance(cluster_name=target_cluster_name,
                                                          event_type=event_type,
                                                          event_uuid=event_uuid,
                                                          candidates=candidates,
                                                          consul_report=event_details['consul'])
                    else:
                        ids = [x.event_uuid for x in existing_events]
                        LOG.warning('ignore reporting event %s %s for host %s, as it is already '
                                 'reported within %s seconds : %s',
                                 event_type, event_uuid, host_name, self._event_report_threshold_seconds, str(ids))
                else:
                    LOG.warning('ignore reporting event %s for host %s , '
                             'as it is not found in any active nova availability_zones : %s',
                             event_type, host_name, str(event_details))
            else:
                LOG.warning('ignore reporting event, no active clusters found for event %s for host %s',
                         event_type, host_name)

            # return True will stop the ha slave to re-send report, when return False, the ha slave will keep
            # sending report until it received True
            return True
        except Exception:
            LOG.exception('failed to record event of type %s for host %s', event_type, host_name)
        return False

    def _get_consul_rebalance_candidates(self, consul_members):
        members = consul_members
        # create a consul role rebalance record for this event, the rebalance thread will handle them later
        consul_servers = [x for x in members if x['Tags']['role'] == 'consul']
        consul_servers_alive = [x for x in consul_servers if x['Status'] == 1]
        consul_slaves = [x for x in members if x['Tags']['role'] == 'node']
        consul_slaves_alive = [x for x in consul_slaves if x['Status'] == 1]
        msg = 'found %s alive consul role of server: %s' % (
              str(len(consul_servers_alive)), str(consul_servers_alive))
        LOG.debug(msg)
        msg = 'found %s alive consul role of slave: %s' % (
              str(len(consul_slaves_alive)), str(consul_slaves_alive))
        LOG.debug(msg)

        # rebalance consul roles
        old_role = ""
        new_role = ""
        candidates = []
        if len(consul_servers_alive) > constants.SERVER_THRESHOLD:
            # move servers to slaves
            LOG.info('more than %s alive consul role of server found, currently %s',
                     str(constants.SERVER_THRESHOLD),
                     str(len(consul_servers_alive)))
            old_role = constants.CONSUL_ROLE_SERVER
            new_role = constants.CONSUL_ROLE_CLIENT
            start = constants.SERVER_THRESHOLD
            end = len(consul_servers_alive)
            for i in range(start, end):
                consul_host = consul_servers_alive[i - start]
                # only active host can be used for rebalance
                if self._is_nova_service_active(consul_host['Name'], self._get_nova_client()):
                    candidates.append({'host': consul_host['Name'],
                                       'old_role': old_role,
                                       'new_role': new_role,
                                       'member': consul_host})
        elif len(consul_servers_alive) < constants.SERVER_THRESHOLD:
            wanted = constants.SERVER_THRESHOLD - len(consul_servers_alive)
            LOG.info('not enough alive consul role of server, found %s, required %s, wanted %s more',
                     str(len(consul_servers_alive)),
                     str(constants.SERVER_THRESHOLD),
                     str(wanted))
            if len(consul_slaves_alive) == 0:
                LOG.error('ignore consul role rebalance as there are no '
                          'alive consul agents to change to servers')
            else:
                # move clients to servers
                old_role = constants.CONSUL_ROLE_CLIENT
                new_role = constants.CONSUL_ROLE_SERVER
                end = 0

                if len(consul_slaves_alive) >= wanted:
                    end = wanted
                if wanted > len(consul_slaves_alive):
                    wanted = len(consul_slaves_alive)
                for i in range(0, len(consul_slaves_alive)):
                    consul_host = consul_slaves_alive[i]
                    # only active host can be used for rebalance
                    if self._is_nova_service_active(consul_host['Name']):
                        wanted = wanted - 1
                        candidates.append({'host': consul_host['Name'],
                                           'old_role': old_role,
                                           'new_role': new_role,
                                           'member': consul_host})
                    if not wanted:
                        break
        else:
            LOG.debug('consul role rebalance is not needed, num of alive consul servers %s meets required %s',
                      str(len(consul_servers_alive)), str(constants.SERVER_THRESHOLD))
        if candidates:
            LOG.info('found consul role rebalance candidates. consul members: %s, candidates : %s',
                     str(consul_members), str(candidates))
        return candidates

    def _report_consul_rebalance(self, cluster_name, event_type, candidates, event_uuid=None, consul_report=None):
        try:
            cluster = db_api.get_cluster(str(cluster_name), read_deleted=False, raise_exception=True)
            if not candidates or (cluster.status != constants.HA_STATE_ENABLED):
                LOG.debug('cluster %s: no rebalance candidates or no need to rebalance', cluster_name)
                return

            LOG.info('report consul rebalance with candidates : %s', str(candidates))
            # create rebalance request for each target hosts, they should be processed one by one
            for candidate in candidates:
                host_id = candidate['host']
                rebalance_action = {'cluster':cluster_name,
                                    'host': host_id,
                                    'old_role': candidate['old_role'],
                                    'new_role': candidate['new_role']
                                    }
                existing_actions = db_api.get_unhandled_consul_role_rebalance_records_by_action(
                    json.dumps(rebalance_action))
                # if for the same host, there are unhandled requests(either server to slave, or slave to server)
                # that's means consecutive host events happened , so only need to create rebalance request
                # base on the most recent consul status
                if existing_actions and len(existing_actions) > 0:
                    # mark all old unhandled existing request as cancelled
                    for old_req in existing_actions:
                        error = 'newer change %s required for host %s' % (json.dumps(rebalance_action), host_id)
                        LOG.info('cancel unhandled consul role rebalance request %s (%s), as %s',
                                 old_req.uuid,
                                 old_req.rebalance_action,
                                 error)
                        db_api.update_consul_role_rebalance(old_req.uuid,
                                                            None,
                                                            None,
                                                            constants.RPC_TASK_STATE_ABORTED,
                                                            error)
                # check nova again to make sure the candidate host is still active
                if not self._is_nova_service_active(host_id, self._get_nova_client()):
                    LOG.warning('host %s is not active in nova, ignore it for consul role rebalance request', host_id)
                else:
                    LOG.info('create consul role rebalance request for host %s : %s', host_id, str(rebalance_action))
                    db_api.add_consul_role_rebalance_record(event_type,
                                                            event_uuid,
                                                            json.dumps(consul_report) if consul_report else None,
                                                            json.dumps(rebalance_action),
                                                            str(uuid4()))
                    #TODO: check there is no rebalance action for another host in same segment
        except:
            LOG.exception('unhandled exception during reporting consul rebalance')

    def _notify_status(self, action, target, identifier):
        enabled = False
        if self._config.has_option("DEFAULT", "notification_enabled"):
            enabled = self._config.getboolean("DEFAULT", "notification_enabled")
        if not enabled:
            return
        obj = ClusterEvent(action, target, identifier)
        get_notification_manager(self._config).send_notification(obj)

    def get_availability_zone_info_for_cluster(self, cluster_id_or_name):
        try:
            if not isinstance(cluster_id_or_name, str) and \
                    not isinstance(cluster_id_or_name, int):
                LOG.info('cluster_id_or_name %s can only be string or int', str(cluster_id_or_name))
                return None

            nova_client = self._get_nova_client()
            clusters = db_api.get_all_active_clusters()
            targets = []
            if isinstance(cluster_id_or_name, str):
                targets = [cluster for cluster in clusters if cluster.name == cluster_id_or_name]
            elif isinstance(cluster_id_or_name, int):
                targets = [cluster for cluster in clusters if cluster.id == cluster_id_or_name]

            if len(targets) > 0:
                availability_zone = targets[0].name
                availability_zone = self._get_availability_zone(nova_client, availability_zone)
                LOG.debug('host availability_zone details for cluster %s : %s',
                          str(cluster_id_or_name), str(availability_zone))
                return availability_zone
            return None
        except:
            LOG.exception('unhandled exception when get availability_zone info '
                          'for cluster %s', str(cluster_id_or_name))
        return None

    def process_consul_role_rebalance_requests(self):
        # wait until current task complete
        with self.consul_role_rebalance_processing_lock:
            if self.consul_role_rebalance_processing_running:
                LOG.debug('consul role rebalance processing task is already running')
                return
            self.consul_role_rebalance_processing_running = True
        LOG.debug('consul role rebalance processing task start to work at %s', str(datetime.utcnow()))

        req_id = None
        try:
            # ----------------------------------------------------------------
            # get all unhandled requests
            # check whether the associated host event is actually finished
            # use RPC to notify host the request
            # wait for response from host to update the record
            # ----------------------------------------------------------------
            unhandled_requests = db_api.get_all_unhandled_consul_role_rebalance_requests()
            if not unhandled_requests:
                # when there are no unhandled consul role rebalance requests, try to detect whether
                # there is a need to generate the role rebalance request to correct any over rebalanced
                # scenarios (issue found from ticket https://platform9.zendesk.com/agent/tickets/1252718)
                self._token = self._get_v3_token()
                nova_active_azs, azInfo = self._get_active_azs(self._token['id'])
                if not nova_active_azs:
                    LOG.error('Error getting availability zone info, will retry')
                    return
                hamgr_all_clusters = db_api.get_all_clusters()
                for az in azInfo['availabilityZoneInfo']:
                    az_name = az['zoneName']
                    hamgr_clusters_for_az = [x for x in hamgr_all_clusters if x.name == az_name]
                    if len(hamgr_clusters_for_az) < 1:
                        continue
                    current_cluster = hamgr_clusters_for_az[0]
                    cluster_name = current_cluster.name
                    cluster_enabled = current_cluster.enabled
                    if not cluster_enabled:
                        continue
                    LOG.debug('check whether need to rebalance consul roles for consul cluster %s', cluster_name)
                    self._rebalance_consul_roles_if_needed(cluster_name=cluster_name,
                                                           event_type=constants.EVENT_CONSUL_INSPECT)
                # once the check is done, just return
                return
            for req in unhandled_requests:
                req_id = req.uuid
                LOG.info('processing consul role rebalance request %s for event %s: %s',
                         req.uuid, str(req.event_uuid), str(req.rebalance_action))
                # first check by time, abort request if timestamp shows the request older than 15 minutes
                if (datetime.utcnow() - req.last_updated) > timedelta(minutes=15):
                    error = 'request created at %s is stale' % req.last_updated
                    LOG.warning('ignore consul role rebalance request %s, as %s', str(req_id), error)
                    db_api.update_consul_role_rebalance(req.uuid,
                                                        None,
                                                        None,
                                                        constants.RPC_TASK_STATE_ABORTED,
                                                        error)
                    continue
                # then check by status, query the same request again to get most recent status (in case it is canceled )
                same_req = db_api.get_consul_role_balance_record_by_uuid(req.uuid)
                if same_req is None or same_req.action_status == constants.RPC_TASK_STATE_ABORTED:
                    LOG.warning('ignore consul role rebalance request %s, as it was already in state %s',
                             req.uuid,
                             req.action_status)
                    continue

                action_obj = json.loads(req.rebalance_action)
                target_cluster = action_obj['cluster']
                target_host_id = action_obj['host']
                target_old_role = action_obj['old_role']
                target_new_role = action_obj['new_role']

                event_uuid = req.event_uuid
                event_type = req.event_name

                if event_type not in constants.HOST_EVENTS:
                    error = 'invalid event type %s' % event_type
                    LOG.warning('ignore consul role rebalance request %s, as %s', event_uuid, error)
                    db_api.update_consul_role_rebalance(req.uuid,
                                                        None,
                                                        None,
                                                        constants.RPC_TASK_STATE_ABORTED,
                                                        error)
                    continue

                # if the event type is host-up or host-down, then there is an such event reported by ha-slave
                # in this scenario, need to sync with the event to see whether the event is finished or not
                if event_type in [constants.EVENT_HOST_UP, constants.EVENT_HOST_DOWN]:
                    if not event_uuid:
                        error = 'no valid event uuid for this event type : %s' % event_type
                        LOG.warning('ignore consul role rebalance request %s, as %s', event_uuid, error)
                        db_api.update_consul_role_rebalance(req.uuid,
                                                            None,
                                                            None,
                                                            constants.RPC_TASK_STATE_ABORTED,
                                                            error)
                        continue

                    host_event = db_api.get_processing_event_by_id(event_uuid)
                    if not host_event:
                        # no such event, so abort this request
                        error = 'no matched host event %s (%s)' % (event_uuid, event_type)
                        LOG.warning('ignore consul role rebalance request %s, as %s', event_uuid, error)
                        db_api.update_consul_role_rebalance(req.uuid,
                                                            None,
                                                            None,
                                                            constants.RPC_TASK_STATE_ABORTED,
                                                            error)
                        continue

                    # check whether host event has finished
                    event_status = host_event.notification_status
                    # masakari notification status can be :
                    # "new", "running", "error", "failed", "ignored", "finished", "aborted"
                    #  will retry when the host event has not been processed
                    if not event_status or event_status in ['new', 'running']:
                        # since the host event has not been handled, can not do rebalance, so keep the request
                        LOG.info('will retry consul role rebalance request, as associated host event %s (%s) has not '
                                 'been processed yet, current status: %s',
                                 event_uuid,
                                 event_type,
                                 event_status)
                        continue

                    # host events has been aborted, no need to rebalance consul role
                    if event_status in ['error', 'failed', 'ignored', 'aborted']:
                        error = "associated host event %s (%s) was in %s status" % (
                            event_uuid, event_type, event_status)
                        LOG.warning('abort consul role rebalance request as the %s', error)
                        db_api.update_consul_role_rebalance(req.uuid,
                                                            None,
                                                            None,
                                                            constants.RPC_TASK_STATE_ABORTED,
                                                            error)
                        continue

                    if event_status != constants.STATE_FINISHED:
                        error = "unexpected status for host event %s : %s" % (event_uuid, event_status)
                        LOG.warning('ignore consul role rebalance request %s (%s), as %s',
                                 host_event.event_uuid,
                                 host_event.event_type,
                                 error)
                        db_api.update_consul_role_rebalance(req.uuid,
                                                            None,
                                                            None,
                                                            constants.RPC_TASK_STATE_ABORTED,
                                                            error)
                        continue

                    # to avoid to send request to a dead host, need to check with nova
                    # is the target host still alive since the original request was created ?
                    if not self._is_nova_service_active(target_host_id):
                        error = 'host %s is inactive ' % target_host_id
                        LOG.warning('ignore consul role rebalance request %s, as %s',
                                 req.uuid,
                                 error)
                        db_api.update_consul_role_rebalance(req.uuid,
                                                            None,
                                                            None,
                                                            constants.RPC_TASK_STATE_ABORTED,
                                                            error)
                        continue

                # the associated host event has been finished, it is ok to start to rebalance roles
                LOG.info('start to process consul role rebalance request %s for event %s with type %s',
                         str(req.uuid), event_uuid, event_type)

                # for all valid events, start to process the rebalance request
                # set db status to running
                db_api.update_consul_role_rebalance(req.uuid,
                                                    None,
                                                    None,
                                                    constants.RPC_TASK_STATE_RUNNING,
                                                    None)

                if req.before_rebalance:
                    LOG.debug('get consul settings from before_rebalance : %s', str(req.before_rebalance))
                    consul_status_before = json.loads(req.before_rebalance)
                    consul_addresses = [x['Addr'] for x in consul_status_before['members']]
                    target_consuls = [x for x in consul_status_before['members'] if x['Name'] == target_host_id]
                    if len(target_consuls) != 1:
                        error = 'no consul status for target host %s' % target_host_id
                        LOG.warning('ignore consul role rebalance request %s, as %s', req.uuid, error)
                        db_api.update_consul_role_rebalance(req.uuid,
                                                            None,
                                                            None,
                                                            constants.RPC_TASK_STATE_ABORTED,
                                                            error)
                        continue
                    join_ips = ','.join(sorted(consul_addresses))
                    cluster_ip = target_consuls[0]['Addr']
                    cluster_name = target_consuls[0]['Tags']['dc']
                else:
                    cluster_name = target_cluster
                    join_ips, cluster_ip_lookup, _ = self._get_join_info(cluster_name)
                    cluster_ip = cluster_ip_lookup[target_host_id]

                rebalance_request = ConsulRoleRebalanceRequest(cluster=target_cluster,
                                                               host_id=target_host_id,
                                                               old_role=target_old_role,
                                                               new_role=target_new_role,
                                                               id=str(req.uuid))

                LOG.info('run consul role rebalance request %s with ip %s to join %s : %s',
                         req.uuid, cluster_ip, join_ips, str(rebalance_request))
                rebalance_result = self._execute_consul_role_rebalance(rebalance_request, cluster_name, cluster_ip,
                                                                       join_ips)
                LOG.info('result of consul role rebalance request %s : %s',
                         str(rebalance_request), str(rebalance_result))
                status_code = rebalance_result['status']
                status_msg = rebalance_result['message']
                consul_status = rebalance_result.get('consul_status', None)
                db_api.update_consul_role_rebalance(req.uuid,
                                                    consul_status,
                                                    None,
                                                    status_code,
                                                    status_msg)
        except Exception as ex:
            error = 'exception : %s' % str(ex)
            LOG.exception('unhandled exception in process_consul_role_rebalance_requests, %s', error)
            if req_id:
                db_api.update_consul_role_rebalance(req_id,
                                                    None,
                                                    None,
                                                    constants.RPC_TASK_STATE_ABORTED,
                                                    error)
        finally:
            # release thread lock
            with self.consul_role_rebalance_processing_lock:
                self.consul_role_rebalance_processing_running = False
        LOG.info('consul role rebalance processing task has finished at %s', str(datetime.utcnow()))

    def process_consul_encryption_configuration(self):
        with self.consul_encryption_processing_lock:
            if self.consul_encryption_processing_running:
                LOG.debug('consul encryption configuration processing task is already running')
                return
            self.consul_encryption_processing_running = True

        LOG.debug('consul encryption configuration processing task start to work at %s', str(datetime.utcnow()))
        try:
            if not self._is_consul_encryption_enabled():
                return

            # CA key and cert only needs to be created once, unless they expired
            ca_changed = False
            if not keyhelper.are_consul_ca_key_cert_pair_exist() or \
                    keyhelper.is_consul_ca_cert_expired():
                ca_changed = keyhelper.create_consul_ca_key_cert_pairs()
                LOG.info('expired CA key and cert are refreshed ? %s', str(ca_changed))
            else:
                LOG.debug('CA key and cert are good for now')

            # svc key and cert need to be created for each enabled cluster
            self._token = self._get_v3_token()
            nova_client = self._get_nova_client()
            clusters = db_api.get_all_active_clusters()
            if not clusters:
                LOG.debug('get_all_active_clusters return none')
                return
            LOG.info('checking certs for %s active clusters : %s', str(len(clusters)), str([x.name for x in clusters]))
            for cluster in clusters:
                cluster_name = cluster.name
                svc_changed = False
                # for ha enabled cluster, when svc key and cert not exist , or expired, then refresh them
                if not keyhelper.are_consul_svc_key_cert_pair_exist(cluster_name) or \
                        keyhelper.is_consul_svc_cert_expired(cluster_name) or \
                        ca_changed:
                    svc_changed = keyhelper.create_consul_svc_key_cert_pairs(cluster_name)
                    LOG.info('expired svc key and cert for cluster %s are refreshed ? %s', cluster_name,
                             str(svc_changed))
                else:
                    LOG.debug('svc key and cert for cluster %s are good for now', cluster_name)

                # get hosts ids from masakari for current cluster
                hosts = masakari.get_nodes_in_segment(self._token, str(cluster_name))
                host_ids = [x['name'] for x in hosts]
                LOG.debug('checking configs for %s hosts in availability_zone %s : %s',
                          str(len(host_ids)), cluster_name, str(host_ids))
                hypervisors = nova_client.hypervisors.list()
                hosts_info = self._resmgr_client.fetch_hosts_details(host_ids, self._token['id'])
                LOG.debug('Details for hosts %s : %s', str(host_ids), str(hosts_info))
                ip_lookup, cluster_ip_lookup, nodes_details = self._get_ips_for_hosts_v2(hypervisors,
                                                                                         host_ids,
                                                                                         hosts_info)
                join_nodes = {}
                # need to push the settings for each host through resmgr
                for host in hosts:
                    host_id = str(host['name'])
                    try:
                        self._token = self._get_v3_token()
                        # get the target host info to make sure the role status is ok
                        resp_role = hosts_info.get(host_id, {}).get('role_settings', {}).get("pf9-ha-slave", {})
                        if not resp_role:
                            LOG.warning('empty settings for role pf9-ha-slave for host %s : %s', str(host_id), str(resp_role))
                            continue
                        LOG.debug('pf9-ha-slave role settings for host %s : %s', host_id, str(resp_role))

                        # issue was found in https://platform9.atlassian.net/browse/IAAS-9826
                        # where after upgraded to 3.9 to 3.10, the ca and svc certs are created
                        # but calls to nova and resmgr failed after the cert creation, the old code
                        # only upgrade resmgr one time, so even though on du certs are not change, but
                        # hosts won't get them.
                        # this fix will always compare certs on du with what hosts have, if they don't
                        # match, will always try to update them
                        gossip_key = keyhelper.get_consul_gossip_encryption_key(cluster_name=str(cluster_name),
                                                                                seed=self._db_pwd)
                        _, ca_cert_content = keyhelper.read_consul_ca_key_cert_pair()
                        svc_key_content, svc_cert_content = keyhelper.read_consul_svc_key_cert_pair(cluster_name)

                        existing_encrypt = resp_role.get('encrypt', "")
                        need_refresh = (existing_encrypt != gossip_key)
                        existing_key_file_content = resp_role.get('key_file_content', "")
                        need_refresh |= (existing_key_file_content != svc_key_content)
                        existing_cert_file_content = resp_role.get('cert_file_content', "")
                        need_refresh |= (existing_cert_file_content != svc_cert_content)
                        existing_ca_file_content = resp_role.get('ca_file_content', "")
                        need_refresh |= (existing_ca_file_content != ca_cert_content)

                        if need_refresh:
                            LOG.info('gossip key for host %s in availability_zone %s needs refresh ? %s',
                                     host_id, cluster_name, str(existing_encrypt != gossip_key))
                            LOG.info('svc key for host %s in availability_zone %s needs refresh ? %s',
                                     host_id, cluster_name, str(existing_key_file_content != svc_key_content))
                            LOG.info('svc cert for host %s in availability_zone %s needs refresh ? %s',
                                     host_id, cluster_name, str(existing_cert_file_content != svc_cert_content))
                            LOG.info('ca cert for host %s in availability_zone %s needs refresh ? %s',
                                     host_id, cluster_name, str(existing_ca_file_content != ca_cert_content))

                            LOG.info('config for host %s in availability_zone %s will be refreshed', str(host_id), cluster_name)
                            valid_ips = [x for x in cluster_ip_lookup.values() if x != '']
                            join_ips = ','.join([str(v) for v in sorted(valid_ips)])
                            join_nodes = {}
                            host_ip = ip_lookup[host_id]
                            # after the pf9-ha-slave role is enabled, the resmgr should have below customized settings:
                            # - cluster_ip, join, ip_address, bootstrap_expect
                            # no matter they are existed or not, always set cluster_ip, join, ip_address.
                            # only the bootstrap_expect can not be determined here
                            data = self._customize_pf9_ha_slave_config(cluster_name, join_ips, host_ip, host_ip, nodes_details)

                            # update resmgr with the new role settings
                            LOG.debug('updating cert settings for cluster %s for host %s with data %s',
                                      cluster_name, host_id, str(data))
                            result = self._resmgr_client.update_role(host_id, "pf9-ha-slave", data, self._token['id'])
                            if not result or result.status_code != requests.codes.ok:
                                LOG.warning('failed to update role settings for host %s', host_id)
                            else:
                                LOG.info('Successfully updated cert settings for cluster %s for host %s',
                                         cluster_name, host_id)
                        else:
                            LOG.debug('key or cert config refresh is not needed for host %s in availability_zone %s',
                                      host_id, cluster_name)
                    except Exception as xes:
                        LOG.exception('unhandled exception in process_consul_encryption_configuration: %s', str(xes))
        except Exception as ex:
            LOG.exception('unhandled exception in process_consul_encryption_configuration : %s', str(ex))
        finally:
            with self.consul_encryption_processing_lock:
                self.consul_encryption_processing_running = False
        LOG.debug('consul encryption configuration processing task has finished at %s', str(datetime.utcnow()))

    def get_common_hosts_configs(self, availability_zone):
        # the result object to be returned
        common_configs = {
            "shared_nfs" : []
        }
        host_ids = []
        without_nfs = []
        with_nfs = []
        try:
            self._token = self._get_v3_token()
            nova_client = self._get_nova_client()

            cluster_exists = db_api.get_cluster(str(availability_zone), raise_exception=False)
            LOG.debug('found clusters %s : %s', str(availability_zone), str(cluster_exists))
            enabled = False
            if cluster_exists:
                enabled = cluster_exists.enabled

            if not enabled:
                # first get all hosts in this availability_zone from nova
                details = nova_client.availability_zones.get_details(availability_zone)
                LOG.debug('host availability_zone details : %s', str(details.__dict__))
                host_ids = details.hosts
            else:
                # get hosts from masakari
                hosts = masakari.get_nodes_in_segment(self._token, str(availability_zone))
                host_ids = [x['name'] for x in hosts]
                LOG.debug('details of hosts from masakari : %s' , str(host_ids))
            LOG.debug('host ids found that associated with availability_zone %s : %s', str(availability_zone), str(host_ids))

            # call resmgr for each host to get configs
            headers = {'X-Auth-Token': self._token['id'], 'Content-Type': 'application/json'}
            # check whether the mounted nfs are same for all hosts
            hosts_info = self._resmgr_client.fetch_hosts_details(host_ids, self._token['id'])
            for host_id in host_ids:
                # object to carry necessary info for making decision
                item = {
                    'host': host_id,
                    'mounted_nfs': [],
                    'nova_instances_path_matched_nfs': [],
                    'nova_instances_path': ''
                }
                # step 1 : get mounted_nfs from resmgr
                host_settings = hosts_info.get(host_id, {})
                if not host_settings:
                    continue
                LOG.debug('settings for host %s : %s', host_id, str(host_settings))
                mounted_nfs_settings = host_settings['extensions'].get('mounted_nfs', None)
                if mounted_nfs_settings:
                    item['mounted_nfs'] = mounted_nfs_settings.get('data', {}).get('mounted', [])
                LOG.debug('mounted nfs settings for host %s : %s', host_id, str(mounted_nfs_settings))
                # step 2 - get instances_path configured for role pf9-ostackhost-neutron
                role_settings = host_settings.get("role_settings", {}).get("pf9-ostackhost-neutron", {})
                if not role_settings:
                    continue
                LOG.debug('settings for role %s : %s', str('pf9-ostackhost-neutron'), str(role_settings))
                instances_path = role_settings.get('instances_path', '')
                LOG.debug('instances_path configuration for host %s : %s', str(host_id), str(instances_path))
                if instances_path:
                    item['nova_instances_path'] = instances_path
                LOG.debug('nova instances path for host %s : %s', host_id, instances_path)
                # step 3 - store found info accordingly
                if len(item['mounted_nfs']) <= 0:
                    LOG.debug('host %s does not nfs mounted', str(host_id))
                    without_nfs.append(item)
                else:
                    if instances_path:
                        path_matched_nfs = \
                            [x for x in item['mounted_nfs'] if x.get('destination', None) in \
                             [instances_path,'/mnt/nfs/instances']]
                        if len(path_matched_nfs) > 0 :
                            item['nova_instances_path_matched_nfs'] = path_matched_nfs
                    with_nfs.append(item)
                LOG.debug('found settings for host %s : %s', host_id, str(item))
        except Exception:
            LOG.exception('unhandled exception when get common hosts configs')
            # raise to caller for handling
            raise

        LOG.debug('hosts without nfs : %s', str(without_nfs))
        LOG.debug('hosts with nfs : %s', str(with_nfs))
        # raise exception with those scenarios
        # - no hosts found for given availability_zone
        # - unable to get settings from resmgr (mounted_nfs, instances_path)
        # - all hosts without shared nfs or matched instances_path
        # - only some hosts with shared nfs and match instances_path
        error = None
        if len(host_ids) <= 0:
            error = "no hosts found for availability_zone %s " % (str(availability_zone))
        else:
            # when rest call to resmgr failed
            if len(without_nfs) == 0 and len(with_nfs) == 0:
                error = 'no settings found for hosts from resgmr: %s' % (str(host_ids))
            # when all hosts have no nfs , or only some hosts have nfs
            if len(without_nfs) > 0 or len(with_nfs) <= 0 or len(with_nfs) != len(host_ids):
                ids_with_nfs = [] if len(with_nfs) <= 0 else [x['host'] for x in with_nfs]
                ids_without_nfs = [x['host'] for x in without_nfs]
                error = 'hosts %s have nfs mounted but hosts %s do not' % (str(ids_with_nfs), str(ids_without_nfs))
            # when not all hosts with nfs that matches nova instances_path
            path_matched_nfs = [x for x in with_nfs if len(x['nova_instances_path_matched_nfs']) > 0]
            if len(host_ids) != len(path_matched_nfs):
                ids_with_matched_path = [x['host'] for x in path_matched_nfs]
                error = 'only hosts %s from %s have nfs setting matches instances_path %s : %s' % \
                        (str(ids_with_matched_path), str(host_ids), str(instances_path), str(path_matched_nfs))
        if error:
            LOG.debug(error)
            raise ha_exceptions.NoCommonSharedNfsException(error)

        # find common nfs items that matches nova settings on all hosts
        common_nfs_set = with_nfs[0]['nova_instances_path_matched_nfs']
        for item in with_nfs:
            common_nfs_set = self._get_common_nfs_set(common_nfs_set, item['nova_instances_path_matched_nfs'])

        # when no common mounted nfs on all hosts
        if len(common_nfs_set) <= 0:
            error = 'no common nfs mounted on all hosts %s' % (str(host_ids))
            LOG.debug(error)
            raise ha_exceptions.NoCommonSharedNfsException(error)

        LOG.debug('mounted nfs set of all hosts %s: %s', str(host_ids), str(common_nfs_set))
        common_configs['shared_nfs'] = list(common_nfs_set)
        LOG.debug('common configs for hosts in availability_zone %s : %s', str(availability_zone), str(common_configs))
        return common_configs

    def _get_common_nfs_set(self, common_set, set_for_check):
        common = []
        for comm in common_set:
            for check in set_for_check:
                if comm['source'] == check['source'] and \
                        comm['permissions'] == check['permissions'] and \
                        comm['destination'] == check['destination'] and \
                        comm['destination_exist'] == check['destination_exist'] and \
                        comm['fstype'] == check['fstype']:
                    common.append(comm)
        return common

    def refresh_consul_status(self):
        report = {}
        controller = get_rebalance_controller(self._config)
        # send the cmd to all active clusters
        active_clusters = db_api.get_all_active_clusters()
        LOG.debug('refresh consl status for clusters : %s', str(active_clusters))
        if not active_clusters:
            return report

        for cluster in active_clusters:
            cluster_name = cluster.name
            LOG.info('refresh consul status for cluster %s', str(cluster_name))
            status = self._get_latest_consul_status(cluster_name)
            LOG.info('refreshed consul report for cluster %s: %s', str(cluster_name), str(status))
            if status:
                report[cluster_name] = status
            else:
                report[cluster_name] = {}
        return report

    def get_active_clusters(self, id=None):
        results = []
        try:
            active_clusters = db_api.get_all_active_clusters()
            if id:
                active_clusters = [cluster for cluster in active_clusters if cluster.id == id]

            self._token = self._get_v3_token()
            for cluster in active_clusters:
                name = cluster.name
                nodes = masakari.get_nodes_in_segment(self._token, name)
                hosts = []
                for node in nodes:
                    hosts.append({
                        'uuid': node['name'],
                        "on_maintenance": node['on_maintenance'],
                    })
                cluster_info = {
                    'id': cluster.id,
                    'status': cluster.status,
                    'enabled': cluster.enabled,
                    'created_at': cluster.created_at,
                    'name': cluster.name,
                    'hosts': hosts
                }
                results.append(cluster_info)
        except Exception as e:
            LOG.exception('unhandled exception when get clusters: %s', str(e))

        if id:
            if len(results) > 0:
                return results[0]
            else:
                return {}

        return results

    def set_consul_agent_role(self, availability_zone, host_id, agent_role):
        LOG.debug('request consul agent on host %s to run in role %s',
                  host_id, agent_role)
        nova_client = self._get_nova_client()
        cluster_name = str(availability_zone)
        # 1 availability_zone needs to be existed, if not, throw AggregateNotFound
        availability_zone = self._get_availability_zone(nova_client, availability_zone)
        LOG.debug('found host availability_zone %s : %s', cluster_name, str(availability_zone))
        # 2 cluster need to be enabled, if not, throw ClusterNotFound
        cluster = db_api.get_cluster(str(availability_zone),
                                      read_deleted=False,
                                      raise_exception=True)
        if cluster.status != constants.HA_STATE_ENABLED:
            LOG.warning('Ignoring set_consul_agent_role as cluster %s status is not enabled currently.',
                        cluster_name)
            return
        LOG.debug('found cluster %s : %s', str(availability_zone), str(cluster))
        # 3 host needs to be in the availability_zone
        host_ids = availability_zone.hosts
        if str(host_id) not in set(host_ids):
            LOG.warning('host %s not in cluster %s : %s', host_id,
                        cluster_name, str(host_ids))
            raise ha_exceptions.HostNotInCluster(host_id, cluster_name)
        # 4 host needs to have role 'pf9-ha-slave'
        self._token = self._get_v3_token()
        slave_role = 'pf9-ha-slave'
        host_settings = self._resmgr_client.get_host_info(host_id,
                                                          self._token['id'])
        LOG.debug('settings of role %s for host %s : %s', slave_role,
                  host_id, str(host_settings))
        if slave_role not in host_settings['roles']:
            raise ha_exceptions.RoleNotExists(host_id, slave_role)
        # 5 existing role settings should have field 'bootstrap_expect'
        # better to check latest consul status
        current_role = None
        LOG.debug('try to get agent role from latest consul status')
        consul_status = self._get_latest_consul_status(cluster_name)
        target_hosts = [x for x in consul_status['members'] if x['Name'] == host_id]
        if target_hosts:
            real_role = target_hosts[0]['Tags']['role']
            # 'consul' - server role
            # 'node' - client role
            if real_role == 'consul':
                current_role = constants.CONSUL_ROLE_SERVER
            elif real_role == 'node':
                current_role = constants.CONSUL_ROLE_CLIENT
            LOG.debug('agent role for host %s from latest consul status is %s', host_id, current_role)
        if not current_role:
            LOG.debug('try to get agent role from resmgr when unable to get agent role from latest consul status')
            role_settings = self._resmgr_client.get_role_settings(host_id,
                                                                  slave_role,
                                                                  self._token['id'])
            if 'bootstrap_expect' not in role_settings:
                raise ha_exceptions.RoleSettingsNotFound(host_id, slave_role, 'bootstrap_expect')
            LOG.debug('get current agent role for host %s by checking resmgr settings : %s', str(host_id), str(role_settings))
            if role_settings['bootstrap_expect'] == self._consul_bootstrap_expect:
                current_role = constants.CONSUL_ROLE_SERVER
            else:
                current_role = constants.CONSUL_ROLE_CLIENT
        LOG.debug('consul agent on host %s is current in role %s', host_id, current_role)
        if agent_role == current_role:
            LOG.debug('consul agent on host %s is already in %s role : %s',
                      host_id,
                      agent_role,
                      str(agent_role))
            return
        # create reblance request
        self._report_consul_rebalance(cluster_name=str(availability_zone),
                                      event_type=constants.EVENT_CONSUL_CHANGE,
                                      candidates= [{
                                          'host': host_id,
                                          'old_role': current_role,
                                          'new_role': agent_role,
                                          'member': {}
                                      }]
                                      )
        LOG.info('reported consul agent role change for host %s from %s to %s', host_id, current_role, agent_role)
        
        
    # Get host-ids within same cluster as a host
    def get_hosts_with_same_cluster(self, host_id):
        host_ids = []
        headers = {"X-AUTH-TOKEN": self._token['id']}
        url = 'http://nova-api.' + self._du_name + '.svc.cluster.local:8774/v2.1/os-aggregates'
        try:
            response = requests.get(url, headers=headers,timeout=NOVA_REQ_TIMEOUT)
        except requests.exceptions.Timeout:
            return host_ids
        if response.status_code == 200:
            data = response.json()
            if 'aggregates' in data:
                filtered_aggregates = list(filter(lambda x: x["availability_zone"]!=None and host_id in x["hosts"], data['aggregates']))
                host_ids = filtered_aggregates[0]['hosts'] if filtered_aggregates else []
        return host_ids

    # Get ip from host-id
    def get_ip_from_host_id(self, host_id):
        ip=""
        if self._hypervisor_details == "":
            headers = {"X-AUTH-TOKEN": self._token['id']}
            # We dont know whats the hypervisor id so be brute force it
            url = 'http://nova-api.' + self._du_name + '.svc.cluster.local:8774/v2.1/os-hypervisors/detail'
            try:
                response = requests.get(url, headers=headers, timeout=NOVA_REQ_TIMEOUT)
            except requests.exceptions.Timeout:
                return ip
            if response.status_code == 200:
                data = response.json()
        else:
            data = self._hypervisor_details
        if 'hypervisors' in data:
            if len(data['hypervisors']) > 0:
                ip = list(filter(lambda x:x['service']['host'] == host_id, data['hypervisors']))[0]['host_ip']
        return ip

    # Generate list of ips for vmha agent to monty
    def generate_ip_list(self, host_id):
        host_ids = self.get_hosts_with_same_cluster(host_id)
        if host_ids == []:
            return {}
        ip_map = {}
        self._hypervisor_details=""
        for host in filter(lambda x: x!=host_id,host_ids):
            # Make this as such only one request is used and then we parse it and get ips for different hosts
            ip_map[host]=self.get_ip_from_host_id(host)
            if not ip_map[host]:
                return {}

        # Make nCr type combinations and choose amongst them randomly
        # if n is less than r, we can't make combinations
        if len(host_ids) - 1 > VMHA_MAX_FANOUT:
            ip_pool = list(combinations(list(ip_map.keys()), VMHA_MAX_FANOUT))
            final_map = {}
            for key in ip_pool[randint(0,len(ip_pool)-1)]:
                final_map[key] = ip_map[key]
            return final_map
        return ip_map
    
    # Check from resmgr if the cluster has vmha enabled
    def check_vmha_enabled_on_resmgr(self,cluster_name):
        headers = {"X-AUTH-TOKEN": self._token['id']}
        url = 'http://resmgr.' + self._du_name + '.svc.cluster.local:18083/v2/clusters/' + cluster_name
        try:
            response = requests.get(url, headers=headers,timeout=NOVA_REQ_TIMEOUT)
        except requests.exceptions.Timeout:
            LOG.debug("request failed with timeout on resmgr")
            return False
        try:
            body = response.json()
            LOG.debug("response from resmgr %s", body)
        except Exception as e:
            LOG.warning("unable to unpack reponse from resmgr")
            return False
        if 'vmHighAvailability' in body:
            if 'enabled' in body['vmHighAvailability']:
                if body['vmHighAvailability']['enabled']:
                    return True
        return False



def get_provider(config):
    db_api.init(config)
    return NovaProvider(config)
