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
from collections import defaultdict
from datetime import datetime
from datetime import timedelta

import requests
from keystoneauth1 import loading
from keystoneauth1 import session
from novaclient import client
from novaclient import exceptions

from hamgr.common import masakari
from hamgr.common import utils
from hamgr.db import api as db_api
from hamgr.notification.manager import get_notification_manager
from hamgr.notification.model import Notification
from hamgr.providers.provider import Provider
from hamgr.rebalance import get_rebalance_controller
from shared import constants
from shared.exceptions import ha_exceptions
from shared.messages.rebalance_request import ConsulRoleRebalanceRequest
from shared.messages.consul_request import ConsulRefreshRequest
from hamgr.common import key_helper as keyhelper

LOG = logging.getLogger(__name__)


class NovaProvider(Provider):
    def __init__(self, config):
        self._username = config.get('keystone_middleware', 'admin_user')
        self._passwd = config.get('keystone_middleware', 'admin_password')
        self._auth_uri = config.get('keystone_middleware', 'auth_uri')
        self._auth_uri_v3 = "%s/v3" % self._auth_uri
        self._tenant = config.get('keystone_middleware', 'admin_tenant_name')
        self._region = config.get('nova', 'region')
        self._resmgr_endpoint = config.get('DEFAULT', 'resmgr_endpoint')
        self._max_auth_wait_seconds = 300
        self._db_uri = config.get("database", "sqlconnectURI")
        self._db_pwd = self._db_uri
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
            LOG.warn("'max_role_auth_wait_seconds' not exist in config file " \
                     "or its value not integer, use default 300")
        if self._max_auth_wait_seconds <= 0:
            self._max_auth_wait_seconds = 300
        self._max_role_converged_wait_seconds = 900
        try:
            self._max_role_converged_wait_seconds = config.getint('DEFAULT', 'max_role_converged_wait_seconds')
        except:
            LOG.warn("'max_role_converged_wait_seconds' not exist in config file " \
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
        # salve failed to report the event to hamgr du to network issue,
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
        self.aggregate_task_lock = threading.Lock()
        self.aggregate_task_running = False
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

    def _get_v3_token(self):
        self._token = utils.get_token(self._auth_uri_v3,
                                      self._tenant, self._username,
                                      self._passwd, self._token)
        return self._token

    def _is_consul_encryption_enabled(self):
        enable = self._config.getboolean('DEFAULT',
                                         'enable_consul_encryption') \
            if self._config.has_option("DEFAULT", "enable_consul_encryption") \
            else False
        return enable

    def _toggle_host_maintenance_state(self, host_id, segment_name,
                                       on_maintenance):
        LOG.debug(
            'toggle masakari host %s in segment %s to maintenance state %s',
            str(host_id), str(segment_name), str(on_maintenance))
        try:
            self._token = self._get_v3_token()
            masakari.update_host_maintenance(self._token, host_id, segment_name,
                                             on_maintenance)
            LOG.info(
                'maintenance state for host %s in masakari segment %s has changed to %s',
                str(host_id), str(segment_name), str(on_maintenance))

        except Exception as e:
            LOG.error('failed to toggle maintenance for host %s to %s , '
                      'error : %s', host_id, str(on_maintenance), str(e))

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
                LOG.debug('no unhandled processing events found')
                return
            events = sorted(unhandled_events, key=lambda x: x.event_time)
            if not events or len(events) <= 0:
                LOG.debug('no unhandled processing events to process')
                return
            LOG.debug('found unhandled events : %s', str(events))
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
                    if event.notification_uuid and event.notification_status != constants.STATE_FINISHED:
                        state = masakari.get_notification_status(self._token, event.notification_uuid)
                        LOG.info('event %s is in state : %s , corresponding masakari notification state : %s',
                                 str(event_uuid), str(event.notification_status), str(state))
                    db_api.update_processing_event_with_notification(
                        event_uuid, None, None,
                        constants.STATE_ABORTED,
                        'event happened long time ago : %s' % str(event_time)
                    )
                    LOG.warn('event %s is aborted from state %s , as it is too stale',
                             str(event_uuid), str(event.notification_status))
                    continue

                # check whether host is active in nova
                LOG.debug('try to check whether host %s in nova is active ', str(host_name))
                is_active = self._is_nova_service_active(host_name, nova_client=nova_client)
                LOG.debug('is host %s active in nova ? %s', host_name,
                          str(is_active))

                # for 'host-down' event
                if event_type == constants.EVENT_HOST_DOWN:
                    if not event.notification_uuid:
                        if is_active:
                            LOG.warn('still report host_down event to masakari, even the host %s is alive in nova',
                                     event_uuid)
                        #
                        # when masakari received host-down notification, the host in masakari will be set on maintenance
                        # if a host in masakari is makred as on maintenance, it will give 409 error if you try to
                        # create another host-down notification for it. we have another task to reset the maintenance
                        # status when the masakari failover segment is not under recovery. so we can wait that complete
                        # here before trying to create another notification. this will avoid the 409 error.
                        #
                        if not masakari.is_failover_segment_under_recovery(self._token, cluster.name):
                            LOG.info('in event %s %s for host %s, try to reset maintenance state in masakari '
                                     'which is active in nova', event_type, event_uuid, host_name)
                            self._toggle_host_maintenance_state(host_name, cluster.name, False)
                            LOG.info('try to check whether host %s in cluster %s in masakari is on maintenance',
                                     str(host_name), str(cluster.name))
                            masakari_host_on_maintenance = masakari.is_host_on_maintenance(self._token, host_name,
                                                                                           cluster.name)
                            if masakari_host_on_maintenance:
                                LOG.warn('%s event %s for host %s, which in masakari segment %s '
                                         'is on maintenance', event_type, str(event_uuid),
                                         host_name, cluster.name)
                        LOG.debug('try to report event %s id %s for host %s to masakari',
                                 str(event_type), str(event_uuid), host_name)
                        notification_obj = self._report_event_to_masakari(event)
                        if notification_obj:
                            LOG.info('event %s for host %s is reported to masakari, notification id %s', event_uuid,
                                     host_name, str(notification_obj))
                            event.notification_uuid = notification_obj['notification']['notification_uuid']
                            state = self._tracking_masakari_notification(event)
                            LOG.info('event %s for host %s is updated with notification state %s', event_uuid,
                                     host_name, state)
                        else:
                            LOG.warn('failed to report event %s for host %s to masakari : %s',
                                     str(event_type), str(host_name), str(event))
                    else:
                        state = self._tracking_masakari_notification(event)
                        LOG.info('event %s for host %s is updated with notification state %s', event_uuid,
                                 host_name, state)
                elif event_type == constants.EVENT_HOST_UP:
                    if is_active:
                        if not masakari.is_failover_segment_under_recovery(self._token, cluster.name):
                            LOG.info('in event %s %s for host %s, try to reset maintenance state in masakari '
                                     'which is active in nova', event_type, event_uuid, host_name)
                            self._toggle_host_maintenance_state(host_name, cluster.name, False)
                        else:
                            LOG.info('need to retry toggle maintence state for host %s as cluster %s under recovery ',
                                     str(host_name), str(cluster.name))

                        db_api.update_processing_event_with_notification(event_uuid,
                                                                         None,
                                                                         None,
                                                                         constants.STATE_FINISHED)
                        LOG.info('event %s for host %s is marked as %s', event_uuid,
                                 host_name, constants.STATE_FINISHED)
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
        except Exception:
            LOG.exception('unhandled exception when process host events')
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
            if notification_obj and notification_obj.get('notification',
                                                         None):
                return notification_obj
        except Exception:
            LOG.error('failed to create masakari notification, error', exc_info=True)
        LOG.warn('no valid masakari notification was created for event %s for host %s: %s', str(event.event_type),
                 str(event.host_name), str(notification_obj))
        return None

    def _tracking_masakari_notification(self, event):
        if not event:
            return None
        self._token = self._get_v3_token()
        notification_uuid = event.notification_uuid
        state = masakari.get_notification_status(self._token, notification_uuid)
        # save the state to event
        LOG.info('updating event %s with masakari notification %s status : %s',
                 str(event.event_uuid), str(notification_uuid), str(state))
        db_api.update_processing_event_with_notification(event.event_uuid,
                                                         event.notification_uuid,
                                                         event.notification_created,
                                                         state)
        return state

    def process_host_aggregate_changes(self):
        try:
            with self.aggregate_task_lock:
                if self.aggregate_task_running:
                    LOG.debug('Check host aggregates for changes task already running')
                    return
                self.aggregate_task_running = True

            LOG.debug('host aggregate change processing task starts to run at %s', str(datetime.utcnow()))
            self._token = self._get_v3_token()

            nova_client = self._get_nova_client()

            # ---------------------------------------------------------------
            # scenarios
            #  1) nova aggregate exists   [ sync from nova aggregate to ha system ]
            #     1.1) corresponding ha cluster exists
            #        1.1.1) masakari (failover segment, hosts) does not match hosts from nova aggregate
            #             X = {hosts in masakari} , Y = {host in nova aggregate}, Z = { common hosts in X and Y}
            #            1.1.1.1) X and Y have no common
            #                 remove all masakari hosts, then add nova aggregate hosts to masakari
            #            1.1.1.2) X and Y have some in common
            #                 remove additional hosts (X-Z) from masakari, add additional host (Y-Z) to masakari
            #                 this also works for
            #                   - Y is subset of X : just add additional hosts to masakari
            #                   - X is subset of Y : just remove additional hosts from masakari
            #        1.1.2) masakari (failover segment, hosts) matches hosts from nova aggregate
            #           no action needed
            #    1.2) corresponding ha cluster does not exist
            #        need to create a disabled ha cluster , and add masakari failover segment and hosts
            #  2) nova aggregate does not exists  [ build nova aggregates from ha system ]
            #     [ not sure whether this is valid scenario, because create nova aggregates needs to know
            #      availability zone infomation , especially when there is no availability zone . ]
            # ----------------------------------------------------------
            # first reconcile hosts between nova aggregates and masakari
            # to make sure real world settings on nova are synced to
            # masakari for vm evacuation management
            # ----------------------------------------------------------
            # find out all host aggregates from nova

            nova_active_aggres = nova_client.aggregates.list()
            LOG.debug('active nova aggregates : %s',str([x.id for x in nova_active_aggres]))
            hamgr_all_clusters = db_api.get_all_clusters()
            LOG.debug('all vmha clusters : %s', str(hamgr_all_clusters))
            # because hosts in nova aggregate reflect the real word settings for HA, so need to reconcile hosts in
            # each aggregate from nova to hamgr and masakari
            # for each active nova aggregate
            #  if there is corresponding ha cluster, then update hosts in masakari with hosts from this nova aggregate
            #  else create ha cluster in hamgr , and in masakari , but put the cluster in disabled state
            for active_agg in nova_active_aggres:
                nova_agg_id = active_agg.id
                nova_agg_name = active_agg.name
                nova_agg_hosts = active_agg.hosts
                LOG.info('%s host ids from nova aggregate id %s name %s: %s', str(len(nova_agg_hosts)),
                         str(nova_agg_id), str(nova_agg_name), str(nova_agg_hosts))
                # is there a ha cluster for this id ?
                hamgr_clusters_for_agg = [x for x in hamgr_all_clusters if x.name == str(nova_agg_id)]
                LOG.info('found hamgr clusters for nova aggregate %s : %s', str(nova_agg_id), str(hamgr_clusters_for_agg))
                if len(hamgr_clusters_for_agg) <= 0:
                    LOG.info('there is no matched vmha cluster with name matches to nova aggregate id %s', str(nova_agg_id))
                    continue
                elif len(hamgr_clusters_for_agg) > 1:
                    LOG.warn('there are more than 1 vmha clusters with name matches to nova aggregate id %s : %s',
                             str(nova_agg_id), str(hamgr_clusters_for_agg))

                # we only support one-to-one mapping between vmha cluster to nova aggregate
                current_cluster = hamgr_clusters_for_agg[0]
                LOG.debug('pick the first matched vmha cluster %s from %s', str(current_cluster), str(hamgr_clusters_for_agg))

                # reconcile hosts to hamgr and masakari
                cluster_name = current_cluster.name
                cluster_enabled = current_cluster.enabled
                cluster_status = current_cluster.status
                cluaster_task_state = current_cluster.task_state

                # ideally the hosts from masakari is the same as hosts in nova aggregates
                # if more hosts in nova aggregate, means they exist in nova aggregate but not in masakari
                # if more hosts in masakari, means some of they are removed from nova aggregate

                masakari_hosts = []

                # add hosts found in nova aggregate but not in masakari segment
                if masakari.is_failover_segment_exist(self._token, str(cluster_name)):
                    masakari_hosts = masakari.get_nodes_in_segment(self._token, str(cluster_name))
                masakari_hids = [x['name'] for x in masakari_hosts]
                common_ids = set(nova_agg_hosts).intersection(set(masakari_hids))
                nova_delta_ids = set(nova_agg_hosts) - set(common_ids)
                if len(nova_delta_ids) > 0:
                    LOG.info('found new hosts added into nova aggregate %s : %s', str(nova_agg_id), str(nova_delta_ids))
                    # need to add those additional hosts into masakari
                    if masakari.is_failover_segment_under_recovery(self._token, str(cluster_name)):
                        LOG.info('will retry to add hosts %s to masakari segment %s, '
                                 'as the segment is under recovery',
                                 str(nova_delta_ids), str(cluster_name))
                    else:
                        # hosts already in nova aggregate so only need to add to masakari
                        LOG.info('add additional hosts %s found in nova aggregate %s to masakari segment %s',
                                 str(nova_delta_ids), nova_agg_name, str(cluster_name))
                        masakari.add_hosts_to_failover_segment(self._token,
                                                               str(cluster_name),
                                                               nova_delta_ids)
                        if cluster_enabled:
                            # in order to make the consul cluster work, by default we set all hosts as consul server
                            # and trigger a role rebalance request
                            LOG.info('add ha slave role to added host : %s', str(nova_delta_ids))
                            self._add_ha_slave_if_not_exist(cluster_name, nova_delta_ids, 'server', common_ids)
                            # trigger a consul role rebalance request
                            time.sleep(30)
                            self._rebalance_consul_roles_if_needed(cluster_name, constants.EVENT_HOST_ADDED)

                masakari_hosts=[]
                # remove host found in masakari segment but not in nova aggregate
                if masakari.is_failover_segment_exist(self._token, str(cluster_name)):
                    masakari_hosts = masakari.get_nodes_in_segment(self._token, str(cluster_name))
                masakari_hids = [x['name'] for x in masakari_hosts]
                common_ids = set(nova_agg_hosts).intersection(set(masakari_hids))
                masakari_delta_ids = set(masakari_hids) - set(common_ids)
                LOG.debug('cluster name  %s, masakari hids : %s, nova agg id %s name %s hids %s, common ids %s, '
                          'masakari additional hids %s, size:%s', str(cluster_name), str(masakari_hids),
                          str(nova_agg_id), str(nova_agg_name), str(nova_agg_hosts), str(common_ids),
                          str(masakari_delta_ids), str(len(masakari_delta_ids)))
                if len(masakari_delta_ids) > 0:
                    LOG.info('found hosts removed from nova aggregate id %s name %s : %s', str(nova_agg_id),
                             str(nova_agg_name), str(masakari_delta_ids))
                    # need to remove those additional hosts found in masakari
                    if masakari.is_failover_segment_under_recovery(self._token, str(cluster_name)):
                        LOG.info('will retry to remove hosts %s from masakari segment %s, '
                                 'as the segment is under recovery',
                                 str(masakari_delta_ids), str(cluster_name))
                    else:
                        LOG.info('remove additional hosts %s found in masakari from segment %s ',
                                 str(masakari_delta_ids),
                                 str(cluster_name))
                        masakari.delete_hosts_from_failover_segment(self._token,
                                                                    str(cluster_name),
                                                                    masakari_delta_ids)
                        if cluster_enabled:
                            LOG.info('remove ha salve role from removed hosts : %s', str(masakari_delta_ids))
                            self._remove_ha_slave_if_exist(masakari_delta_ids)
                            LOG.info('try to rebalance consul roles after removed hosts : %s',
                                     str(masakari_delta_ids))
                            time.sleep(30)
                            self._rebalance_consul_roles_if_needed(cluster_name, constants.EVENT_HOST_REMOVED)

                # when cluster is enabled, make sure pf9-ha-slave role is on all active hosts
                # ----------------------------------------------------------
                # then reconcile masakari hosts and pf9-ha-slave on those
                # hosts for active ha clusters to make sure the
                # pf9-ha-slave role is installed on hosts in all active
                # clusters to provide monitoring ability
                # ----------------------------------------------------------
                if (not cluster_enabled) and \
                        (cluster_status in [constants.HA_STATE_DISABLED, constants.HA_STATE_ERROR]) and \
                        (cluaster_task_state in [constants.TASK_COMPLETED, constants.TASK_ERROR_REMOVING]):
                    # if nova aggregate exist but vmha cluster is disabled, try to remove pf9-ha-slave
                    # in case previous action(disable vmha, or hostagent failed) failed to remove the pf9-ha-slave
                    # this will make sure pf9-ha-slave not left on hosts when vmha disabled for a nova aggregate
                    try:
                        LOG.debug('cluster (name %s, status %s, task state %s) is not enabled, '
                                 'try to remove pf9-ha-slave roles from hosts',
                                 str(cluster_name), str(cluster_status), str(cluaster_task_state))
                        self._deauth(nova_agg_hosts)
                    except Exception:
                        LOG.warn('remove pf9-ha-slave on hosts %s failed, will retry later', str(nova_agg_hosts))

                    continue

                LOG.debug('cluster %s is enabled, now checking slave role ', str(cluster_name))
                aggregate_id = current_cluster.name
                segment_name = str(cluster_name)
                aggregate = self._get_aggregate(nova_client, aggregate_id)
                aggregate_host_ids = set(aggregate.hosts)
                new_active_host_ids = set()
                new_inactive_host_ids = set()
                existing_active_host_ids = set()
                existing_inactive_host_ids = set()
                removed_host_ids = set()
                nova_active_host_ids = set()
                try:
                    nodes = masakari.get_nodes_in_segment(self._token,
                                                          segment_name)
                    masakari_host_ids = set([node['name'] for node in nodes])

                    LOG.debug("hosts records from nova : %s",
                              ','.join(list(aggregate_host_ids)))
                    LOG.debug("hosts records from masakari : %s",
                              ','.join(list(masakari_host_ids)))

                    for host in aggregate_host_ids:
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

                    removed_host_ids = masakari_host_ids - aggregate_host_ids

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
                    for hid in nova_active_host_ids:
                        # already know the host is active in nova
                        xhosts = [x for x in nodes if str(x['name']) == str(hid)]
                        LOG.debug('matched active nova host %s : %s', str(hid), str(xhosts))
                        if len(xhosts) == 1 and xhosts[0]['on_maintenance']:
                            LOG.debug('toggle maintenance for host %s', str(hid))
                            # need to unlock the host when the segment is not in use, otherwise will retry later
                            if not masakari.is_failover_segment_under_recovery(self._token, segment_name):
                                LOG.info('active host %s was found in maintenance state, reset maintentance state',
                                         str(xhosts[0]['name']))
                                self._toggle_host_maintenance_state(hid, segment_name, False)

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
                        current_roles = self._get_roles_for_hosts(nova_active_host_ids)
                        LOG.debug("found roles : %s", str(current_roles))
                        role_missed_host_ids = set()

                        for hid in nova_active_host_ids:
                            h_roles = current_roles[hid]
                            LOG.debug("host %s  has roles : %s", str(hid), ','.join(h_roles))
                            if "pf9-ha-slave" not in h_roles:
                                role_missed_host_ids.add(hid)

                        if len(role_missed_host_ids) > 0:
                            txt_ids = ','.join(list(role_missed_host_ids))
                            LOG.info("assign ha-slave role for hosts which is missing pf9-ha-slave: %s", txt_ids)
                            # hosts are not changed but just missing pf9-ha-slave, it is ok to just re-assign roles
                            self._assign_roles(nova_client, aggregate_id, nova_active_host_ids)
                            LOG.debug("ha-slave roles are assigned to : %s", txt_ids)
                        else:
                            LOG.info("all hosts already have pf9-ha-slave role")

                    if len(new_active_host_ids) > 0:
                        # need to put pf-ha-slave role on them
                        LOG.info('new active hosts are added into aggregate %s but not in masakari : %s ',
                                 str(aggregate_id),
                                 str(new_active_host_ids))
                        # same here, since we don't know the consul roles on existing hosts, so just
                        # make thoese hosts as consul server, and trigger a role rebalance request
                        self._add_ha_slave_if_not_exist(str(aggregate_id), new_active_host_ids, 'server',
                                                        masakari_host_ids)
                        # trigger consul role rebalance request
                        time.sleep(30)
                        self._rebalance_consul_roles_if_needed(cluster_name, constants.EVENT_HOST_ADDED)

                    if len(new_inactive_host_ids) > 0:
                        # need to wait for them become active so can put pf9-ha-slave role on them
                        LOG.info('new inactive hosts are added into aggregate %s but '
                                 'not in masakari : %s, wait for hosts to become active',
                                 segment_name,
                                 str(new_inactive_host_ids))

                    if len(removed_host_ids) > 0:
                        LOG.info('hosts are removed from nova aggregate %s : %s, now remove pf9-ha-slave role',
                                 aggregate_id, str(removed_host_ids))
                        self._remove_ha_slave_if_exist(removed_host_ids)
                        LOG.info('try to rebalance consul roles after removed hosts : %s', str(removed_host_ids))
                        time.sleep(30)
                        self._rebalance_consul_roles_if_needed(cluster_name, constants.EVENT_HOST_REMOVED)

                    if len(existing_inactive_host_ids) > 0:
                        LOG.warn('existing inactive hosts in segment %s : %s , wait for hosts to become active',
                                 segment_name, str(existing_inactive_host_ids))

                except ha_exceptions.ClusterBusy:
                    pass
                except ha_exceptions.InsufficientHosts:
                    LOG.warn('Detected number of aggregate %s hosts is '
                             'insufficient', aggregate_id)
                except ha_exceptions.SegmentNotFound:
                    LOG.warn('Masakari segment for cluster: %s was not found',
                             current_cluster.name)
                except ha_exceptions.HostPartOfCluster:
                    LOG.error("Not enabling cluster as cluster hosts are part of"
                              "another cluster")
                except Exception as e:
                    LOG.exception('Exception while processing aggregate %s: %s',
                                  aggregate_id, e)
        except Exception as e:
            LOG.exception('unhandled exception in host aggregate change processing task : %s', str(e))
        finally:
            with self.aggregate_task_lock:
                LOG.debug('Aggregate changes task completed')
                self.aggregate_task_running = False
        LOG.debug('host aggregate change processing task has finished at %s', str(datetime.utcnow()))

    def _rebalance_consul_roles_if_needed(self, cluster_name, event_type, event_uuid=None):
        if event_type not in constants.HOST_EVENTS:
            LOG.warn('ignore invalided rebalance consul role for requested event type %s', event_type)
            return
        controller = get_rebalance_controller(self._config)
        request = ConsulRefreshRequest(cluster=cluster_name, cmd='refresh')
        status = controller.ask_for_consul_cluster_status(request)
        LOG.info('current consul report: %s', str(status))
        if status and status['status'] == constants.RPC_TASK_STATE_FINISHED:
            report = json.loads(status['report'])
            LOG.debug('consul status refresh report : %s', str(report))

            # rather than directly rebalance, just create the rebalance requests and let rebalance processor
            # drive the whole process
            LOG.info('report consul rebalance for event %s with consul report : %s', event_type, str(report))
            self._report_consul_rebalance(event_type, event_uuid, report)

            # we can also record the consul status
            try:
                if not report:
                    return
                reportedBy = report.get('reportedBy', None)
                leader = report.get('leader', None)
                peers = report.get('peers', None)
                members = report.get('members', None)
                kv = report.get('kv', None)
                joins = report.get('joins', None)
                LOG.debug('refreshed consul status : reportedBy %s , leader %s, peers %s, members %s, kv %s, joins %s',
                          str(reportedBy),str(leader), str(peers), str(members), str(kv), str(joins))
                if not reportedBy:
                    LOG.warn('consul refresh report has no info for reportedBy')
                    return

                nova_client = self._get_nova_client()
                clusters = db_api.get_all_active_clusters()
                if not clusters:
                    return
                target_cluster_id = None
                target_cluster_name = None
                for cluster in clusters:
                    aggregate_id = cluster.name
                    aggregate = self._get_aggregate(nova_client, aggregate_id)
                    hosts = set(aggregate.hosts)
                    if str(reportedBy) in hosts:
                        target_cluster_id = cluster.id
                        target_cluster_name = cluster.name
                        break
                if not target_cluster_id:
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
            except Exception as ex:
                LOG.warn('unhandled exception when try to add new consul status : %s', str(ex))
        else:
            LOG.warn('null consul cluster report or the request has not been processed by host : %s', str(status))

    def _execute_consul_role_rebalance(self, rebalance_request, cluster_name, host_ip, join_ips):
        target_host_id = rebalance_request['host_id']
        target_old_role = rebalance_request['old_role']
        target_new_role = rebalance_request['new_role']

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
        url = '%s/v1/hosts' % self._resmgr_endpoint
        self._token = self._get_v3_token()
        headers = {'X-Auth-Token': self._token['id'], 'Content-Type': 'application/json'}
        # get the target host info to make sure the role status is ok
        req_url = url + "/" + target_host_id
        result = requests.get(req_url, headers=headers)
        data = result.json()

        if result.status_code != requests.codes.ok:
            LOG.warn('call to %s was unsuccessful, result : %s', req_url, str(result))
        if 'pf9-ha-slave' not in data['roles']:
            error = 'host %s does not have pf9-ha-slave role' % target_host_id
            return {'status': constants.RPC_TASK_STATE_ABORTED, 'error': error}

        data = self._customize_pf9_ha_slave_config(cluster_name, join_ips, host_ip, host_ip)
        # step 1 : modify 'bootstrap_expect' base on the new role
        data['bootstrap_expect'] = 3 if target_new_role == constants.CONSUL_ROLE_SERVER else 0
        LOG.info('update resmgr for consul role rebalance host %s with data %s', target_host_id, str(data))
        # update resmgr with the new settings
        req_url = '%s/%s/roles/pf9-ha-slave' % (url, target_host_id)
        result = requests.put(req_url,
                              headers=headers,
                              json=data)
        LOG.debug('req_url : %s , result : %s', req_url, str(result))
        succeeced = (result.status_code == requests.codes.ok)
        resp = None
        if not succeeced:
            LOG.warn('failed to update resmgr for consul role rebalance host %s with data %s, resp %s',
                     target_host_id,
                     str(data),
                     str(result))
            return {'status': constants.RPC_TASK_STATE_ABORTED, 'error': 'failed to update resmgr'}
        else:
            # step 2 : notify pf9-ha-slave with RPC message contains the rebalance request
            LOG.info('sending consul role rebalance request %s ', str(rebalance_request))
            resp = get_rebalance_controller(self._config).rebalance_and_wait_for_result(rebalance_request)

        if resp:
            return {'status': resp['status'], 'error': ''}
        else:
            msg = 'empty consul role rebalance response for request : %s' % str(rebalance_request)
            return {'status': constants.RPC_TASK_STATE_ERROR, 'error': msg}

    def _add_ha_slave_if_not_exist(self, cluster_name, host_ids_for_adding, consul_role, peer_host_ids):
        if consul_role not in ['server', 'agent']:
            consul_role = 'server'
        nova_client = self._get_nova_client()
        self._token = self._get_v3_token()
        current_roles = self._get_roles_for_hosts(host_ids_for_adding)
        role_missed_host_ids = []
        for hid in host_ids_for_adding:
            h_roles = current_roles[hid]
            if "pf9-ha-slave" not in h_roles:
                role_missed_host_ids.append(hid)
        if len(role_missed_host_ids) > 0:
            LOG.info('authorize pf9-ha-slave during adding new hosts: %s ', str(role_missed_host_ids))
            all_hosts = list(set(host_ids_for_adding).union(set(peer_host_ids)))
            ip_lookup, cluster_ip_lookup = self._get_ips_for_hosts(nova_client, all_hosts)
            self._auth(cluster_name, ip_lookup, cluster_ip_lookup, self._token, role_missed_host_ids, consul_role)
            self._wait_for_role_to_ok_v2(role_missed_host_ids)

    def _remove_ha_slave_if_exist(self, host_ids):
        current_roles = self._get_roles_for_hosts(host_ids)
        role_assigned_host_ids = []
        for hid in host_ids:
            h_roles = current_roles[hid]
            if "pf9-ha-slave" in h_roles:
                role_assigned_host_ids.append(hid)
        if len(role_assigned_host_ids) > 0:
            LOG.info('remove pf9-ha-slave role from hosts: %s ', str(host_ids))
            self._deauth(host_ids)
            self._wait_for_role_to_ok_v2(host_ids)

    def _is_nova_service_active(self, host_id, nova_client=None):
        if not nova_client:
            nova_client = self._get_nova_client()
        binary = 'nova-compute'
        services = nova_client.services.list(binary=binary, host=host_id)
        if len(services) == 1:
            if services[0].state == 'up':
                disabled_reason = 'Host disabled by PF9 HA manager'
                if services[0].status != 'enabled' and \
                        services[0].disabled_reason == disabled_reason:
                    LOG.info('host %s state is up, status is %s, '
                             'disabled reason %s, so enable it',
                             str(host_id), str(services[0].status),
                             str(services[0].disabled_reason))
                    nova_client.services.enable(binary=binary, host=host_id)
                LOG.debug("nova host %s is up and enabled", str(host_id))
                return True
            LOG.debug("nova host %s state is down", str(host_id))
            return False
        else:
            LOG.error('Found %s nova compute services with %s host id'
                      % (str(len(services)), host_id))
            raise ha_exceptions.HostNotFound(host_id)

    def _get_nova_client(self):
        # reference:
        # https://docs.openstack.org/python-novaclient/latest/reference/api/index.html
        loader = loading.get_plugin_loader('password')
        auth = loader.load_from_options(auth_url=self._auth_uri,
                                        username=self._username,
                                        password=self._passwd,
                                        project_name=self._tenant)
        sess = session.Session(auth=auth)
        nova = client.Client(2, session=sess, region_name=self._region)
        return nova

    def _get_all(self, nova_client):
        aggregates = nova_client.aggregates.list()
        result = []
        for aggr in aggregates:
            obj = self._get_one(nova_client, aggr.id)
            if obj:
                result.append(obj)
            else:
                LOG.warning('no hamgr cluster db record for aggregate id %s', str(aggr.id))
        return result

    def _get_one(self, nova_client, aggregate_id):
        _ = self._get_aggregate(nova_client, aggregate_id)
        # host aggregate id is the 'name' field in hamgr cluster table
        name = str(aggregate_id)
        cluster = db_api.get_cluster(name)
        if cluster is None:
            LOG.warning('no hamgr cluster record for host aggregate id %s', str(aggregate_id))
            return {}
        enabled = cluster.enabled if cluster is not None else False
        if enabled is True:
            task_state = 'completed' if cluster.task_state is None else \
                cluster.task_state
        else:
            task_state = cluster.task_state
        return dict(id=aggregate_id, enabled=enabled, task_state=task_state)

    def get(self, aggregate_id):
        nova_client = self._get_nova_client()
        if aggregate_id is not None:
            obj = self._get_one(nova_client, aggregate_id)
            return [obj] if obj else []
        else:
            return self._get_all(nova_client)

    def _get_aggregate(self, nova_client, aggregate_id):
        try:
            return nova_client.aggregates.get(aggregate_id)
        except exceptions.NotFound:
            raise ha_exceptions.AggregateNotFound(aggregate_id)

    def _check_if_host_in_other_cluster(self, hosts):
        clusters = db_api.get_all_active_clusters()
        invalid_hosts = set()
        valid_hosts = set()
        nova_client = self._get_nova_client()
        for cluster in clusters:
            aggregate = self._get_aggregate(nova_client, cluster.name)
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

    def _get_roles_for_hosts(self, host_ids):
        current_roles = {}
        # rather than make several calls to resmgr, just make one
        settings = self._get_settings_for_hosts()
        for hid in host_ids:
            json_resps = [x for x in settings if x['id'] == hid]
            if len(json_resps) > 0:
                json_resp = json_resps[0]
                if json_resp['info']['responding'] is True:
                    current_roles[hid] = json_resp['roles']
                else:
                    LOG.warn('role status for host %s is not responding : %s', str(hid), str(json_resp))
                    current_roles[hid] = []
        return current_roles

    def _validate_hosts(self, hosts, check_cluster=True, check_host_insufficient=True):
        # TODO Make this check configurable
        # Since the consul needs 3 to 5 servers for bootstrapping, it is safe
        # to enable HA only if 4 hosts are present. So that even if 1 host goes
        # down after cluster is created, we can reconfigure it.
        if check_host_insufficient and len(hosts) < 4:
            raise ha_exceptions.InsufficientHosts()

        LOG.debug("check host in other cluster : %s", str(check_cluster))
        if check_cluster is True:
            self._check_if_host_in_other_cluster(hosts)

        # Check host state and role status in resmgr before proceeding
        # make one call to resmgr , not for each host
        settings = self._get_settings_for_hosts()
        for host in hosts:
            json_resps = [x for x in settings if x['id'] == host]
            if len(json_resps) > 0:
                json_resp = json_resps[0]
                LOG.debug('role status for host %s : %s', host, str(json_resp))
                if json_resp.get('role_status', '') != 'ok':
                    LOG.warn('Role status of host %s is not ok : %s, not enabling HA '
                             'at the moment.', host, json_resp.get('role_status', ''))
                    raise ha_exceptions.InvalidHostRoleStatus(host)
                if json_resp['info']['responding'] is False:
                    LOG.warn('Host %s is not responding : %s, not enabling HA at the '
                             'moment.', host, json_resp['info']['responding'])
                    raise ha_exceptions.HostOffline(host)

    def _customize_pf9_ha_slave_config(self, cluster_name, consul_join_ips, consul_host_ip, consul_cluster_ip):
        # since the pf9-ha-slave role is controlled by hamgr, all the customizable settings for pf9-ha-slave role
        # that need to be synced with hamgr should be list here to be passed down to resmgr for updating
        # the settings for pf9-ha-slave on host.
        # the keys needs to match the keys which are customizable in pf9-ha-slave config file
        # first the basic settings
        customize_cfg = dict(cluster_name=str(cluster_name),
                             join=consul_join_ips,
                             ip_address=consul_host_ip,
                             cluster_ip=consul_cluster_ip)
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

        return customize_cfg

    def _auth(self, aggregate_id, ip_lookup, cluster_ip_lookup, token, nodes, role):
        assert role in ['server', 'agent']
        url = '%s/v1/hosts' % self._resmgr_endpoint
        headers = {'X-Auth-Token': token['id'],
                   'Content-Type': 'application/json'}
        valid_ips = [x for x in cluster_ip_lookup.values() if x != '']
        ips = ','.join([str(v) for v in valid_ips])
        LOG.info('ips for consul members to join : %s', ips)
        for node in nodes:
            start_time = datetime.now()
            LOG.info('Authorizing pf9-ha-slave role on node %s using IP %s',
                     node, ip_lookup[node])
            data = self._customize_pf9_ha_slave_config(aggregate_id, ips, ip_lookup[node], cluster_ip_lookup[node])
            data['bootstrap_expect'] = 3 if role == 'server' else 0
            auth_url = '/'.join([url, node, 'roles', 'pf9-ha-slave'])
            LOG.info('authorize pf9-ha-slave on host %s with consul role %s, data %s', node, role, str(data))
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
                if datetime.now() - start_time > timedelta(seconds=self._max_auth_wait_seconds):
                    break
            if resp.status_code != requests.codes.ok:
                LOG.warn('authorize pf9-ha-slave on host %s failed, data %s, resp %s', node, str(data), str(resp))
            resp.raise_for_status()

    def _fetch_role_details_for_host(self, host_id, host_roles):
        LOG.debug('find role details for host %s with role map %s', str(host_id), str(host_roles))
        roles = filter(lambda x: x.startswith('pf9-ostackhost'), host_roles)
        # host can only have 'pf9-ostackhost' or 'pf9-ostackhost-neutron'
        # can not have both
        if len(roles) != 1:
            LOG.warn('only allow one ostackhost role for host %s, but found : %s', str(host_id), str(roles))
            return None
        rolename = roles[0]
        # Query consul_ip from resmgr ostackhost role settings
        self._token = self._get_v3_token()
        headers = {'X-Auth-Token': self._token['id'],
                   'Content-Type': 'application/json'}
        resmgr_url = '%s/v1/hosts/' % self._resmgr_endpoint
        host_url = '/'.join([resmgr_url, host_id, 'roles', rolename])
        resp = requests.get(host_url, headers=headers)
        resp.raise_for_status()
        json_resp = resp.json()
        LOG.debug("role details for host %s : %s", str(host_id),
                  json.dumps(json_resp))
        return json_resp

    def _get_cluster_ip(self, host_id, json_resp):
        if not json_resp:
            LOG.warn('_get_cluster_ip : response object is none, can not parse cluster ip')
            return None
        LOG.info('try to get cluster_ip for host %s from resp %s', str(host_id), str(json_resp))
        if 'cluster_ip' not in json_resp:
            raise ha_exceptions.ClusterIpNotFound(host_id)
        return str(json_resp['cluster_ip'])

    def _get_consul_ip(self, host_id, json_resp):
        if not json_resp:
            return None
        LOG.info('try to get consul_ip for host %s from resp %s', str(host_id), str(json_resp))
        if 'consul_ip' in json_resp and json_resp['consul_ip']:
            return str(json_resp['consul_ip'])
        LOG.info('consul_ip is not set for host %s, fall back to cluster_ip', str(host_id))
        return self._get_cluster_ip(host_id, json_resp)

    def _get_ips_for_hosts(self, nova_client, hosts):
        all_hypervisors = nova_client.hypervisors.list()
        LOG.info('getting roles for hosts %s', str(hosts))
        # only get roles for authorized hosts to avoid errors
        # where resmgr has no state info for unauthorized hosts
        roles_map = self._get_roles_for_hosts(hosts)
        LOG.info('roles map of hosts %s : %s', str(hosts), str(roles_map))
        lookup = set(hosts)
        ip_lookup = dict()
        cluster_ip_lookup = dict()
        for hyp in all_hypervisors:
            host_id = hyp.service['host']
            cluster_ip = ''
            if host_id in lookup:
                ip_lookup[host_id] = hyp.host_ip
                # Overwrite host_ip value with consul_ip or cluster_ip
                # from ostackhost role
                if roles_map.get(host_id, None) is not None:
                    json_resp = self._fetch_role_details_for_host(host_id, roles_map[host_id])
                    LOG.debug('role details for host %s : %s', host_id, str(json_resp))
                    consul_ip = self._get_consul_ip(host_id, json_resp)
                    if consul_ip:
                        LOG.info('Using consul ip %s from ostackhost role',
                                 consul_ip)
                        ip_lookup[host_id] = consul_ip
                    cluster_ip = self._get_cluster_ip(host_id, json_resp)
                    if cluster_ip:
                        LOG.info('Using cluster ip %s from ostackhost role',
                                 cluster_ip)
                        cluster_ip_lookup[host_id] = cluster_ip

                    # if one exist but the other does not, just set to the existing one
                    # so at least each look up table has ip for each host
                    LOG.debug('ips of host %s : consul ip -> %s , cluster ip -> %s, details -> %s',
                              host_id, consul_ip, cluster_ip, str(json_resp))
                    if not consul_ip and cluster_ip:
                        ip_lookup[host_id] = cluster_ip
                    if not cluster_ip and consul_ip:
                        cluster_ip_lookup[host_id] = consul_ip
                else:
                    LOG.warn('no role map for hypervisor %s', str(host_id))
        # throw exception if num of items in both ip_lookup and cluster_ip_lookup
        # not equals to num of hosts, this will fail the caller earlier
        if len(ip_lookup.keys()) != len(hosts):
            ids = set(hosts).difference(set(ip_lookup.keys()))
            LOG.warn('size not match : ip_lookup : %s, hosts : %s,  found no consul ip for hosts %s',
                     str(ip_lookup), str(hosts), str(ids))
            raise ha_exceptions.HostsIpNotFound(list(ids))
        if len(cluster_ip_lookup.keys()) != len(hosts):
            ids = set(hosts).difference(set(cluster_ip_lookup.keys()))
            LOG.warn('size not match : cluster_ip_lookup : %s, hosts : %s, found no cluster ip for hosts %s',
                     str(cluster_ip_lookup), str(hosts), str(ids))
            raise ha_exceptions.HostsIpNotFound(list(ids))
        return ip_lookup, cluster_ip_lookup

    def _assign_roles(self, nova_client, aggregate_id, hosts):
        hosts = sorted(hosts)
        leader = hosts[0]
        servers = hosts[1:4]

        agents = []
        if len(hosts) >= 5:
            servers += hosts[4:5]
            agents = hosts[5:]

        ip_lookup, cluster_ip_lookup = self._get_ips_for_hosts(nova_client, hosts)
        if leader not in ip_lookup:
            LOG.error('Leader %s not found in nova', leader)
            raise ha_exceptions.HostNotFound(leader)

        leader_ip = ip_lookup[leader]

        self._token = self._get_v3_token()
        LOG.info('authorize pf9-ha-slave during assign roles')
        self._auth(aggregate_id, ip_lookup, cluster_ip_lookup, self._token, [leader] + servers, 'server')
        self._auth(aggregate_id, ip_lookup, cluster_ip_lookup, self._token, agents, 'agent')

    def __perf_meter(self, method, time_start):
        time_end = datetime.utcnow()
        LOG.debug('[metric] "%s" call : %s (start : %s , '
                  'end : %s)', str(method), str(time_end - time_start),
                  str(time_start), str(time_end))

    def _enable(self, aggregate_id, hosts=None,
                next_state=constants.TASK_COMPLETED):
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
        nova_client = self._get_nova_client()
        str_aggregate_id = str(aggregate_id)
        cluster = None
        cluster_id = None
        try:
            cluster = db_api.get_cluster(str_aggregate_id)
        except ha_exceptions.ClusterNotFound:
            pass
        except Exception:
            LOG.exception('error when get cluster %s from db',
                          str(str_aggregate_id))
            raise

        if cluster:
            cluster_id = cluster.id
            if cluster.task_state not in [constants.TASK_COMPLETED]:
                if cluster.task_state == constants.TASK_MIGRATING and \
                        next_state == constants.TASK_MIGRATING:
                    LOG.info('Enabling HA has part of cluster migration')
                else:
                    LOG.info('Cluster %s is running task %s, cannot enable',
                             str_aggregate_id, cluster.task_state)
                    raise ha_exceptions.ClusterBusy(str_aggregate_id,
                                                    cluster.task_state)

        # make sure no host exists in multiple host aggregation
        aggregate = self._get_aggregate(nova_client, aggregate_id)
        if not hosts:
            hosts = aggregate.hosts
        # observed that hosts sometimes need longer time to get to converged state
        # so here don't validate hosts, let the _handle_enable_request processor retry
        # this method's role is to save the request to db
        # this will avoid the REST API return 409
        try:
            LOG.debug('create ha cluster for enabling request')
            cluster = db_api.create_cluster_if_needed(str_aggregate_id,
                                                      constants.TASK_CREATING)
            if cluster.task_state != constants.TASK_CREATING:
                db_api.update_cluster_task_state(cluster.id, constants.TASK_CREATING)
            # set the status to 'request-enable'
            db_api.update_request_status(cluster.id, constants.HA_STATE_REQUEST_ENABLE)
            # publish status
            self._notify_status(constants.HA_STATE_REQUEST_ENABLE, "cluster", cluster.id)
        except Exception as e:
            print str(e)
            if cluster_id:
                db_api.update_cluster_task_state(cluster_id, next_state)
            LOG.error('unhandled exception : %s', str(e))

    def _handle_enable_request(self, request, hosts=None, next_state=constants.TASK_COMPLETED):
        LOG.debug('handling enable request')
        if not request:
            return
        nova_client = self._get_nova_client()
        str_aggregate_id = request.name
        cluster_id = request.id
        cluster_name = str(request.name)
        time_begin = datetime.utcnow()
        self.__perf_meter('db_api.get_cluster', time_begin),
        time_begin = datetime.utcnow()
        aggregate = self._get_aggregate(nova_client, str_aggregate_id)
        self.__perf_meter('_get_aggregate', time_begin)
        if not hosts:
            hosts = aggregate.hosts
        else:
            LOG.info('Enabling HA on some of the hosts %s of the %s aggregate',
                     str(hosts), str_aggregate_id)
        try:
            # observed that hosts sometimes need longer time to get to converged state
            # if this happens, _validate_hosts will throw exceptions, then the request will
            # not be processed until all requirements are met
            self._validate_hosts(hosts)
        except ha_exceptions.HostPartOfCluster:
            # if because host in two clusters, just report the request in error status
            db_api.update_request_status(cluster_id, constants.HA_STATE_ERROR)
            LOG.warn('found hosts exist in other clusters')
            raise
        except (ha_exceptions.HostOffline,
                ha_exceptions.InvalidHostRoleStatus,
                ha_exceptions.InvalidHypervisorRoleStatus) as ex:
            LOG.warn('exception when handle enable request : %s, will retry the request', str(ex))
            return
        except Exception:
            LOG.exception('unhandled exception in validate hosts when handle enable request')
            return
        time_begin = datetime.utcnow()
        self._token = self._get_v3_token()
        self.__perf_meter('utils.get_token', time_begin)
        try:
            # when request to enable a cluster, first create CA if not exist, and svc key and certs
            if self._is_consul_encryption_enabled():
                ca_changed = False
                svc_changed = False
                if not keyhelper.are_consul_ca_key_cert_pair_exist() or \
                        keyhelper.is_consul_ca_cert_expired():
                    ca_changed = keyhelper.create_consul_ca_key_cert_pairs()
                    LOG.info('CA key and cert are generated ? %s', str(ca_changed))
                else:
                    LOG.debug('CA key and cert are good for now')

                if not keyhelper.are_consul_svc_key_cert_pair_exist(cluster_name) or \
                        keyhelper.is_consul_svc_cert_expired(cluster_name):
                    svc_changed = keyhelper.create_consul_svc_key_cert_pairs(cluster_name)
                    LOG.info('svc key and cert for cluster name %s are generated ? %s', cluster_name, str(svc_changed))
                else:
                    LOG.debug('svc key and cert for cluster name %s are good for now', cluster_name)

            # 1. mark request as 'enabling' in status
            LOG.info('updating status of cluster %s to %s', str(cluster_id), constants.HA_STATE_ENABLING)
            time_begin = datetime.utcnow()
            db_api.update_request_status(cluster_id, constants.HA_STATE_ENABLING)
            self._notify_status(constants.HA_STATE_ENABLING, "cluster", cluster_id)
            self.__perf_meter('db_api.update_enable_or_disable_request_status', time_begin)

            # 2. Push roles
            time_begin = datetime.utcnow()
            LOG.info('assign roles for hosts during handling enabling request')
            self._assign_roles(nova_client, str_aggregate_id, hosts)
            self.__perf_meter('_assign_roles', time_begin)

            # 3. Create masakari fail-over segment
            time_begin = datetime.utcnow()
            if not masakari.is_failover_segment_exist(self._token, str(cluster_id)):
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
                except exceptions.ClusterNotFound:
                    pass
            raise
        else:
            if cluster_id is not None:
                time_begin = datetime.utcnow()
                db_api.update_cluster_task_state(cluster_id, next_state)
                self.__perf_meter('db_api.update_cluster_task_state', time_begin)

    def _get_settings_for_host(self, host_id):
        self._token = self._get_v3_token()
        headers = {'X-Auth-Token': self._token['id'],
                   'Content-Type': 'application/json'}
        url = '%s/v1/hosts' % self._resmgr_endpoint
        auth_url = '/'.join([url, host_id])
        resp = requests.get(auth_url, headers=headers)
        resp.raise_for_status()
        return resp.json()

    def _get_settings_for_hosts(self):
        self._token = self._get_v3_token()
        headers = {'X-Auth-Token': self._token['id'],
                   'Content-Type': 'application/json'}
        url = '%s/v1/hosts' % self._resmgr_endpoint
        resp = requests.get(url, headers=headers)
        resp.raise_for_status()
        return resp.json()

    def _wait_for_role_to_ok(self, nodes):
        for node in nodes:
            start_time = datetime.now()
            resp = self._get_settings_for_host(node)
            while resp.get('role_status', '') != 'ok':
                time.sleep(30)
                resp = self._get_settings_for_host(node)
                if datetime.now() - start_time > timedelta(seconds=self._max_role_converged_wait_seconds):
                    LOG.info(
                        "host %s is not converged, starting from %s to %s ",
                        str(node), str(start_time), str(datetime.now()))
                    raise ha_exceptions.RoleConvergeFailed(node)

    def _wait_for_role_to_ok_v2(self, nodes):

        timeout = len(nodes) * self._max_role_converged_wait_seconds
        start_time = datetime.now()
        converged=set()
        while True:
            if datetime.now() - start_time > timedelta(seconds=timeout):
                LOG.info(
                    "not all hosts are converged, starting from %s to %s ",
                    str(node), str(start_time), str(datetime.now()))
                break
            settings = self._get_settings_for_hosts()
            all_ok = True
            for node in nodes:
                resps = [x for x in settings if x['id'] == node]
                if len(resps) > 0:
                    resp = resps[0]
                    if resp.get('role_status', '') == 'ok':
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
            raise ha_exceptions.RoleConvergeFailed(','.join(not_converged))

    def _deauth(self, nodes):
        self._token = self._get_v3_token()
        headers = {'X-Auth-Token': self._token['id'],
                   'Content-Type': 'application/json'}
        url = '%s/v1/hosts/' % self._resmgr_endpoint

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
                if datetime.now() - start_time > timedelta(seconds=self._max_auth_wait_seconds):
                    break
            resp.raise_for_status()

    def _disable(self, aggregate_id, synchronize=False,
                 next_state=constants.TASK_COMPLETED):
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
        cluster = db_api.get_cluster(str_aggregate_id)
        if not cluster:
            LOG.warn('no cluster with id for disable %s', str_aggregate_id)
            return

        if cluster.task_state not in [constants.TASK_COMPLETED,
                                      constants.TASK_ERROR_REMOVING]:
            if cluster.task_state == constants.TASK_MIGRATING and \
                    next_state == constants.TASK_MIGRATING:
                LOG.info('disabling HA as part of cluster migration')
            else:
                LOG.info('Cluster %s is busy in %s state', cluster.name,
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
            LOG.error('unhandled exceptions when disable cluster with name %s : %s ', str_aggregate_id, str(e))

    def process_ha_enable_disable_requests(self):
        with self.ha_status_processing_lock:
            if self.ha_status_processing_running:
                LOG.debug('ha status processing task is already running')
                return
            self.ha_status_processing_running = True
        LOG.debug('ha status processing task starts to run at %s', str(datetime.utcnow()))
        try:
            requests = db_api.get_all_unhandled_enable_or_disable_requests()
            if requests:
                for request in requests:
                    if request.status == constants.HA_STATE_REQUEST_ENABLE:
                        self._handle_enable_request(request)
                    if request.status == constants.HA_STATE_REQUEST_DISABLE:
                        self._handle_disable_request(request)
        finally:
            with self.ha_status_processing_lock:
                self.ha_status_processing_running = False
        LOG.debug('ha status processing task has finished at %s', str(datetime.utcnow()))

    def _handle_disable_request(self, request, next_state=constants.TASK_COMPLETED):
        LOG.info('handling disable request : %s', str(request))
        cluster = request
        # the name used to query db needs to be string, not int
        #  name in failover segment matchs the name used in vmha cluster
        aggregate_name = str(cluster.name)
        try:
            self._token = self._get_v3_token()
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
                    nova_client = self._get_nova_client()
                    aggregate = self._get_aggregate(nova_client, aggregate_name)
                    hosts = set(aggregate.hosts)
            except ha_exceptions.SegmentNotFound:
                LOG.warn('Masakari segment for cluster: %s was not found, '
                         'skipping deauth', aggregate_name)

            # need to check whether masakari is working on this failover
            # segment for any notifications. if there is such notification
            # in 'new', 'running', 'error' state, then it is locked down
            # until the notification is not in those state, then we can
            # delete the failover segment, otherwise there will be
            # 409 conflict error
            need_to_wait = masakari.is_failover_segment_under_recovery(
                self._token, aggregate_name)
            if need_to_wait:
                LOG.info('not disable cluster %s for now, as masakari segment %s is under recovery, '
                         'will retry disabling request later', aggregate_name, aggregate_name)
                return

            # change status from 'request-disable' to 'disabling'
            LOG.info('set cluster id %s task state to %s', str(cluster.id), str(constants.HA_STATE_DISABLING))
            db_api.update_request_status(cluster.id
                                         , constants.HA_STATE_DISABLING)
            self._notify_status(constants.HA_STATE_DISABLING, "cluster", cluster.id)
            if hosts:
                LOG.debug('de-authorize roles from hosts and wait for complation')
                self._deauth(hosts)
                self._wait_for_role_to_ok_v2(hosts)
            else:
                LOG.info('no hosts found in segment %s', str(aggregate_name))
            LOG.debug('delete masakari segment from masakari')
            masakari.delete_failover_segment(self._token, str(aggregate_name))
            # change status from 'disabling' to 'disabled'
            db_api.update_request_status(cluster.id
                                         , constants.HA_STATE_DISABLED)
            self._notify_status(constants.HA_STATE_DISABLED, "cluster", cluster.id)
            LOG.debug('successfully processed disable request')
        except Exception:
            if cluster:
                # mark the status as 'error'
                db_api.update_request_status(cluster.id
                                             , constants.HA_STATE_ERROR)
                self._notify_status(constants.HA_STATE_ERROR, "cluster", cluster.id)
                db_api.update_cluster_task_state(cluster.id,
                                                 constants.TASK_ERROR_REMOVING)
            raise
        else:
            if cluster:
                db_api.update_cluster(cluster.id, False)
                db_api.update_cluster_task_state(cluster.id, next_state)

    def put(self, aggregate_id, method):
        LOG.info('process %s request for aggregate id %s', method, str(aggregate_id))
        if method == 'enable':
            self._enable(aggregate_id)
        else:
            self._disable(aggregate_id)

    def _get_cluster_for_host(self, host_id, nova_client=None):
        if not nova_client:
            nova_client = self._get_nova_client()
        clusters = db_api.get_all_active_clusters()
        for cluster in clusters:
            aggregate_id = cluster.name
            aggregate = self._get_aggregate(nova_client, aggregate_id)
            if host_id in aggregate.hosts:
                return cluster
        raise ha_exceptions.HostNotFound(host=host_id)

    def _remove_host_from_cluster(self, cluster, host, nova_client=None):
        if not nova_client:
            nova_client = self._get_nova_client()
        aggregate_id = cluster.name
        aggregate = self._get_aggregate(nova_client, aggregate_id)
        current_host_ids = set(aggregate.hosts)

        try:
            self._token = self._get_v3_token()
            nodes = masakari.get_nodes_in_segment(self._token, aggregate_id)
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
                self._disable(aggregate_id, synchronize=True,
                              next_state=constants.TASK_MIGRATING)
                self._enable(aggregate_id, hosts=list(host_list),
                             next_state=constants.TASK_MIGRATING)
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
            LOG.debug('prepare to write event, host %s, event %s ,  details %s',
                      host_name, event_type, event_details)
            # find the cluster id from given host id
            clusters = db_api.get_all_active_clusters()
            if clusters:
                target_cluster_id = None
                target_cluster_name = None
                for cluster in clusters:
                    aggregate_id = cluster.name
                    aggregate = self._get_aggregate(nova_client, aggregate_id)
                    hosts = set(aggregate.hosts)
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

                    if not existing_changes or len(existing_changes) <= 0:
                        # write change event into db
                        db_api.create_change_event(target_cluster_id,
                                                   event_details,
                                                   event_details['event']['eventId'])
                        LOG.info('change event record is created for host %s in cluster '
                                 '%s', host_name, target_cluster_id)
                    else:
                        LOG.warn('ingnore reporing event %s for host %s, as it is already reported within %s seconds',
                                 event_type, host_name, self._event_report_threshold_seconds)

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

                    if not existing_events or len(existing_events) <= 0:
                        # write events processing record
                        event_uuid = event_details['event']['eventId']
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
                                                 joins,
                                                 event_uuid)

                        LOG.info('event processing record is created for host %s on '
                                 'event %s in cluster %s', host_name, event_type,
                                 target_cluster_id)

                        LOG.info('checking consul rebalance for host event %s happened to host %s',
                                 event_type, host_name)
                        self._report_consul_rebalance(event_type, event_uuid, event_details['consul'])
                    else:
                        LOG.warn('ignore reporting event %s for host %s, as it is already reported within %s seconds',
                                 event_type, host_name, self._event_report_threshold_seconds)
                else:
                    LOG.warn('ignore reporting event %s for host %s , as it is not found in any active nova aggregates',
                             event_type, host_name)
            else:
                LOG.warn('ignore reporting event, no active clusters found for event %s for host %s',
                         event_type, host_name)

            # return True will stop the ha slave to re-send report, when return False, the ha slave will keep
            # sending report until it received True
            return True
        except Exception:
            LOG.exception('failed to record event')
        return False

    def _find_consul_rebalance_candidates(self, consul_members):
        members = consul_members
        # create a consul role rebalance record for this event, the rebalance thread will handle them later
        consul_servers = [x for x in members if x['Tags']['role'] == 'consul']
        consul_servers_alive = [x for x in consul_servers if x['Status'] == 1]
        consul_slaves = [x for x in members if x['Tags']['role'] == 'node']
        consul_slaves_alive = [x for x in consul_slaves if x['Status'] == 1]
        msg = 'found %s alive consul role of server : %s' % (
            str(len(consul_servers_alive)), str(consul_servers_alive))
        LOG.info(msg)
        msg = 'found %s alive consul role of slave : %s' % (str(len(consul_slaves_alive)), str(consul_slaves_alive))
        LOG.info(msg)
        # rebalance consul roles
        server_threshold = 5
        old_role = ""
        new_role = ""
        candicates = []
        if len(consul_servers_alive) > server_threshold:
            # move servers to slaves
            LOG.info('more than %s alive consul role of server found, actual %s',
                     str(server_threshold),
                     str(len(consul_servers_alive)))
            old_role = constants.CONSUL_ROLE_SERVER
            new_role = constants.CONSUL_ROLE_CLIENT
            start = server_threshold
            end = len(consul_servers_alive)
            for i in range(start, end):
                consul_host = consul_servers_alive[i - start]
                # only active host can be used for rebalance
                if self._is_nova_service_active(consul_host['Name'], self._get_nova_client()):
                    candicates.append({'host': consul_host['Name'],
                                       'old_role': old_role,
                                       'new_role': new_role,
                                       'member': consul_host})
        elif len(consul_servers_alive) < server_threshold:
            wanted = server_threshold - len(consul_servers_alive)
            LOG.info('not enough alive consul role of server, found %s, required %s, wanted %s',
                     str(len(consul_servers_alive)),
                     str(server_threshold),
                     str(wanted))
            if wanted == server_threshold:
                LOG.error('ignore consul role rebalance as not enough alive consul slaves to change to servers')
            else:
                # move slaves to servers
                old_role = constants.CONSUL_ROLE_CLIENT
                new_role = constants.CONSUL_ROLE_SERVER
                start = 0
                end = 0

                if len(consul_slaves_alive) >= wanted:
                    end = wanted
                elif len(consul_slaves_alive) < wanted:
                    end = len(consul_slaves_alive)
                for i in range(start, end):
                    consul_host = consul_slaves_alive[i - start]
                    # only active host can be used for rebalance
                    if self._is_nova_service_active(consul_host['Name']):
                        candicates.append({'host': consul_host['Name'],
                                           'old_role': old_role,
                                           'new_role': new_role,
                                           'member': consul_host})
        else:
            LOG.info('consul role rebalance is not needed, num of alive consul servers %s meets required %s',
                     str(len(consul_servers_alive)),
                     str(server_threshold))
        return candicates

    def _report_consul_rebalance(self, event_type, event_uuid, consul_report):
        try:
            candicates = self._find_consul_rebalance_candidates(consul_report.get('members', ''))
            LOG.info('found consul rebalance candidates : %s', str(candicates))
            # create rebalance request for each target hosts, they should be processed one by one
            for candidate in candicates:
                host_id = candidate['host']
                rebalance_action = {'host': host_id, 'old_role': candidate['old_role'],
                                    'new_role': candidate['new_role']}
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
                    LOG.warn('host %s is not active in nova, ignore it for consul role rebalance request', host_id)
                else:
                    LOG.info('create consul role rebalance request for host %s : %s', host_id, str(rebalance_action))
                    db_api.add_consul_role_rebalance_record(event_type,
                                                            event_uuid,
                                                            json.dumps(consul_report),
                                                            json.dumps(rebalance_action)
                                                            )
        except:
            LOG.exception('unhandled exception during reporting consul rebalance')

    def _notify_status(self, action, target, identifier):
        obj = Notification(action, target, identifier)
        get_notification_manager(self._config).send_notification(obj)

    def get_aggregate_info_for_cluster(self, cluster_id):
        try:
            nova_client = self._get_nova_client()
            clusters = db_api.get_all_active_clusters()
            for cluster in clusters:
                if cluster.id != cluster_id:
                    continue

                aggregate_id = cluster.name
                aggregate = self._get_aggregate(nova_client, aggregate_id)
                LOG.debug('host aggregate details for cluster %s : %s',
                          str(cluster_id), str(aggregate))
                return aggregate
            return None
        except:
            LOG.exception('unhandled exception when get aggregate info '
                          'for cluster %s', str(cluster_id))
        return None

    def process_consul_role_rebalance_requests(self):
        # wait until current task complete
        with self.consul_role_rebalance_processing_lock:
            if self.consul_role_rebalance_processing_running:
                LOG.debug('consul role rebalance processing task is already running')
                return
            self.consul_role_rebalance_processing_running = True
        LOG.info('consul role rebalance processing task start to work at %s', str(datetime.utcnow()))

        req_id = None
        try:
            # ----------------------------------------------------------------
            # get all unhandled requests
            # check whether the associated host event is actually finished
            # use RPC to notify host the request
            # wait for response from host to update the record
            # ----------------------------------------------------------------
            unhandled_requests = db_api.get_all_unhandled_consul_role_rebalance_requests()
            if not unhandled_requests or len(unhandled_requests) <= 0:
                # LOG.debug('no unhandled consul role rebalance requests found from db for now')
                return
            for req in unhandled_requests:
                req_id = req.uuid
                LOG.info('processing consul role rebalance request %s for event %s: %s',
                         req.uuid, str(req.event_uuid), str(req.rebalance_action))
                # first check by time, abort request if timestamp shows the request older than 15 minutes
                if (datetime.utcnow() - req.last_updated) > timedelta(minutes=15):
                    error = 'request created at %s is staled' % req.last_updated
                    LOG.warn('ignore consul role rebalance request %s, as %s', str(req_id), error)
                    db_api.update_consul_role_rebalance(req.uuid,
                                                        None,
                                                        None,
                                                        constants.RPC_TASK_STATE_ABORTED,
                                                        error)
                    continue
                # then check by status, query the same request again to get most recent status (in case it is canceled )
                req = db_api.get_consul_role_balance_record_by_uuid(req.uuid)
                if req is None or req.action_status == constants.RPC_TASK_STATE_ABORTED:
                    LOG.warn('ignore consul role rebalance request %s, as it was already in state %s',
                             req.uuid,
                             req.action_status)
                    continue

                action_obj = json.loads(req.rebalance_action)
                target_host_id = action_obj['host']
                target_old_role = action_obj['old_role']
                target_new_role = action_obj['new_role']

                event_uuid = req.event_uuid
                event_type = req.event_name

                if event_type not in constants.HOST_EVENTS:
                    error = 'invalid event type %s' % event_type
                    LOG.warn('ignore consul role rebalance request %s, as %s', event_uuid, error)
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
                        LOG.warn('ignore consul role rebalance request %s, as %s', event_uuid, error)
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
                        LOG.warn('ignore consul role rebalance request %s, as %s', event_uuid, error)
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
                    if event_status is None or event_status in ['new', 'running']:
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
                        LOG.warn('ignore consul role rebase request %s (%s), as %s',
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
                        LOG.warn('ignore consul role rebalance request %s, as %s',
                                 req.uuid,
                                 error)
                        db_api.update_consul_role_rebalance(req.uuid,
                                                            None,
                                                            None,
                                                            constants.RPC_TASK_STATE_ABORTED,
                                                            error)
                        continue

                # the associated host event has been finished, it is ok to start to rebalance roles
                LOG.info('start to process consul role rebalance for event %s with type %s',
                         event_uuid, event_type)

                # for all valid events, start to process the rebalance request
                # set db status to running
                db_api.update_consul_role_rebalance(req.uuid,
                                                    None,
                                                    None,
                                                    constants.RPC_TASK_STATE_RUNNING,
                                                    None)

                consul_status_before = json.loads(req.before_rebalance)
                consul_addresses = [x['Addr'] for x in consul_status_before['members']]
                target_consuls = [x for x in consul_status_before['members'] if x['Name'] == target_host_id]
                if len(target_consuls) != 1:
                    error = 'no consul status for target host %s' % target_host_id
                    LOG.warn('ignore consul role rebalance request %s, as %s', req.uuid, error)
                    db_api.update_consul_role_rebalance(req.uuid,
                                                        None,
                                                        None,
                                                        constants.RPC_TASK_STATE_ABORTED,
                                                        error)
                    continue

                join_ips = ','.join(consul_addresses)
                cluster_ip = target_consuls[0]['Addr']
                cluster_name = target_consuls[0]['Tags']['dc']
                rebalance_request = ConsulRoleRebalanceRequest(cluster=cluster_name,
                                                               host_id=target_host_id,
                                                               old_role=target_old_role,
                                                               new_role=target_new_role)

                # data = self._customize_pf9_ha_slave_config(join_ips, cluster_ip, cluster_ip)
                LOG.info('run consul role rebalance request with ip %s to join %s : %s',
                         cluster_ip, join_ips, str(rebalance_request))
                rebalance_result = self._execute_consul_role_rebalance(rebalance_request, cluster_name, cluster_ip,
                                                                       join_ips)
                LOG.info('result of consul role rebalance request %s : %s',
                         str(rebalance_request), str(rebalance_result))
                status_code = rebalance_result['status']
                status_msg = rebalance_result['error']
                db_api.update_consul_role_rebalance(req.uuid,
                                                    None,
                                                    None,
                                                    status_code,
                                                    status_msg)
                # if the error code is not none , means something happened (most cases are the target host is down
                # before the above process happened, so get empty response), let's just create new rebalance
                # request
                if status_code == constants.RPC_TASK_STATE_ERROR:
                    LOG.info('rebalance request %s failed for event %s (%s), re-try rebalance for this event if needed'
                             '. code : %s, message : %s',
                             str(req.uuid), event_uuid, event_type, status_code, status_msg)
                    self._rebalance_consul_roles_if_needed(cluster_name, req.event_name, event_uuid=event_uuid)
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
        LOG.info('consul encryption configuration processing task start to work at %s', str(datetime.utcnow()))
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
            LOG.info('checking certs for %s active clusters : %s', str(len(clusters)), str([x.name for x in clusters]))
            for cluster in clusters:
                cluster_name = cluster.name
                svc_changed = False
                # for ha enabled cluster, when svc key and cert not exist , or expired, then refresh them
                if not keyhelper.are_consul_svc_key_cert_pair_exist(cluster_name) or \
                        keyhelper.is_consul_svc_cert_expired(cluster_name):
                    svc_changed = keyhelper.create_consul_svc_key_cert_pairs(cluster_name)
                    LOG.info('expired svc key and cert for cluster %s are refreshed ? %s', cluster_name,
                             str(svc_changed))
                else:
                    LOG.debug('svc key and cert for cluster %s are good for now', cluster_name)

                # get hosts ids from masakari for current cluster
                hosts = masakari.get_nodes_in_segment(self._token, str(cluster_name))
                LOG.debug('hosts in aggregate %s : %s', cluster_name, str(hosts))
                host_ids = [x['name'] for x in hosts]
                LOG.debug('checking configs for %s hosts in aggregate %s : %s',
                          str(len(host_ids)), cluster_name, str(host_ids))
                ip_lookup, cluster_ip_lookup = self._get_ips_for_hosts(nova_client, host_ids)
                # need to push the settings for each host through resmgr
                for host in hosts:
                    host_id = str(host['name'])
                    url = '%s/v1/hosts' % self._resmgr_endpoint
                    try:
                        self._token = self._get_v3_token()
                        headers = {'X-Auth-Token': self._token['id'], 'Content-Type': 'application/json'}
                        # get the target host info to make sure the role status is ok
                        req_url = url + "/" + host_id
                        result = requests.get(req_url, headers=headers)
                        if not result or result.status_code != requests.codes.ok:
                            LOG.warn('call to %s was unsuccessful, result : %s', req_url, str(result))
                            continue
                        resp_host = result.json()
                        if 'pf9-ha-slave' not in resp_host['roles']:
                            LOG.warn('host %s does not have pf9-ha-slave role', host_id)
                            continue

                        req_url = '%s/%s/roles/pf9-ha-slave' % (url, host_id)
                        result = requests.get(req_url, headers=headers)
                        if not result or result.status_code != requests.codes.ok:
                            LOG.warn('call to %s was unsuccessful, result : %s', req_url, str(result))
                            continue
                        resp_role = result.json()
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
                        LOG.info('gossip key for host %s in aggregate %s needs refresh ? %s',
                                 host_id, cluster_name, str(existing_encrypt != gossip_key))
                        existing_key_file_content = resp_role.get('key_file_content', "")
                        need_refresh |= (existing_key_file_content != svc_key_content)
                        LOG.info('svc key for host %s in aggregate %s needs refresh ? %s',
                                 host_id, cluster_name, str(existing_key_file_content != svc_key_content))
                        existing_cert_file_content = resp_role.get('cert_file_content', "")
                        need_refresh |= (existing_cert_file_content != svc_cert_content)
                        LOG.info('svc cert for host %s in aggregate %s needs refresh ? %s',
                                 host_id, cluster_name, str(existing_cert_file_content != svc_cert_content))
                        existing_ca_file_content = resp_role.get('ca_file_content', "")
                        need_refresh |= (existing_ca_file_content != ca_cert_content)
                        LOG.info('ca cert for host %s in aggregate %s needs refresh ? %s',
                                 host_id, cluster_name, str(existing_ca_file_content != ca_cert_content))

                        if need_refresh:
                            LOG.info('configs for host %s in aggregate %s will be refreshed', str(host_id), cluster_name)
                            valid_ips = [x for x in cluster_ip_lookup.values() if x != '']
                            join_ips = ','.join([str(v) for v in valid_ips])
                            host_ip = ip_lookup[host_id]
                            # after the pf9-ha-slave role is enabled, the resmgr should have below customized settings:
                            # - cluster_ip, join, ip_address, bootstrap_expect
                            # no matter they are existed or not, always set cluster_ip, join, ip_address.
                            # only the bootstrap_expect can not be determined here
                            data = self._customize_pf9_ha_slave_config(cluster_name, join_ips, host_ip, host_ip)
                            # update resmgr with the new settings
                            req_url = '%s/%s/roles/pf9-ha-slave' % (url, host_id)
                            result = requests.put(req_url,
                                                  headers=headers,
                                                  json=data)
                            LOG.debug('req_url : %s , result : %s', req_url, str(result))
                            if not result or result.status_code != requests.codes.ok:
                                LOG.warn('failed to update settings for host %s : %s', host_id, str(data))

                        else:
                            LOG.debug('key or cert config refresh is not needed for host %s in aggregate %s',
                                      host_id, cluster_name)
                    except Exception as xes:
                        LOG.exception('unhandled exception %s', str(xes))
        except Exception as ex:
            LOG.exception('unhandled exception in process_consul_encryption_configuration : %s', str(ex))
        finally:
            with self.consul_encryption_processing_lock:
                self.consul_encryption_processing_running = False
        LOG.info('consul encryption configuration processing task has finished at %s', str(datetime.utcnow()))

    def get_common_hosts_configs(self, aggregate_id):
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

            cluster_exists = db_api.get_cluster(str(aggregate_id), raise_exception=False)
            LOG.debug('found clusters %s : %s', str(aggregate_id), str(cluster_exists))
            enabled = False
            if cluster_exists:
                enabled = cluster_exists.enabled

            if not enabled:
                # first get all hosts in this aggregate from nova
                details = nova_client.aggregates.get_details(aggregate_id)
                LOG.debug('host aggregate details : %s', str(details.__dict__))
                host_ids = details.hosts
            else:
                # get hosts from masakari
                hosts = masakari.get_nodes_in_segment(self._token, str(aggregate_id))
                host_ids = [x['name'] for x in hosts]
                LOG.debug('details of hosts from masakari : %s' , str(host_ids))
            LOG.debug('host ids found that associated with aggregate %s : %s', str(aggregate_id), str(host_ids))

            # call resmgr for each host to get configs
            headers = {'X-Auth-Token': self._token['id'], 'Content-Type': 'application/json'}
            # check whether the mounted nfs are same for all hosts
            for host_id in host_ids:
                # object to carry necessary info for making decision
                item = {
                    'host': host_id,
                    'mounted_nfs': [],
                    'nova_instances_path_matched_nfs': [],
                    'nova_instances_path': ''
                }
                # step 1 : get mounted_nfs from resmgr
                url = '%s/v1/hosts/%s' % (self._resmgr_endpoint, host_id)
                result = requests.get(url, headers=headers)
                if result.status_code != requests.codes.ok:
                    LOG.error('request %s for host %s was not success : %s', str(url), host_id, str(result.__dict__))
                    continue
                host_settings = result.json()
                LOG.debug('resp of %s for host %s : %s', str(url), host_id, str(host_settings))
                mounted_nfs_settings = host_settings['extensions'].get('mounted_nfs', None)
                if mounted_nfs_settings:
                    item['mounted_nfs'] = mounted_nfs_settings.get('data', {}).get('mounted', [])
                LOG.debug('mounted nfs settings for host %s : %s', host_id, str(mounted_nfs_settings))
                # step 2 - get instances_path configured for role pf9-ostackhost-neutron
                url = '%s/v1/hosts/%s/roles/pf9-ostackhost-neutron' % (self._resmgr_endpoint, host_id)
                result = requests.get(url, headers=headers)
                if result.status_code != requests.codes.ok:
                    LOG.error('request %s for host %s was not success : %s', str(url), host_id, str(result.__dict__))
                    continue
                role_settings = result.json()
                LOG.debug('resp of %s : %s', str(url), str(role_settings))
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
        # - no hosts found for given aggregate
        # - unable to get settings from resmgr (mounted_nfs, instances_path)
        # - all hosts without shared nfs or matched instances_path
        # - only some hosts with shared nfs and match instances_path
        error = None
        if len(host_ids) <= 0:
            error = "no hosts found for aggregate %s " % (str(aggregate_id))
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
        LOG.debug('common configs for hosts in aggregate %s : %s', str(aggregate_id), str(common_configs))
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
        for cluster in active_clusters:
            cluster_name = cluster.name
            LOG.info('refresh consul status for cluster %s', str(cluster_name))
            request = ConsulRefreshRequest(cluster=cluster_name, cmd='refresh')
            status = controller.ask_for_consul_cluster_status(request)
            LOG.info('refreshed consul report for cluster %s: %s', str(cluster_name), str(status))
            if status and status['status'] == constants.RPC_TASK_STATE_FINISHED:
                report[cluster_name] = json.loads(status['report'])
            else:
                report[cluster_name] = {}
        return report

def get_provider(config):
    db_api.init(config)
    return NovaProvider(config)
