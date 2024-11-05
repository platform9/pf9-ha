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

import logging
import json
from datetime import datetime, timedelta
from flask import jsonify
from flask import request
from hamgr import app
from hamgr.context import error_handler
from shared.exceptions import ha_exceptions as exceptions
import shared.constants as constants
from hamgr import provider_factory
from shared.constants import LOGGER_PREFIX

LOG = logging.getLogger(LOGGER_PREFIX + __name__)

CONTENT_TYPE_HEADER = {'Content-Type': 'application/json'}


def get_provider():
    return provider_factory.ha_provider()


@app.route('/v1/ha', methods=['GET'])
@error_handler
def get_all():
    provider = get_provider()
    status = provider.get(None)
    return jsonify(status=status)


@app.route('/v1/ha/<availability_zone>', methods=['GET'])
@error_handler
def get_status(availability_zone):
    try:
        provider = get_provider()
        status = provider.get(availability_zone)
        return jsonify(status=status)
    except exceptions.AvailabilityZoneNotFound:
        LOG.error('Availability zone %s was not found', availability_zone)
        return jsonify(dict(success=False)), 404, CONTENT_TYPE_HEADER


@app.route('/v1/ha/<availability_zone>/<action>', methods=['PUT'])
@error_handler
def update_status(availability_zone, action):
    if not isinstance(action, str):
        return jsonify(dict(error='Not Found')), 400, CONTENT_TYPE_HEADER
    action = action.lower()
    if action not in ['enable', 'disable']:
        return jsonify(dict(error='Invalid action')), 400, CONTENT_TYPE_HEADER
    try:
        provider = get_provider()
        provider.put(availability_zone, action)
        return jsonify(dict(success=True)), 200, CONTENT_TYPE_HEADER
    except exceptions.InsufficientHosts as ex:
        LOG.error('Bad request: %s', ex)
        return jsonify(dict(error=str(ex))), 400, CONTENT_TYPE_HEADER
    except exceptions.AvailabilityZoneNotFound:
        LOG.error('Availability Zone %s was not found', availability_zone)
        return jsonify(dict(success=False)), 404, CONTENT_TYPE_HEADER
    except (exceptions.HostOffline, exceptions.InvalidHostRoleStatus,
            exceptions.InvalidHypervisorRoleStatus) as ex:
        LOG.error('Cannot update cluster status since %s', ex)
        return jsonify(dict(error=str(ex))), 409, CONTENT_TYPE_HEADER
    except exceptions.HostPartOfCluster as ex:
        LOG.error("Cannot enable cluster since %s", ex)
        # According to HTTP response code list, error code 412 represents
        # precondition failed.
        return jsonify(
            dict(success=False, error=str(ex))), 412, CONTENT_TYPE_HEADER


@app.route('/v1/ha/<uuid:host_uuid>', methods=['POST'])
@error_handler
def update_host_status(host_uuid):
    host_id = str(host_uuid)
    json_obj = request.get_json()
    event = json_obj.get('event', None)
    event_details = json_obj.get('event_details', {})
    event_details['host_id'] = host_id
    postby = event_details['event'].get('reportedBy', '')
    LOG.info('received event %s for host %s from host %s : %s',
             str(event), host_id, str(postby), str(event_details))
    provider = get_provider()
    if event and event == 'host-down':
        masakari_notified = provider.host_down(event_details)
    elif event and event == 'host-up':
        masakari_notified = provider.host_up(event_details)
    else:
        LOG.warning('Invalid request')
        return jsonify(dict(success=False)), 422, CONTENT_TYPE_HEADER
    LOG.info('received %s event for host %s has been processed, result : %s',
             event, host_id, str(masakari_notified))
    if masakari_notified:
        return jsonify(dict(success=True)), 200, CONTENT_TYPE_HEADER
    return jsonify(dict(success=False)), 403, CONTENT_TYPE_HEADER


@app.route('/v1/consul', methods=['GET'])
@error_handler
def get_all_consul_status():
    ha_provider = provider_factory.ha_provider()
    status = ha_provider.refresh_consul_status()
    return jsonify(status)

@app.route('/v1/consul/<availability_zone>', methods=['GET'])
@error_handler
def get_consul_status(availability_zone=None):
    ha_provider = provider_factory.ha_provider()
    db_provider = provider_factory.db_provider()
    records = db_provider.get_latest_consul_status(availability_zone)
    results = []
    staled = False
    LOG.debug('latest consul status for availability zone %s : %s', availability_zone, str(records))
    for record in records:
        if record is None:
            continue
        # to make ui side simple, just return those fields
        #  - 'zone name'
        #  - 'host Id'
        #  - 'host status'
        #  - 'consul role'
        #  - 'is leader'
        #  - 'last update'

        # get zone name by cluster id
        LOG.debug('find availability zone for record : %s', str(record))
        az = ha_provider.get_availability_zone_info_for_cluster(record.clusterName)
        if not az:
            LOG.debug('no availability zone found with name : %s', record.clusterName)
            continue
        LOG.debug('availability zone found : %s', az)
        availability_zone_host_ids = availability_zone.hosts

        leader = record.leader
        # need to remove the prefix u int string json , otherwise loads will fail
        members = json.loads(str(record.members).replace('u\'', '\'').replace('\'','\"'))
        for host_id in availability_zone_host_ids:
            matches = [x for x in members if x['Name'] == host_id]
            if len(matches) <= 0:
                continue
            member = matches[0]
            if not member:
                LOG.warning('host %s exists in availability zone %s but does not '
                         'in consul members report : %s',
                         str(host_id),
                         str(availability_zone),
                         str(members))
                continue

            # get host status in consul members: 1=Alive, 2=Leaving, 3=Left, 4=Failed
            host_status = 'up' if member['Status'] == 1 else 'down'
            # get consul role for host
            host_role = 'server' if member['Tags']['role'] == 'consul' else 'agent'
            is_leader = False
            if host_role == "server":
                # only server role will have port 8300 in Tags
                is_leader = (leader == ('%s:%s' % (member['Addr'], member['Tags']['port'])))

            result = {
                'zoneName': availability_zone,
                'hostId': host_id,
                'hostStatus': host_status,
                'consulRole': host_role,
                'isLeader': is_leader,
                'lastUpdate': datetime.strftime(record.lastUpdate,
                                                '%Y-%m-%d %H:%M:%S')
            }
            results.append(result)
            staled = staled | ((datetime.utcnow() - record.lastUpdate) > timedelta(minutes=5))
    # when no db records or they are old than 5 minutes, then use RPC to get real time status
    # don't write back to db because it will blow away the db (tons of calls every 6 minutes)
    # will generate lots of db records
    if len([x for x in results if x]) <= 0 or staled:
        LOG.info('no consul status records found in db or they staled, now get real time status by RPC')
        realtime_status = ha_provider.refresh_consul_status()
        target_availability_zone = None
        if realtime_status :
            agg_ids = realtime_status.keys()

            for agg_id in agg_ids:
                aggregate = ha_provider.get_aggregate_info_for_cluster(str(agg_id))

                if not aggregate:
                    LOG.info('no host aggregate found with id %s', str(agg_id))
                    continue

                if availability_zone and availability_zone == agg_id:
                    target_aggregate = aggregate
                aggregate_name = aggregate.name
                aggregate_zone = aggregate.availability_zone
                for member in realtime_status[agg_id]['members']:
                    host_id = member['Name']
                    host_status = 'up' if member['Status'] == 1 else 'down'
                    host_role = 'server' if member['Tags']['role'] == 'consul' else 'agent'
                    is_leader = False
                    if host_role == "server":
                        # only server role will have port 8300 in Tags
                        is_leader = (realtime_status[agg_id]['leader'] == ('%s:%s' % (member['Addr'], member['Tags']['port'])))
                    # convert the data to output schema
                    results.append({'availabilityZoneName': availability_zone,
                                    'hostId': host_id,
                                    'hostStatus': host_status,
                                    'consulRole': host_role,
                                    'isLeader': is_leader,
                                    'lastUpdate': datetime.strftime(datetime.utcnow(),
                                                                    '%Y-%m-%d %H:%M:%S')})
        else:
            LOG.info('real time consul status not found')
        if availability_zone and target_availability_zone:
            results = [x for x in results if x['availabilityZoneName'] == str(target_availability_zone.name)]

    return jsonify(results)


@app.route('/v1/config/<availability_zone>', methods=['GET'])
@error_handler
def get_hosts_configs(availability_zone):
    """
    return the reported hosts configs from resmgr,
    :param availability_zone: the availability zone name
    :return:
    """
    try:
        ha_provider = provider_factory.ha_provider()
        # scenario 1 - called before /enable API is called
        # to determine whether there is shared nfs server
        # use nova to get list of hosts for given availability zone
        # scenario 2 - called after the /enable API is called
        # use masakari to get list of host
        config = ha_provider.get_common_hosts_configs(availability_zone)
        return jsonify(config)
    except exceptions.NoCommonSharedNfsException as e1:
        return jsonify(dict(success=False, error=str(e1))), 500, CONTENT_TYPE_HEADER
    except Exception as e2:
        return jsonify(dict(success=False, error=str(e2))), 500, CONTENT_TYPE_HEADER


@app.route('/v1/cluster', methods=['GET'])
@error_handler
def get_vmha_clusters():
    """
    get info for all active vmha clusters and their associated hosts from masakari.
    :return:
    info about active vmha clusters
    """
    ha_provider = provider_factory.ha_provider()
    results = ha_provider.get_active_clusters(id=None)
    return jsonify(results)


@app.route('/v1/cluster/<availability_zone>', methods=['GET'])
@error_handler
def get_vmha_cluster_by_name(availability_zone):
    """
    get vmha cluster info for given name
    :param availability_zone: the availability zone name
    :return:
    info about the given active vmha cluster
    """
    ha_provider = provider_factory.ha_provider()
    results = ha_provider.get_active_clusters(name=availability_zone)
    return jsonify(results)


@app.route('/v1/consul/<availability_zone>/agent/<uuid:host_uuid>/role/<role>', methods=['PUT'])
@error_handler
def set_consul_role(availability_zone, host_uuid, role):
    """
    set the consul cluster role (master or slave) for a host in given cluster.
    :param availability_zone:  the name of ha cluster
    :param host_id: the uuid of host in ha cluster
    :param role: the consul agent role, 'server' or 'client'
    :return:
    """
    host_id = str(host_uuid)
    if role not in [constants.CONSUL_ROLE_SERVER, constants.CONSUL_ROLE_CLIENT]:
        return jsonify(dict(error='Invalid role')), 400, CONTENT_TYPE_HEADER
    ha_provider = provider_factory.ha_provider()
    try:
        ha_provider.set_consul_agent_role(availability_zone, host_id, role)
        return jsonify(dict(success=True)), 200, CONTENT_TYPE_HEADER
    except exceptions.AvailabilityZoneNotFound:
        LOG.error('Availability zone %s was not found', availability_zone)
        return jsonify(dict(success=False)), 412, CONTENT_TYPE_HEADER
    except exceptions.ClusterNotFound:
        LOG.error('Cluster %s was not found', availability_zone)
        return jsonify(dict(success=False)), 412, CONTENT_TYPE_HEADER
    except exceptions.HostNotFound:
        LOG.error("Host %s was not found", host_id)
        return jsonify(dict(success=False)), 412, CONTENT_TYPE_HEADER
    except exceptions.HostNotInCluster:
        LOG.error("Host %s is not in cluster %s", host_id, availability_zone)
        return jsonify(dict(success=False)), 412, CONTENT_TYPE_HEADER
    except exceptions.RoleSettingsNotFound:
        LOG.error("Host %s does not have required role settings", host_id)
        return jsonify(dict(success=False)), 412, CONTENT_TYPE_HEADER
    except Exception as e:
        LOG.error(e)
        return jsonify(dict(success=False, error=e.message)), 412, CONTENT_TYPE_HEADER


def app_factory(global_config, **local_conf):
    return app
