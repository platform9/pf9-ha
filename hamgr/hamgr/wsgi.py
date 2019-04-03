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

from ConfigParser import ConfigParser
import logging
import json
from datetime import datetime
from flask import jsonify
from flask import request
from hamgr import app
from hamgr.context import error_handler
from shared.exceptions import ha_exceptions as exceptions
from hamgr import provider_factory

LOG = logging.getLogger(__name__)

CONTENT_TYPE_HEADER = {'Content-Type': 'application/json'}


def get_provider():
    return provider_factory.ha_provider()


@app.route('/v1/ha', methods=['GET'])
@error_handler
def get_all():
    provider = get_provider()
    status = provider.get(None)
    return jsonify(status=status)


@app.route('/v1/ha/<int:aggregate_id>', methods=['GET'])
@error_handler
def get_status(aggregate_id):
    try:
        provider = get_provider()
        status = provider.get(aggregate_id)
        return jsonify(status=status)
    except exceptions.AggregateNotFound:
        LOG.error('Aggregate %s was not found', aggregate_id)
        return jsonify(dict(success=False)), 404, CONTENT_TYPE_HEADER


@app.route('/v1/ha/<int:aggregate_id>/<action>', methods=['PUT'])
@error_handler
def update_status(aggregate_id, action):
    if not isinstance(action, basestring):
        return jsonify(dict(error='Not Found')), 400, CONTENT_TYPE_HEADER
    action = action.lower()
    if action not in ['enable', 'disable']:
        return jsonify(dict(error='Invalid action')), 400, CONTENT_TYPE_HEADER
    try:
        provider = get_provider()
        provider.put(aggregate_id, action)
        return jsonify(dict(success=True)), 200, CONTENT_TYPE_HEADER
    except exceptions.InsufficientHosts as ex:
        LOG.error('Bad request was made. %s', ex)
        return jsonify(dict(error=ex.message)), 400, CONTENT_TYPE_HEADER
    except exceptions.AggregateNotFound:
        LOG.error('Aggregate %s was not found', aggregate_id)
        return jsonify(dict(success=False)), 404, CONTENT_TYPE_HEADER
    except (exceptions.HostOffline, exceptions.InvalidHostRoleStatus,
            exceptions.InvalidHypervisorRoleStatus) as ex:
        LOG.error('Cannot update cluster status since %s', ex)
        return jsonify(dict(error=ex.message)), 409, CONTENT_TYPE_HEADER
    except exceptions.HostPartOfCluster as ex:
        LOG.error("Cannot enable cluster since %s", ex)
        # According to HTTP response code list, error code 412 represents
        # precondition failed.
        return jsonify(
            dict(success=False, error=ex.message)), 412, CONTENT_TYPE_HEADER


@app.route('/v1/ha/<uuid:host_id>', methods=['POST'])
@error_handler
def update_host_status(host_id):
    json_obj = request.get_json()
    event = json_obj.get('event', None)
    event_details = json_obj.get('event_details', {})
    event_details['host_id'] = str(host_id)
    postby = event_details['event'].get('reportedBy', '')
    LOG.info('received event %s for host %s from host %s : %s', str(event), str(host_id), str(postby),
             str(event_details))
    provider = get_provider()
    if event and event == 'host-down':
        masakari_notified = provider.host_down(event_details)
    elif event and event == 'host-up':
        masakari_notified = provider.host_up(event_details)
    else:
        LOG.warn('Invalid request')
        return jsonify(dict(success=False)), 422, CONTENT_TYPE_HEADER
    LOG.info('received %s event for host %s has been processed, result : %s',
             event, str(host_id), str(masakari_notified))
    if masakari_notified:
        return jsonify(dict(success=True)), 200, CONTENT_TYPE_HEADER
    return jsonify(dict(success=False)), 403, CONTENT_TYPE_HEADER


@app.route('/v1/consul/', methods=['GET'])
@app.route('/v1/consul/<int:aggregate_id>', methods=['GET'])
@error_handler
def get_consul_status(aggregate_id=None):
    ha_provider = provider_factory.ha_provider()
    db_provider = provider_factory.db_provider()
    records = db_provider.get_latest_consul_status(aggregate_id)
    results = []
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
        aggregate = ha_provider.get_aggregate_info_for_cluster(record.clusterId)
        aggregate_name = aggregate.name
        aggregate_zone = aggregate.availability_zone
        aggregate_host_ids = aggregate.hosts

        leader = record.leader
        members = json.loads(record.members)
        for host_id in aggregate_host_ids:
            matches = [x for x in members if x['Name'] == host_id]
            if len(matches) <= 0:
                continue
            member = matches[0]
            if not member:
                LOG.warn('host %s exists in aggregate %s but does not '
                         'in consul members report : %s',
                         str(host_id),
                         str(aggregate_id),
                         str(members))
                continue

            # get host status in consul members
            host_status = 'up' if member['Status'] == 1 else 'down'
            # get consul role for host
            host_role = 'server' if member['Tags']['role'] == 'consul' else 'agent'
            is_leader = False
            if host_role == "server":
                # only server role will have port 8300 in Tags
                is_leader = (leader == ('%s:%s' % (member['Addr'], member['Tags']['port'])))

            result = {
                'aggregateName': aggregate_name,
                'zoneName': aggregate_zone,
                'hostId': host_id,
                'hostStatus': host_status,
                'consulRole': host_role,
                'isLeader': is_leader,
                'lastUpdate': datetime.strftime(record.lastUpdate,
                                                '%Y-%m-%d %H:%M:%S')
            }
            results.append(result)
    return jsonify(results)


@app.route('/v1/config/<int:aggregate_id>', methods=['GET'])
@error_handler
def get_hosts_configs(aggregate_id):
    """
    return the reported hosts configs from resmgr,
    :param aggregate_id: the host aggregate name
    :return:
    """
    try:
        ha_provider = provider_factory.ha_provider()
        # scenario 1 - called before /enable API is called
        # to determine whether there is shared nfs server
        # use nova to get list of hosts for given aggregate id
        # scenario 2 - called after the /enable API is called
        # use masakari to get list of host
        config = ha_provider.get_common_hosts_configs(aggregate_id)
        return jsonify(config)
    except exceptions.NoCommonSharedNfsException as e1:
        return jsonify(dict(success=False, error=e1.message)), 500, CONTENT_TYPE_HEADER
    except Exception as e2:
        return jsonify(dict(success=False, error=e2.message)), 500, CONTENT_TYPE_HEADER


def app_factory(global_config, **local_conf):
    return app
