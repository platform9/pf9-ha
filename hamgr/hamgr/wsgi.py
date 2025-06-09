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
import requests
import time
from hamgr import app
from hamgr.context import error_handler
from shared.exceptions import ha_exceptions as exceptions
import shared.constants as constants
from hamgr import provider_factory
from shared.constants import LOGGER_PREFIX

LOG = logging.getLogger(LOGGER_PREFIX + __name__)

CONTENT_TYPE_HEADER = {'Content-Type': 'application/json'}

VMHA_CACHE = {}
VMHA_TABLE={}
MAX_FAILED_TIME = 10
# ^ in sec

class MockEvent:
    def __init__(self, kwargs):
        for key, value in kwargs.items():
            setattr(self, key, value)


def get_provider():
    return provider_factory.ha_provider()


def get_providers_for_host(host_id, event_type):
    """Get the appropriate providers for a host based on the services it runs.
    
    A host can run both cinder and nova services, so we need to return all
    applicable providers.
    
    :param host_id: Host ID
    :param event_type: Event type (host-up or host-down)
    :return: List of applicable providers for the host
    """
    providers = []
    
    cinder_provider = provider_factory.cinder_provider()
    cinder_provider._token = cinder_provider._get_v3_token()
    cinder_hosts = cinder_provider._get_cinder_hosts()
    
    if host_id in cinder_hosts:
        LOG.info('Host %s runs cinder volume services', host_id)
        providers.append(cinder_provider)
    
    nova_provider = provider_factory.ha_provider()
    nova_provider._token = nova_provider._get_v3_token()
    
    LOG.info('Adding nova provider for host %s', host_id)
    providers.append(nova_provider)
    
    return providers


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
    
    # Get all applicable providers for this host
    providers = get_providers_for_host(host_id, event)
    
    # Track if all providers successfully processed the event
    success = True
    
    # Process the event with all applicable providers
    for provider in providers:
        try:
            if event and event == 'host-down':
                provider_success = provider.host_down(event_details)
            elif event and event == 'host-up':
                provider_success = provider.host_up(event_details)
            else:
                LOG.warning('Invalid request')
                return jsonify(dict(success=False)), 422, CONTENT_TYPE_HEADER
                
            LOG.info('Provider %s processed %s event for host %s, result: %s',
                     provider.__class__.__name__, event, host_id, str(provider_success))
            
            # Success is true only if all providers succeed
            success = success and provider_success
        except Exception as e:
            LOG.exception('Provider %s failed to process %s event for host %s: %s',
                         provider.__class__.__name__, event, host_id, str(e))
    
    if success:
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


@app.route('/v1/vmha/hostlist/<host_id>', methods=['GET'])
@error_handler
def host_list_handler(host_id):
    """
    VMHA agent queries this endpoint to get list of the hosts with same
    cluster to gossip to.

    Args:
        host_id (str): The host_id of the host which wants to get the list of its neighbours
    """
    nova_provider = provider_factory.ha_provider()
    nova_provider._token = nova_provider._get_v3_token()
    
    # Using this as a simple cleanup job for VMHA CACHE. This does not correspond to the functionality of the endpoint
    # but I kinda find it would be easier to do here
    try:
        for host in VMHA_CACHE:
            ret = nova_provider.get_ip_from_host_id(host)
            if not ret:
                # Asserting if the ip of that host doesn't exist then host doesn't exist
                VMHA_CACHE.pop(host, None)
    except:
        pass
        
    return jsonify(nova_provider.generate_ip_list(host_id))


@app.route('/v1/vmha/hoststatus/<host_id>', methods=['POST'])
@error_handler
def host_status_handler(host_id):
    """
    VMHA agents send status of hosts to this endpoint

    Args:
        host_id (str): The host_id of the host malfunctioning
    """
    
    body = request.get_json()
    LOG.debug(f"Body of request for hoststatus {body}")
    if len(body)==0:
        return jsonify(dict(success=False, error="host not found in body")), 412, CONTENT_TYPE_HEADER
    
    # Use nova provider
    nova_provider = provider_factory.ha_provider()

    # Get list of all the hosts that are in failed state
    for host in body:
        if host not in VMHA_TABLE:
            VMHA_TABLE[host]=[]
        if body[host]!="Success":
            VMHA_TABLE[host].append(False)
        else:
            VMHA_TABLE[host].append(True)
        # Remove older status to keep a queue of most recent statuses
        if len(VMHA_TABLE[host]) >= 5:
            VMHA_TABLE[host].pop(0)
        LOG.debug(f"Cache looks like {VMHA_CACHE}. Table looks like this {VMHA_TABLE}")
        if VMHA_TABLE[host].count(True)-VMHA_TABLE[host].count(False) <= 0:
            if host in VMHA_CACHE:
                if time.time() - VMHA_CACHE[host] > MAX_FAILED_TIME and VMHA_CACHE[host]!=0:
                    LOG.info(f"Triggering migration of VMs on host {host} after being failed for {time.time() - VMHA_CACHE[host]} seconds")
                    event = MockEvent(
                        {
                            'host_name': host,
                            'event_time': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                            'event_type': 'COMPUTE_HOST',
                            'event_uuid': None,
                            'event': 'FAILED',
                            'host_status': 'DOWN'
                        }
                    )
                    ret = nova_provider._report_event_to_masakari(event)
                    LOG.info(f"Return from masakari func {ret}")
                    if ret==None:
                        return jsonify(dict(success=False, error="unable to send masakari notification")), 500, CONTENT_TYPE_HEADER
                    # The host is processed. Escape the check above
                    VMHA_CACHE[host] = 0
            else:
                LOG.debug(f"Start timer on host {host}")
                VMHA_CACHE[host] = time.time()
        else:
            # the host is OK
            # Remove it from cache if it exists
            if host in VMHA_CACHE:
                LOG.debug(f"Host {host} is back up. Removing from VMHA_CACHE")
                VMHA_CACHE.pop(host, None)

    return jsonify(dict(success=True)), 204, CONTENT_TYPE_HEADER

def app_factory(global_config, **local_conf):
    global VMHA_CACHE
    global VMHA_TABLE
    VMHA_CACHE = {}
    VMHA_TABLE = {}
    return app
