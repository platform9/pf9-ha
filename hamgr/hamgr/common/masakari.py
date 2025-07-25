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

import requests
from six.moves.configparser import ConfigParser

import shared.exceptions.ha_exceptions as exceptions
from shared.constants import LOGGER_PREFIX

LOG = logging.getLogger(LOGGER_PREFIX + __name__)

conf = ConfigParser()
conf.read(['/etc/pf9/hamgr/hamgr.conf'])

_URL = conf.get("masakari", "masakari_endpoint")


def get_failover_segment(token, name):
    url = '/'.join([_URL, 'segments'])
    headers = {'X-Auth-Token': token['id'],
               'Content-Type': 'application/json'}
    resp = requests.get(url, headers=headers)
    LOG.debug('resp when get segment %s : %s', name, str(resp.__dict__))
    resp.raise_for_status()

    if 'segments' in resp.json():
        expected_seg = [s for s in resp.json()['segments'] if s['name'] == name]
        if len(expected_seg) == 0:
            raise exceptions.SegmentNotFound(name)
    else:
        raise exceptions.SegmentNotFound(name)
    return expected_seg[0]


def get_all_failover_segments(token):
    url = '/'.join([_URL, 'segments'])
    headers = {'X-Auth-Token': token['id'],
               'Content-Type': 'application/json'}
    resp = requests.get(url, headers=headers)
    resp.raise_for_status()
    return resp.json()['segments']


def is_failover_segment_exist(token, segment_name):
    try:
        get_failover_segment(token, segment_name)
    except exceptions.SegmentNotFound:
        return False
    return True


def get_nodes_in_segment(token, name):
    segment = None
    try:
        segment = get_failover_segment(token, name)
    except exceptions.SegmentNotFound:
        segment = None

    if segment is None:
        return []

    url = '/'.join([_URL, 'segments', segment['uuid'], 'hosts'])
    headers = {'X-Auth-Token': token['id'],
               'Content-Type': 'application/json'}
    resp = requests.get(url, headers=headers)
    resp.raise_for_status()
    LOG.debug('resp when get nodes from segment %s : %s', name, str(resp.__dict__))
    return resp.json()['hosts']


def is_failover_segment_under_recovery(token, name):
    hosts = []
    try:
        # find all hosts in current segment
        hosts = get_nodes_in_segment(token, name)
    except exceptions.SegmentNotFound:
        return False

    # get notifications for those hosts
    for host in hosts:
        notifications = get_notifications(token, host['uuid'])['notifications']
        # if for any host there are associated notifications in
        # 'new', 'error', 'running' state, then it means this
        # failover segment is locked for recovery
        recovery_states = ['new', 'error', 'running']
        has_target_status = [x for x in notifications if
                             x['status'] in recovery_states]
        if len(has_target_status) > 0:
            LOG.info('masakari segment %s is under recovery, as host %s '
                     'has notifications in recovery state %s : %s',
                     name,
                     host['uuid'],
                     str(recovery_states),
                     str(has_target_status))
            return True

    return False


def delete_all_nodes_in_failover_segment(token, name):
    headers = {'X-Auth-Token': token['id']}
    try:
        nodes = get_nodes_in_segment(token, name)
        for node in nodes:
            if node.get('on_maintenance', None):
                LOG.warning('host %s in segment %s is on maintenance',
                            str(node['name']), str(name))
            url = '/'.join([_URL, 'segments', node['failover_segment_id'],
                            'hosts', node['uuid']])
            LOG.info('Removing host from masakari segment %s: %s', str(name), str(url))
            resp = requests.delete(url, headers=headers)
            if resp.status_code not in [requests.codes.no_content,
                                        requests.codes.not_found]:
                LOG.warning('unexpected status %s while deleting host %s',
                            str(resp), str(node))
                resp.raise_for_status()
    except Exception:
        LOG.exception('error when deleting host from segment %s', name)
        raise


def delete_failover_segment(token, name):
    headers = {'X-Auth-Token': token['id']}
    seg = None
    try:
        seg = get_failover_segment(token, name)
    except exceptions.SegmentNotFound as e:
        LOG.error('error when get segment info for %s : %s', str(name), str(e))
        return
    if seg is None:
        LOG.debug('masakari segment %s does not exist ', str(name))
        return

    # Delete hosts in failover segment before deleting the segment itself
    delete_all_nodes_in_failover_segment(token, name)
    # Retry once more to confirm all hosts are deleted from the segment
    delete_all_nodes_in_failover_segment(token, name)

    url = '/'.join([_URL, 'segments', seg['uuid']])
    LOG.debug('delete masakari segment : %s', str(url))
    resp = requests.delete(url, headers=headers)
    if resp.status_code not in [requests.codes.no_content,
                                requests.codes.not_found]:
        LOG.error('unexpected status %s when deleting segment %s', str(resp),
                  str(name))
        resp.raise_for_status()
    else:
        LOG.info('Successfully deleted masakari segment %s', str(name))


def create_failover_segment(token, name, hosts):
    existing_segment = None
    try:
        name = str(name)
        existing_segment = get_failover_segment(token, name)
    except exceptions.SegmentNotFound:
        existing_segment = None

    headers = {'X-Auth-Token': token['id'], 'Content-Type': 'application/json'}
    url = '/'.join([_URL, 'segments'])
    data = dict(name=name, service_type='COMPUTE', recovery_method='auto', description='Created by HA Manager')
    if existing_segment:
        LOG.warning('Segment %s already exists, now try to update it if needed : %s', name, str(existing_segment))
        # update it rather than delete then re-create
        if existing_segment['service_type'] != data['service_type'] or \
                existing_segment['recovery_method'] != data['recovery_method'] or \
                existing_segment['description'] != data['description']:
            resp = requests.put(url + "/" + str(existing_segment['id']), headers=headers,
                                data=json.dumps(dict(segment=data)))
            resp.raise_for_status()
            LOG.debug('Updated existing masakari segment %s with properties : %s', name, str(data))

        # now update hosts
        existing_hosts = get_nodes_in_segment(token, name)
        existing_host_names = [x['name'] for x in existing_hosts]
        common = set(existing_host_names).intersection(set(hosts))
        hosts_deleted = set(existing_host_names) - set(common)
        hosts_added = set(hosts) - set(common)
        if len(hosts_deleted):
            LOG.info('Remove hosts for existing masakari segment %s : %s', name, str(hosts_deleted))
            delete_hosts_from_failover_segment(token, name, list(hosts_deleted))
        if len(hosts_added):
            LOG.debug('Add hosts for existing masakari segment %s : %s', name, str(hosts_added))
            add_hosts_to_failover_segment(token, name, list(hosts_added))
    else:
        LOG.info('Segment %s not exists, now try to create one', name)
        resp = requests.post(url, headers=headers, data=json.dumps(dict(segment=data)))
        resp.raise_for_status()

        # add hosts into the segment
        add_hosts_to_failover_segment(token, name, hosts)


def create_notification(token, ntype, hostname, time, payload):
    data = dict(type=ntype, hostname=hostname, generated_time=time,
                payload=payload)
    headers = {
        'X-Auth-Token': token['id'],
        'Content-Type': 'application/json'
    }
    url = '/'.join([_URL, 'notifications'])
    resp = requests.post(url, headers=headers,
                         data=json.dumps(dict(notification=data)))
    if resp.status_code == requests.codes.accepted:
        LOG.debug('Status notification successfully accepted by masakari')
    elif resp.status_code == requests.codes.conflict and \
            resp.content.find(b'ignored as the host is already under maintenance'):
        LOG.warning('Masakari ignored the notification since host %s is already '
                    'under maintenance', hostname)
    else:
        LOG.error('Masakari rejected notification with error %s: %s',
                  resp.status_code, resp)
        resp.raise_for_status()
    LOG.debug('masakari notification is created : %s', str(resp.json()))
    return resp.json()


def get_notifications(token, host_id, generated_since=None):
    """Get all the notifications for the specific host ID"""
    headers = {
        'X-Auth-Token': token['id'],
        'Content-Type': 'application/json'
    }
    url = '/'.join([_URL, 'notifications'])
    query_params = {
        'source_host_uuid': host_id
    }
    if generated_since:
        query_params['generated-since'] = generated_since
    resp = requests.get(url, params=query_params, headers=headers)
    if resp.status_code == requests.codes.ok:
        LOG.debug('Fetched masakari notifications for %s', host_id)
    else:
        LOG.error('Error fetching masakari notifications for %s', host_id)
        resp.raise_for_status()
    return resp.json()


def get_notification_status(token, uuid):
    headers = {
        'X-Auth-Token': token['id'],
        'Content-Type': 'application/json'
    }
    url = '/'.join([_URL, 'notifications', uuid])
    resp = requests.get(url, headers=headers)
    resp.raise_for_status()
    obj = resp.json()
    return obj['notification']['status']


def is_host_on_maintenance(token, host_id, segment_name):
    # confirm the given segment_id eixist
    target_segment = get_failover_segment(token, segment_name)
    # confirm host exist in target segment
    hosts = get_nodes_in_segment(token, segment_name)
    xhosts = [x for x in hosts if x['name'] == str(host_id)]
    if len(xhosts) != 1:
        LOG.error('host %s does not exist in masakari segment %s', host_id,
                  segment_name)
        return False

    host_uuid = xhosts[0]['uuid']
    on_maintenance = xhosts[0]['on_maintenance']
    LOG.debug('uuid of host %s : %s , on_maintenance : %s', host_id, str(host_uuid), str(on_maintenance))
    return on_maintenance


def update_host_maintenance(token, host_id, segment_name, on_maintenance):
    if not host_id:
        raise exceptions.ArgumentException('host_id is null or empty')
    if not segment_name:
        raise exceptions.ArgumentException('segment_name is null or empty')
    if str(on_maintenance).lower() not in ['true', 'false']:
        raise exceptions.ArgumentException('on_maintenance can only be true or false')

    # confirm the given segment_id exists
    target_segment = get_failover_segment(token, segment_name)
    LOG.debug('found segment %s : %s', segment_name, str(target_segment))
    segment_uuid = target_segment['uuid']
    # confirm host exist in target segment
    hosts = get_nodes_in_segment(token, segment_name)
    xhosts = [x for x in hosts if x['name'] == str(host_id)]
    if len(xhosts) != 1:
        LOG.error('host %s does not exist in masakari segment %s', host_id,
                  segment_name)
        return
    host_uuid = xhosts[0]['uuid']
    LOG.debug('uuid of host %s : %s', host_id, str(host_uuid))
    # update the host maintenance status
    headers = {
        'X-Auth-Token': token['id'],
        'Content-Type': 'application/json'
    }
    url = '/'.join([_URL, 'segments', segment_uuid, 'hosts', host_uuid])
    data = dict(host=dict(name=host_id,
                          on_maintenance=on_maintenance,
                          type='COMPUTE',
                          reserved='False',
                          control_attributes=''))
    resp = requests.put(url, headers=headers, data=json.dumps(data))
    resp.raise_for_status()
    LOG.debug('host %s in segment %s is updated with maintenance status : '
              '%s', host_id, segment_name, str(on_maintenance))


def add_hosts_to_failover_segment(token, segment_name, host_ids):
    segment = None
    existing_hosts = []
    try:
        segment = get_failover_segment(token, str(segment_name))
        existing_hosts = get_nodes_in_segment(token, str(segment_name))
    except exceptions.SegmentNotFound:
        LOG.warning('masakari segment %s does not exist for adding hosts %s', segment_name, str(host_ids))
        return

    if segment is None:
        LOG.warning('masakari segment %s does not exist', segment_name)
        return

    url = '/'.join([_URL, 'segments', segment['uuid'], 'hosts'])
    headers = {'X-Auth-Token': token['id'], 'Content-Type': 'application/json'}
    for hid in host_ids:
        # check whether the host has already been added to avoid conflict
        if len(existing_hosts) > 0 and len([x for x in existing_hosts if x['name'] == hid]) > 0:
            LOG.debug('ignore adding host %s as it already exist in segment %s', hid, segment_name)
            continue
        data = dict(host=dict(name=hid,
                              type='COMPUTE',
                              reserved='False',
                              on_maintenance='False',
                              control_attributes=''))
        resp = requests.post(url, headers=headers, data=json.dumps(data))
        resp.raise_for_status()


def delete_hosts_from_failover_segment(token, segment_name, host_ids):
    segment = None
    try:
        segment = get_failover_segment(token, str(segment_name))
    except exceptions.SegmentNotFound:
        LOG.warning('masakari segment %s does not exist for deleting hosts %s', segment_name, str(host_ids))
        return

    if segment is None:
        LOG.warning('masakari segment %s does not exist', segment_name)
        return

    # when delete a host, requires host uuid, not host name (id)
    headers = {'X-Auth-Token': token['id']}
    segment_uuid = segment['uuid']
    hosts = get_nodes_in_segment(token, segment_name)
    for hid in host_ids:
        matched = [x for x in hosts if x['name'] == hid]
        if len(matched) != 1:
            continue
        uuid = matched[0]['uuid']
        url = '/'.join([_URL, 'segments', segment_uuid, 'hosts', uuid])
        resp = requests.delete(url, headers=headers)
        resp.raise_for_status()
