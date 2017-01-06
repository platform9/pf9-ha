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
import requests
import logging

from hamgr import exceptions

LOG = logging.getLogger(__name__)
_URL = 'http://localhost:8080/masakari/v1'

def get_failover_segment(token, name):
    url = '/'.join([_URL, 'segments'])
    headers = {'X-Auth-Token': token['id'],
               'Content-Type': 'application/json'}
    resp = requests.get(url, headers=headers)
    resp.raise_for_status()

    if 'segments' in resp.json():
        expected_seg = filter(lambda s: s['name'] == name, resp.json()['segments'])
        if len(expected_seg) == 0:
            raise exceptions.SegmentNotFound(name)
    else:
        raise exceptions.SegmentNotFound(name)

    return expected_seg[0]

def get_nodes_in_segment(token, name):
    segment = get_failover_segment(token, name)
    url = '/'.join([_URL, 'segments', segment['uuid'], 'hosts'])
    headers = {'X-Auth-Token': token['id'],
               'Content-Type': 'application/json'}
    resp = requests.get(url, headers=headers)
    resp.raise_for_status()

    return resp.json()['hosts']


def delete_failover_segment(token, name):
    headers = {'X-Auth-Token': token['id']}
    seg = None
    try:
        seg = get_failover_segment(token, name)
        # Delete hosts in failover segment before deleting the segment itself
        nodes = get_nodes_in_segment(token, name)
        for node in nodes:
            url = '/'.join([_URL, 'segments', node['failover_segment_id'], 'hosts', node['uuid']])
            resp = requests.delete(url, headers=headers)
            if resp.status_code not in [ requests.codes.no_content, requests.codes.not_found ]:
                resp.raise_for_status()
    except exceptions.SegmentNotFound:
        return

    url = '/'.join([_URL, 'segments', seg['uuid']])

    resp = requests.delete(url, headers=headers)
    if resp.status_code not in [ requests.codes.no_content, requests.codes.not_found ]:
        resp.raise_for_status()


def create_failover_segment(token, name, hosts):
    try:
        _ = get_failover_segment(token, name)
    except exceptions.SegmentNotFound:
        pass
    else:
        LOG.warn('Segment %s already exists', name)
        delete_failover_segment(token, name)

    headers = {'X-Auth-Token': token['id'], 'Content-Type': 'application/json'}
    url = '/'.join([_URL, 'segments'])
    data = dict(name=name, service_type='COMPUTE', recovery_method='auto', description='Created by HA Manager')

    resp = requests.post(url, headers=headers, data=json.dumps(dict(segment=data)))
    resp.raise_for_status()

    seg = resp.json()['segment']
    url = '/'.join([_URL, 'segments', seg['uuid'], 'hosts'])
    for h in hosts:
        data = dict(host=dict(name=h,
                              type='COMPUTE',
                              reserved='False',
                              on_maintenance='False',
                              control_attributes=''))
        resp = requests.post(url, headers=headers, data=json.dumps(data))
        resp.raise_for_status()


def create_notification(token, ntype, hostname, time, payload):
    data = dict(type=ntype, hostname=hostname, generated_time=time, payload=payload)
    headers = {
        'X-Auth-Token': token['id'],
        'Content-Type': 'application/json'
    }
    url = '/'.join([_URL, 'notifications'])
    resp = requests.post(url, headers=headers,
                         data=json.dumps(dict(notification=data)))
    if resp.status_code == requests.codes.accepted:
        LOG.info('Status notification successfully accepted by masakari')
        LOG.info('Masakari response: %s', resp.json())
    elif resp.status_code == requests.codes.conflict and \
        resp.content.find('ignored as the host is already under maintenance'):
        LOG.warn('Masakari ignored the notification since host %s is already under maintenance',
                 hostname)
    else:
        LOG.error('Masakari rejected notification with error %s: %s', resp.status_code, resp)
        resp.raise_for_status()


def get_notifications(token, host_id, generated_since=None):
    """
    Get all the notifications for the specific host ID
    """
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
        LOG.debug('Fetched notifications for %s', host_id)
    else:
        LOG.error('Error fetching notifications for %s', host_id)
        resp.raise_for_status()
    return resp.json()
