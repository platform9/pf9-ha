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

    expected_seg = filter(lambda s: s['name'] == name, resp.json()['segments'])

    if len(expected_seg) == 0:
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
    seg = None
    try:
        seg = get_failover_segment(token, name)
    except exceptions.SegmentNotFound:
        return

    url = '/'.join([_URL, 'segments', seg['uuid']])
    headers = {'X-Auth-Token': token['id']}

    resp = requests.delete(url, headers=headers)

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
                              on_maintenant='False',
                              control_attributes=''))
        resp = requests.post(url, headers=headers, data=data)
        resp.raise_for_status()


