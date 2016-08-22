#
# Copyright (c) 2016, Platform9 Systems. All Rights Reserved
#
import logging
import requests
import json

from novaclient import client, exceptions

from provider import Provider
import hamgr.db.api as db_api
import hamgr.exceptions as ha_exceptions

LOG = logging.getLogger(__name__)


class NovaProvider(Provider):

    def __init__(self, config):
        self._username = config.get('keystone_middleware', 'admin_user')
        self._passwd = config.get('keystone_middleware', 'admin_password')
        self._auth_uri = config.get('keystone_middleware', 'auth_uri')
        self._tenant = config.get('keystone_middleware', 'admin_tenant_name')
        self._region = config.get('nova', 'region')

    def _get_client(self):
        return client.Client(2,
                             self._username,
                             self._passwd,
                             self._tenant,
                             self._auth_uri + '/v2.0',
                             region_name=self._region,
                             insecure=True,
                             connection_pool=False)

    def _get_token(self):
        headers = {'Content-Type': 'application/json'}
        data = json.dumps(dict(auth=dict(tenantName=self._tenant,
                                         passwordCredentials=dict(username=self._username,
                                                                  password=self._passwd))))
        url = self._auth_uri + '/v2.0/tokens'
        resp = requests.post(url, headers=headers, data=data, verify=False)
        resp.raise_for_status()
        return resp.json()

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
            cluster = db_api.get_cluster(aggregate_id)
        except ha_exceptions.ClusterNotFound:
            pass

        enabled = cluster.enabled if cluster is not None else False
        return dict(id=aggregate_id, enabled=enabled)

    def get(self, aggregate_id):
        client = self._get_client()

        return [self._get_one(client, aggregate_id)] if aggregate_id is not None \
            else self._get_all(client)

    def _get_aggregate(self, client, aggregate_id):
        try:
            return client.aggregates.get(aggregate_id)
        except exceptions.NotFound:
            raise ha_exceptions.AggregateNotFound(aggregate_id)

    def _validate_cluster(self, aggregate_id, enabled):
        cluster = db_api.get_cluster(aggregate_id)
        return cluster.enabled == enabled

    def _validate_hosts(self, hosts, aggregate_id):
        # TODO Make this check configurable
        if len(hosts) < 3:
            raise ha_exceptions.InsufficientHosts()
        # TODO check if host is part of any other aggregates

    def _auth(self, ip_lookup, cluster_id, token, nodes, role, ip=None):
        assert role in ['server', 'agent']
        url = 'http://localhost:8080/resmgr/v1/hosts/'
        headers = {'X-Auth-Token': token['access']['token']['id'],
                   'Content-Type': 'application/json'}
        for node in nodes:
            data = dict(join=ip, ip_address=ip_lookup[node].host_ip)
            data['bootstrap_expect'] = 3 if role == 'server' else 0
            auth_url = '/'.join([url, node, 'roles', 'pf9-ha-slave'])
            resp = requests.put(auth_url, headers=headers,
                                json=data, verify=False)
            resp.raise_for_status()
            role_id = -1
            if role == 'server':
                if ip == ip_lookup[node].host_ip:
                    # leader
                    role_id = db_api.Role.leader
                else:
                    # server
                    role_id = db_api.Role.server
            else:
                # agent
                role_id = db_api.Role.agent
            db_api.update_node(node, cluster_id,  role_id)

    def _get_ips(self, client, nodes):
        all_hypervisors = client.hypervisors.list()
        lookup = set(nodes)
        ip_lookup = dict()
        for hyp in all_hypervisors:
            if getattr(hyp, 'OS-EXT-PF9-HYP-ATTR:host_id') in lookup:
                ip_lookup[getattr(hyp, 'OS-EXT-PF9-HYP-ATTR:host_id')] = hyp
        return ip_lookup

    def _assign_roles(self, client, cluster_id, hosts):
        hosts = sorted(hosts)
        leader = hosts[0]
        servers = hosts[1:3]

        agents = []
        if len(hosts) >= 5:
            servers += hosts[3:5]
            agents = hosts[5:]

        ip_lookup = self._get_ips(client, hosts)
        if leader not in ip_lookup:
            LOG.error('Leader %s not found in nova', leader)
            raise exceptions.HostNotFound(leader)

        leader_ip = ip_lookup[leader].host_ip
        token = self._get_token()
        self._auth(ip_lookup, cluster_id, token, [leader] + servers,
                   'server', ip=leader_ip)
        self._auth(ip_lookup, cluster_id, token, agents, 'agent',
                   ip=leader_ip)

    def _enable(self, aggregate_id):
        client = self._get_client()
        db_api.create_cluster_if_needed(aggregate_id)

        if self._validate_cluster(aggregate_id, False) is False:
            LOG.info('Cluster %s already HA enabled', aggregate_id)
            return

        aggregate = self._get_aggregate(client, aggregate_id)
        cluster = db_api.get_cluster(aggregate_id)
        db_api.add_nodes_if_needed(aggregate.hosts, cluster.id)
        self._validate_hosts(aggregate.hosts, cluster.id)
        self._assign_roles(client, cluster.id, aggregate.hosts)
        db_api.update_cluster(cluster.id, True)

    def _deauth(self, nodes):
        token = self._get_token()
        headers = {'X-Auth-Token': token['access']['token']['id'],
                   'Content-Type': 'application/json'}
        url = 'http://localhost:8080/resmgr/v1/hosts/'
        for node in nodes:
            auth_url = '/'.join([url, node, 'roles', 'pf9-ha-slave'])
            resp = requests.delete(auth_url, headers=headers)
            resp.raise_for_status()

    def _disable(self, aggregate_id):
        if self._validate_cluster(aggregate_id, True) is False:
            LOG.info('Cluster %s already HA disabled', aggregate_id)
            return

        cluster = db_api.get_cluster(aggregate_id)
        nodes = db_api.get_all_nodes(cluster_id=cluster.id)
        if nodes:
            self._deauth(nodes)
        db_api.update_cluster(cluster.id, False)
    
    def put(self, aggregate_id, method):
        if method == 'enable':
            self._enable(aggregate_id)
        else:
            self._disable(aggregate_id)


def get_provider(config):
    db_api.init(config)
    return NovaProvider(config)

