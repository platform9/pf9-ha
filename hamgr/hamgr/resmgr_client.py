import requests
import logging

LOG = logging.getLogger(__name__)


class ResmgrClient:
    def __init__(self, base_url):
        if not base_url:
            raise Exception('base_url for ResmgrClient can not be empty')
        self._base_url = base_url.rstrip("/")
        if self._base_url.rfind('v1') == -1:
            self._base_url = self._base_url + "/v1"

    def get(self, token, route):
        headers = {'X-Auth-Token': token, 'Content-Type': 'application/json'}
        req_url = '{0}/{1}'.format(self._base_url, route)
        resp = requests.get(req_url, headers=headers)
        if resp.status_code != requests.codes.ok:
            LOG.info('request "%s" not succeeded, returns : %s', str(req_url), str(resp))
        return resp

    def put(self, token, route, json_data):
        headers = {'X-Auth-Token': token, 'Content-Type': 'application/json'}
        req_url = '{0}/{1}'.format(self._base_url, route)
        resp = requests.put(req_url, headers=headers, json=json_data)
        if resp.status_code != requests.codes.ok:
            LOG.info('request "%s" not succeeded, returns : %s', str(req_url), str(resp))
        return resp

    def post(self, token, route, json_data):
        headers = {'X-Auth-Token': token, 'Content-Type': 'application/json'}
        req_url = '{0}/{1}'.format(self._base_url, route)
        resp = requests.post(req_url, headers=headers, json=json_data)
        if resp.status_code != requests.codes.ok:
            LOG.info('request "%s" not succeeded, returns : %s', str(req_url), str(resp))
        return resp

    def delete(self, token, route):
        headers = {'X-Auth-Token': token, 'Content-Type': 'application/json'}
        req_url = '{0}/{1}'.format(self._base_url, route)
        resp = requests.delete(req_url, headers=headers)
        if resp.status_code != requests.codes.ok:
            LOG.info('request "%s" not succeeded, returns : %s', str(req_url), str(resp))
        return resp

    def get_hosts_info(self, token):
        return self.get(token, "/hosts")

    def get_host_info(self, host_id, token):
        return self.get(token, "/hosts/{0}".format(host_id))

    def get_role_settings(self, host_id, role_name, token):
        return self.get(token, "/hosts/{0}/roles/{1}".format(host_id, role_name))

    def update_role(self, host_id, role_name, role_settings, token):
        return self.put(token, "/hosts/{0}/roles/{1}".format(host_id, role_name), role_settings)

    def delete_role(self, host_id, role_name, token):
        return self.delete(token, "/hosts/{0}/roles/{1}".format(host_id, role_name))

    def fetch_hosts_details(self, host_ids , token):
        hosts_details = {}
        for host_id in host_ids:
            resp = self.get_host_info(host_id, token)
            host_info = {}
            if resp.status_code == requests.codes.ok:
                host_info = resp.json()
            hosts_details[host_id] = host_info
            roles = host_info.get('roles', [])
            settings = {}
            for role in roles:
                resp = self.get_role_settings(host_id, role, token)
                role_settings = {}
                if resp.status_code == requests.codes.ok:
                    role_settings = resp.json()
                settings.update({role:role_settings})
            hosts_details[host_id].update(dict(role_settings=settings))

        return hosts_details