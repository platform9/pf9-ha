import requests
import logging
from shared.constants import LOGGER_PREFIX

LOG = logging.getLogger(LOGGER_PREFIX + __name__)


class ResmgrClient:
    def __init__(self, base_url):
        if not base_url:
            raise Exception('base_url for ResmgrClient can not be empty')
        self._base_url = base_url.rstrip("/")
        if self._base_url.rfind('v1') == -1:
            self._base_url = self._base_url + "/v1"

    def _send_request(self, method, url, token, data={}):
        headers = {'X-Auth-Token': token, 'Content-Type': 'application/json'}
        if method == "get":
            resp = requests.get(url, headers=headers)
            resp.raise_for_status()
            return resp.json()
        if method == "put":
            resp = requests.put(url, headers=headers, json=data)
        if method == "delete":
            resp = requests.delete(url, headers=headers)
        return resp

    def get_hosts_info(self, token):
        return self._send_request("get", self._base_url + "/hosts", token)

    def get_host_info(self, host_id, token):
        url = "%s/hosts/%s" % (self._base_url, host_id)
        return self._send_request("get", url, token)

    def get_role_settings(self, host_id, role_name, token):
        url = "%s/hosts/%s/roles/%s" % (self._base_url, host_id, role_name)
        return self._send_request("get", url, token)

    def get_app_info(self, host_id, token):
        url = "http://localhost:8082/v1/hosts/%s/apps" % host_id
        return self._send_request("get", url, token)

    def update_role(self, host_id, role_name, role_settings, token):
        url = "%s/hosts/%s/roles/%s" % (self._base_url, host_id, role_name)
        return self._send_request("put", url, token, role_settings)

    def delete_role(self, host_id, role_name, token):
        url = "%s/hosts/%s/roles/%s" % (self._base_url, host_id, role_name)
        return self._send_request("delete", url, token)

    def fetch_hosts_details(self, host_ids, token):
        hosts_details = {}
        for host_id in host_ids:
            host_info = self.get_host_info(host_id, token)
            hosts_details[host_id] = host_info
            roles = host_info.get('roles', [])
            settings = {}
            for role in roles:
                if role not in ["pf9-ostackhost-neutron", "pf9-ha-slave"]:
                    continue
                role_settings = self.get_role_settings(host_id, role, token)
                settings.update({role: role_settings})
            hosts_details[host_id].update(dict(role_settings=settings))
        return hosts_details

    def fetch_app_details(self, host_ids, token):
        app_details = {}
        for host_id in host_ids:
            app_info = self.get_app_info(host_id, token)
            if "desired_apps" not in app_info.get("pf9-ha-slave", {}):
                app_details[host_id] = {"converged": True}
        return app_details
