from keystoneauth1 import session 
from keystoneauth1.identity import v3
from six.moves.configparser import ConfigParser
import logging
import requests
import subprocess
import sys

# configure logging
logs_format = '[%(asctime)s] %(levelname)s - %(message)s'
logger = logging.getLogger()
# this is needed in order to see the logs through K8s logs
handler = logging.FileHandler('/var/log/pf9/haprobe.log', mode='w')
logger.setLevel(logging.DEBUG)
formatter = logging.Formatter(logs_format)
handler.setFormatter(formatter)
logger.addHandler(handler)
 
def load_config(config_file):
    config = ConfigParser()
    config.read(config_file)
    return config

def get_keystone_session():
    config = load_config("/etc/pf9/hamgr/hamgr-api-paste.ini")
    auth = v3.Password(
        auth_url = config.get('filter:authtoken', 'auth_url'),
        username = config.get('filter:authtoken', 'username'),
        password = config.get('filter:authtoken', 'password'),
        project_name = config.get('filter:authtoken', 'project_name'),
        user_domain_id = config.get('filter:authtoken', 'user_domain_id'),
        project_domain_id = config.get('filter:authtoken', 'project_domain_id'))
    return session.Session(auth=auth)

if __name__ == "__main__":
    config = load_config("/etc/pf9/hamgr/hamgr.conf")
    sess = get_keystone_session()
    auth_ref = sess.auth.get_auth_ref(sess)
    catalog = auth_ref.service_catalog.get_endpoints(service_type='hamgr', interface='internal',region_name=config.get('DEFAULT', 'region_name'), service_name='hamgr' )
    hamgr_endpoint = catalog['hamgr'][0]['url']
    token = sess.get_token()
    HAMGR_URL = hamgr_endpoint + '/version'
    headers = {"X-AUTH-TOKEN": token}
    response = requests.get(HAMGR_URL, headers=headers,timeout=60)
    data = response.json()
    if response.status_code == 200:
        if data["status"] == None:
            result = subprocess.run(['supervisorctl', 'restart', 'hamgr'], capture_output=True, text=True)
            logger.info(result.stdout)
    else: 
        logger.error(f"Failed to connect hamgr")
        sys.exit(1)
    sys.exit(0)
