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
import base64
import os
import datetime
from cryptography import x509
from cryptography.hazmat.backends import default_backend
from subprocess import Popen, PIPE
from shared.constants import LOGGER_PREFIX

from six import iteritems
from six.moves.configparser import ConfigParser

LOG = logging.getLogger(LOGGER_PREFIX + __name__)

# /etc/pf9
global_config_base = '/etc/pf9'
# /etc/pf9/hamgr
hamgr_config_base = os.path.join(global_config_base, 'hamgr')
# /etc/pf9/hamgr/hamgr.conf
hamgr_config = os.path.join(hamgr_config_base, 'hamgr.conf')


# reference https://jamielinux.com/docs/openssl-certificate-authority/

def execute_shell_command(command, in_directory=None, env_variables=dict()):
    envs = os.environ.copy()
    if env_variables:
        for x, y in iteritems(env_variables):
            envs[x] = y
    LOG.debug('command "%s"', str(command))
    if in_directory:
        os.chdir(in_directory)
    p = Popen(command,
              stdout=PIPE,
              stderr=PIPE,
              cwd=in_directory,
              env=envs,
              shell=True)
    msg, err = p.communicate()
    ret = p.returncode
    LOG.debug('command "%s" finished. returncode: %s, stdout: %s, stderr: %s',
              str(command), str(ret), str(msg), str(err))
    return ret


def clean_openssl_index_db():
    folder_certs = os.path.join(hamgr_config_base, 'certs')
    file_index = os.path.join(folder_certs, 'index.txt')
    if os.path.exists(file_index):
        execute_shell_command('rm -f %s' % file_index)
        execute_shell_command('touch %s' % file_index)


def get_consul_gossip_encryption_key(cluster_name="", seed=""):
    # the key is composed with starting magic code 'pf9-dc'
    # and the cluster name (availability zone name), if longer
    # than require 16 bytes, then trim it, when shorter ,
    # append 0 until length is 16
    str_name = "-" + str(cluster_name)
    key = 'pf9-dc%s' % str_name
    if seed:
        key = seed
    # key needs to be 16 bytes
    total = len(key) + len(str_name)
    if total <= 16:
        key = key + str_name
    else:
        if len(key) > 16:
            if len(str_name) > 16:
                key = key[0:16]
            else:
                key = key[0:(16 - len(str_name))] + str_name
        else:
            if len(str_name) < 16:
                key = key[0:(16 - len(str_name))] + str_name

    needed = 16 - len(key)
    if needed <= 0:
        key = key[0:16]
    else:
        while len(key) < 16:
            key = key + '-0'
        key = key[0:16]
    LOG.debug('consul gossip encryption key clear text : %s', key)
    # key needs to be 64 base encoded
    return base64.b64encode(key.encode()).decode()


def get_general_configs():
    # prefer the hamgr config first, if not exist then fall back
    # to global config. with this way we can change ansible later to
    # pass values to hamgr config and retire global config
    configs = dict(du_fqdn=None,
                   customer_shortname=None,
                   customer_fullname=None,
                   region_name=None)
    parser = ConfigParser()
    section = 'DEFAULT'
    if not os.path.exists(hamgr_config):
        return configs
    with open(hamgr_config) as fp:
        parser.readfp(fp)

    # detect settings from hamgr config
    if parser.has_option(section, 'du_fqdn') and \
            parser.has_option(section, 'customer_shortname') and \
            parser.has_option(section, 'customer_fullname') and \
            parser.has_option(section, 'region_name'):
        LOG.debug('read du general settings from %s', hamgr_config)
        configs['du_fqdn'] = parser.get(section, 'du_fqdn')
        configs['customer_shortname'] = parser.get(section,
                                                   'customer_shortname')
        configs['customer_fullname'] = parser.get(section,
                                                  'customer_fullname')
        configs['region_name'] = parser.get(section, 'region_name')

    # if no required settings from hamgr config, throw exception
    if not configs['du_fqdn'] or \
            not configs['customer_shortname'] or \
            not configs['customer_fullname'] or \
            not configs['region_name']:
        raise Exception('missing configuration values in file %s for '
                        'du_fqdn , customer_shortname , customer_fullname, region_name', hamgr_config)

    return configs


def symbolic_link_ca_key_cert_pairs_if_exist():
    # create required folders
    setup_folders_if_not_exist()

    # if deployment created CA exist,
    # link /etc/pf9/hamgr/certs/ca/ca.cert.pem , ca/ca.key.pem to
    # /etc/pf9/certs/hamgr/cert.pem, key.pem
    folder_certs = os.path.join(hamgr_config_base, 'certs')
    envs = dict(RANDFILE='%s/.rnd' % folder_certs)
    key = os.path.join(global_config_base, 'certs', 'hamgr', 'key.pem')
    cert = os.path.join(global_config_base, 'certs', 'hamgr', 'cert.pem')

    if os.path.exists(key) and os.path.exists(cert):
        LOG.debug('symbolic link deployment created hamgr CA key and cert')
        ca_key = os.path.join(folder_certs, 'ca', 'ca.key.pem')
        ca_cert = os.path.join(folder_certs, 'ca', 'ca.cert.pem')
        cmd = 'ln -sf %s %s' % (key, ca_key)
        ret1 = execute_shell_command(cmd,
                                     in_directory=folder_certs,
                                     env_variables=envs)
        cmd = 'ln -sf %s %s' % (cert, ca_cert)
        ret2 = execute_shell_command(cmd,
                                     in_directory=folder_certs,
                                     env_variables=envs)
        if ret1 != 0 or ret2 != 0:
            LOG.debug('failed to create soft link to %s or %s', key, cert)
            return False
        return True
    LOG.debug('create symbolic link failed as deployment created CA not exist')
    return False


def setup_folders_if_not_exist():
    folder_certs = os.path.join(hamgr_config_base, 'certs')
    if not os.path.exists(folder_certs):
        execute_shell_command('mkdir -p %s' % folder_certs)

    folders = ['ca', 'crl', 'newcerts', 'private']
    for folder in folders:
        folder_child = os.path.join(folder_certs, folder)
        if not os.path.exists(folder_child):
            execute_shell_command('mkdir -p %s' % folder_child)


def setup_openssl_if_not_exist():
    setup_folders_if_not_exist()
    folder_certs = os.path.join(hamgr_config_base, 'certs')
    file_index = os.path.join(folder_certs, 'index.txt')
    if os.path.exists(file_index):
        execute_shell_command('rm -f %s' % file_index)
    execute_shell_command('touch %s' % file_index)
    file_serial = os.path.join(folder_certs, 'serial')
    if os.path.exists(file_serial):
        execute_shell_command('rm -f %s' % file_serial)
    execute_shell_command('echo 1000 | tee %s' % file_serial)
    # openssl.cnf provides the template for openssl tool to customize
    # the way how certs are created, like additional policy, extensions
    ssl_cnf = os.path.join(folder_certs, 'openssl.cnf')
    ssl_config = """
    [ca]
    default_ca = customer_ca

    [customer_ca]
    # directory and file locations
    dir = {}
    certs = $dir/ca
    crl_dir = $dir/crl
    new_certs_dir = $dir/newcerts
    database = $dir/index.txt
    serial = $dir/serial
    RANDFILE= $dir/private/.rnd

    # root key and root cert
    private_key = $dir/ca/ca.key.pem
    certificate = $dir/ca/ca.cert.pem

    # cert revocation list
    crlnumber = $dir/crlnumber
    crl = $dir/crl/ca.crl.pem
    crl_extensions = crl_extensions
    default_crl_days = 30

    default_crl_days = 7
    default_days = 365
    default_md = sha1
    policy = customer_ca_policy
    x509_extensions = certificate_extensions

    [crl_extensions]
    authorityKeyIndentifier = keyid:always

    [customer_ca_policy]
    commonName = supplied
    stateOrProvinceName = optional
    countryName = optional
    emailAddress = optional
    organizationName = optional
    organizationalUnitName = optional

    [certificate_extensions]
    basicConstraints = CA:false

    [req]
    default_bits = 2048
    default_keyfile = ./ca/ca.key.pem
    default_md = sha1
    prompt = yes
    distinguished_name = root_ca_distinguished_name
    x509_extensions = root_ca_extensions

    [root_ca_distinguished_name]
    commonName = hostname

    [root_ca_extensions]
    basicConstraints = CA:true
    keyUsage = keyCertSign, cRLSign

    [client_ca_extensions]
    basicConstraints = CA:false
    keyUsage = digitalSignature
    extendedKeyUsage = 1.3.6.1.5.5.7.3.2

    [server_ca_extensions]
    basicConstraints = CA:false
    keyUsage = keyEncipherment
    extendedKeyUsage = 1.3.6.1.5.5.7.3.1
    """
    if not os.path.exists(ssl_cnf):
        with open(ssl_cnf, 'w') as cnffp:
            cnffp.write(ssl_config.format(folder_certs))


def are_hamgr_ca_key_cert_pairs_exist():
    exists = True
    names = ['key.pem', 'cert.pem']
    # look for deployment created hamgr CA at /etc/pf9/certs/hamgr
    for name in names:
        exists = exists & os.path.exists(os.path.join(global_config_base,
                                                      'certs',
                                                      'hamgr',
                                                      name))
    return exists


def are_consul_ca_key_cert_pair_exist():
    # assume the deployment system has already created hamgr CA cert and key
    # under /etc/pf9/certs/hamgr/{key.pem}, {cert.pem}
    # first check them, if they exist, then assume CA exist
    # otherwise check
    # /etc/pf9/hamgr/certs/ca/{ca.key.pem}, {ca.cert.pem}
    if are_hamgr_ca_key_cert_pairs_exist():
        LOG.debug('deployment created hamgr CA exists')
        symbolic_link_ca_key_cert_pairs_if_exist()
        return True

    # fallback to hamgr itself when deployment created hamgr CA not exist
    exists = True
    names = ['ca.key.pem', 'ca.cert.pem']
    for name in names:
        exists = exists & os.path.exists(os.path.join(hamgr_config_base,
                                                      'certs',
                                                      'ca',
                                                      name))
    LOG.debug('hamgr created CA exists ? %s', str(exists))
    return exists


def is_cert_expired(cert_file, expire_threshold_days=1):
    if not os.path.exists(cert_file):
        return True

    content = None
    with open(cert_file) as fp:
        content = fp.read()

    if not content:
        return True

    is_expired = False
    try:
        cert = x509.load_pem_x509_certificate(content.encode(), default_backend())
        expire_at = cert.not_valid_after
        utc_now = datetime.datetime.utcnow()
        time_delta = expire_at - utc_now
        is_expired = True \
            if time_delta < datetime.timedelta(days=expire_threshold_days) \
            else False
        if is_expired:
            LOG.info('cert %s not valid after %s', str(cert_file), str(expire_at))
    except Exception as ex:
        LOG.error('unhandled exception when detect expiration of cert %s : %s',
                  cert_file, str(ex))
    return is_expired


def is_consul_ca_cert_expired():
    # first look at deployment created CA
    cert = os.path.join(global_config_base, 'certs', 'hamgr', 'cert.pem')
    if os.path.exists(cert):
        return is_cert_expired(cert)
    # otherwise fall back to hamgr created CA
    ca_cert = os.path.join(hamgr_config_base, 'certs', 'ca', 'ca.cert.pem')
    is_expired = is_cert_expired(ca_cert)
    return is_expired


def is_consul_svc_cert_expired(cluster_name):
    ca_cert = os.path.join(hamgr_config_base,
                           'certs', 'svc_%s' % cluster_name,
                           'svc.cert.pem')
    is_expired = is_cert_expired(ca_cert)
    return is_expired


def are_consul_svc_key_cert_pair_exist(cluster_name):
    exists = True
    names = ['svc.key.pem', 'svc.cert.pem']
    for name in names:
        exists = exists & os.path.exists(os.path.join(hamgr_config_base,
                                                      'certs',
                                                      'svc_%s' % cluster_name,
                                                      name))
    return exists


def read_key_cert_pair(key_file, cert_file, base64_encode=True):
    content_key = ''
    content_cert = ''
    LOG.debug('key file to read %s', key_file)
    if os.path.exists(key_file):
        with open(key_file) as fp:
            content_key = fp.read()
    else:
        LOG.warning('key file %s requested does not exist', key_file)
    LOG.debug('cert file to read %s', cert_file)
    if os.path.exists(cert_file):
        with open(cert_file) as fp:
            content_cert = fp.read()
    else:
        LOG.warning('cert file %s requested does not exist', cert_file)
    if base64_encode:
        content_key = base64.b64encode(content_key.encode())
        content_cert = base64.b64encode(content_cert.encode())
    return content_key.decode(), content_cert.decode()


def read_consul_ca_key_cert_pair(base64_encode=True):
    # if deployment created key and cert exist, use that
    key = os.path.join(global_config_base, 'certs', 'hamgr', 'key.pem')
    cert = os.path.join(global_config_base, 'certs', 'hamgr', 'cert.pem')
    if os.path.exists(key) and os.path.exists(cert):
        return read_key_cert_pair(key, cert, base64_encode=base64_encode)

    # fall back to hamgr itself created CA if deployment created not there
    key = os.path.join(hamgr_config_base, 'certs', 'ca', 'ca.key.pem')
    cert = os.path.join(hamgr_config_base, 'certs', 'ca', 'ca.cert.pem')
    return read_key_cert_pair(key, cert, base64_encode=base64_encode)


def read_consul_svc_key_cert_pair(cluster_name, base64_encode=True):
    key = os.path.join(hamgr_config_base,
                       'certs',
                       'svc_%s' % cluster_name,
                       'svc.key.pem')
    cert = os.path.join(hamgr_config_base,
                        'certs',
                        'svc_%s' % cluster_name,
                        'svc.cert.pem')
    return read_key_cert_pair(key, cert, base64_encode=base64_encode)


def create_consul_ca_key_cert_pairs():
    if are_hamgr_ca_key_cert_pairs_exist():
        symbolic_link_ca_key_cert_pairs_if_exist()

    # fallback to hamgr to create CA
    setup_openssl_if_not_exist()
    cfg_general = get_general_configs()
    folder_certs = os.path.join(hamgr_config_base, 'certs')
    envs = dict(RANDFILE='%s/.rnd' % folder_certs)
    cmd = 'openssl req -x509 -newkey rsa:2048 -days 9999 -config openssl.cnf ' \
          '-subj /CN=%s/ -outform PEM -out ca/ca.cert.pem -nodes ' \
          % (cfg_general['customer_shortname'])
    ret = execute_shell_command(cmd,
                                in_directory=folder_certs,
                                env_variables=envs)
    if ret == 0:
        LOG.debug('consul encryption CA key and cert are created')
        return True
    LOG.warning('consul encryption CA key and cert are not created')
    return False


def create_consul_svc_key_cert_pairs(cluster_name=""):
    if cluster_name is None:
        cluster_name = ""

    setup_openssl_if_not_exist()

    folder_certs = os.path.join(hamgr_config_base, 'certs')

    if not are_consul_ca_key_cert_pair_exist() or is_consul_ca_cert_expired():
        LOG.warning('can not create svc key or cert for cluster %s, ' \
                 'because CA key or cert creation failed', cluster_name)
        return False

    cfg_general = get_general_configs()
    if not os.path.exists(folder_certs):
        execute_shell_command('mkdir -p %s' % folder_certs)

    folders = ['svc_%s' % cluster_name]
    for folder in folders:
        folder_child = os.path.join(folder_certs, folder)
        if not os.path.exists(folder_child):
            execute_shell_command('mkdir -p %s' % folder_child)

    clean_openssl_index_db()
    cmd = 'openssl genrsa -out svc_%s/svc.key.pem 2048 ' % (cluster_name)
    envs = dict(RANDFILE='%s/.rnd' % folder_certs)
    ret = execute_shell_command(cmd,
                                in_directory=folder_certs,
                                env_variables=envs)
    if ret != 0:
        LOG.warning('consul encryption service key is not created')
        return False
    else:
        LOG.debug('consul encryption service key is created')

    clean_openssl_index_db()
    cmd = 'openssl req -new -key svc_%s/svc.key.pem ' \
          '-out svc_%s/svc.req.pem -outform PEM ' \
          '-subj /CN=%s/O=services -nodes ' \
          % (cluster_name, cluster_name, cfg_general['customer_shortname'])
    ret = execute_shell_command(cmd,
                                in_directory=folder_certs,
                                env_variables=envs)
    if ret != 0:
        LOG.warning('consul encryption service cert is not created')
        return False
    else:
        LOG.debug('consul encryption service cert is created')

    clean_openssl_index_db()
    cmd = 'openssl ca -config openssl.cnf ' \
          '-in svc_%s/svc.req.pem -out svc_%s/svc.cert.pem ' \
          '-notext -batch -extensions server_ca_extensions -days 9999 ' \
          % (cluster_name, cluster_name)
    ret = execute_shell_command(cmd,
                                in_directory=folder_certs,
                                env_variables=envs)
    if ret != 0:
        LOG.warning('consul encryption service cert signing failed')
        return False
    else:
        LOG.debug('consul encryption service cert is signed with CA')
    return True
