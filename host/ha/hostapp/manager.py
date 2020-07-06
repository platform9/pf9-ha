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
import os
import io
import logging
from ConfigParser import ConfigParser
from datetime import datetime
from datetime import timedelta
from subprocess import call
from time import sleep
from base64 import b64decode
from shutil import rmtree
from ha.utils import consul_helper
from ha.utils import report
from oslo_config import cfg
from shared import constants
from shared.rpc.rpc_manager import RpcManager
from shared.messages.rebalance_response import ConsulRoleRebalanceResponse
from shared.messages.consul_response import ConsulRefreshResponse
from shared.messages import message_types
from shared.exceptions import ha_exceptions
from shared.constants import LOGGER_PREFIX

LOG = logging.getLogger(LOGGER_PREFIX + __name__)


def _show_conf(conf):
    LOG.debug('-----------------------------------')
    output = ""
    output = output + '\r\n' + 'CONF : dirs - %s' % str(conf.config_dirs)
    output = output + '\r\n' + 'CONF : sections - %s' % str(conf.list_all_sections())
    output = output + '\r\n'
    output = output + '\r\n' + '[DEFAULT]' + '\r\n'
    for key, val in conf.iteritems():
        if isinstance(val, cfg.ConfigOpts.GroupAttr):
            output = output + '\r\n'
            output = output + '\r\n' + '[' + str(key) + ']' + '\r\n'
            for g_key, g_val in val.iteritems():
                output  = output + '\r\n' + str(g_key) + '=' + str(g_val)
        else:
            if key in ['config_dir', 'config_file']:
                continue
            output = output + '\r\n' + str(key) + '=' + str(val)
    LOG.debug(output)
    LOG.debug('-----------------------------------')


CONF = cfg.CONF

# make it a method so test can use it
def setup_conf_options():
    consul_grp = cfg.OptGroup('consul', title='Opt group for consul')
    consul_opts = [
        cfg.IntOpt('status_check_interval', default=10,
                   help='Time interval in seconds between status checks'),
        cfg.StrOpt('join',
                   help='Comma separated list of IP addresses of the '
                        'servers to connect to'),
        cfg.IntOpt('bootstrap_expect', default=3,
                   help='Whether to start consul as server or agent. Valid '
                        'values are 0, 1, 3 and 5. 0 indicates that consul is '
                        'started in agent mode while 1, 3 and 5 indicate that '
                        'consul is started in server mode with bootstrap_expect '
                        'being that specified value.'),
        cfg.StrOpt('cluster_name', default='', help='the name of consul cluster'),
        cfg.StrOpt('encrypt', default='', help='the encrypt key for encryption of Consul network traffic'),
        cfg.StrOpt('verify_incoming', default='false', help='whether to verify consul incoming traffice'),
        cfg.StrOpt('verify_outgoing', default='false', help='whether to verify consul outgoing traffice'),
        cfg.StrOpt('verify_server_hostname', default='false', help='whether to verify consul server name'),
        cfg.StrOpt('ca_file_content', default='', help='base64 encoded consul CA cert content'),
        cfg.StrOpt('cert_file_content', default='', help='base64 encoded consul server cert content'),
        cfg.StrOpt('key_file_content', default='', help='base64 encoded consul server key content'),
        cfg.StrOpt('consul_log_level', default='info', help='log level of consul agent'),
        cfg.StrOpt('cluster_details', default='', help='the hosts and their ips in expected consul cluster')
    ]

    default_opts = [
        cfg.StrOpt('host', default='',
                   help='Platform9 Host ID')
    ]

    CONF.register_group(consul_grp)
    CONF.register_opts(consul_opts, consul_grp)

    role_balance_grp = cfg.OptGroup(name='consul_role_rebalance', title='group of options for consul_role_rebalance')
    role_balance_opts = [
        cfg.StrOpt('role_rebalance_enabled', default='True', help='whether the auto rebalance is enabled'),
        cfg.StrOpt('amqp_host', default='localhost', help='the RPC host fqdn or ip'),
        cfg.StrOpt('amqp_port', default='5672', help='the RPC host port'),
        cfg.StrOpt('amqp_user', default='', help='the user name for accessing RPC host'),
        cfg.StrOpt('amqp_password', default='', help='the password for accessing RPC host'),
        cfg.StrOpt('amqp_virtualhost', default='/', help='the RPC virtual host path'),
        cfg.StrOpt('amqp_exchange_name', default='consul-role-rebalance-exchange', help='the RPC exchange name'),
        cfg.StrOpt('amqp_exchange_type', default='topic', help='the RPC exchange type'),
        cfg.StrOpt('amqp_routingkey_sending', default='receiving', help='the RPC message routing key for sending to du'),
        cfg.StrOpt('amqp_routingkey_receiving', default='sending', help='the RPC message routing key for receiving from du')
    ]

    CONF.register_group(role_balance_grp)
    CONF.register_opts(role_balance_opts, role_balance_grp)

    CONF.register_opts(default_opts)

# call setup
setup_conf_options()

PF9_CONSUL_DATA_DIR='/opt/pf9/consul-data-dir'
PF9_CONSUL_CONF_DIR = '/opt/pf9/etc/pf9-consul/'
STOPPING = False

# global variables
global_hostid = None
global_join_ips = None
global_rpc_mgr = None
global_consul_mgr = None
global_skip_config_refresh = False

def add_consul_secure_settings(conf):
    conf['encrypt'] = CONF.consul.encrypt
    conf['verify_incoming'] = True if str(CONF.consul.verify_incoming).lower() == 'true' else False
    conf['verify_outgoing'] = True if str(CONF.consul.verify_outgoing).lower() == 'true' else False
    conf['verify_server_hostname'] = True if str(CONF.consul.verify_server_hostname).lower() == 'true' else False
    if CONF.consul.ca_file_content:
        content = b64decode(CONF.consul.ca_file_content)
        file = os.path.join(PF9_CONSUL_CONF_DIR, 'consul_ca.pem')
        with open(file, 'w') as cafp:
            cafp.write(content)
        conf['ca_file'] = file
    if CONF.consul.cert_file_content:
        content = b64decode(CONF.consul.cert_file_content)
        file = os.path.join(PF9_CONSUL_CONF_DIR, 'consul_cert.pem')
        with open(file, 'w') as certfp:
            certfp.write(content)
        conf['cert_file'] = file
    if CONF.consul.key_file_content:
        content = b64decode(CONF.consul.key_file_content)
        file = os.path.join(PF9_CONSUL_CONF_DIR, 'consul_key.pem')
        with open(file, 'w') as keyfp:
            keyfp.write(content)
        conf['key_file'] = file
    LOG.debug('consul secure settings : %s', str(conf))
    return conf


def generate_consul_conf(server_mode, ignore_expect=False):
    cfg_file = None
    try:
        LOG.debug('try to generate consul config : server_mode - %s, '
                  'ignore_expect - %s,  bootstrap_expect - %s',
                  str(server_mode),
                  str(ignore_expect),
                  str(CONF.consul.bootstrap_expect))
        if not ignore_expect:
            server_mode = (CONF.consul.bootstrap_expect > 0)

        retry_join = CONF.consul.join.split(',')
        ip_address = consul_helper.get_ip_address()
        bind_address = consul_helper.get_bind_address()
        log_levels=["trace", "debug", "info", "warn", "err"]

        if not server_mode:
            LOG.debug('generate consul config as slave')
            # Start consul with agent conf
            with open(PF9_CONSUL_CONF_DIR + 'client.json.template', 'r') as fptr:
                agent_conf = json.load(fptr)
            agent_conf['advertise_addr'] = ip_address
            agent_conf['bind_addr'] = bind_address
            agent_conf['disable_remote_exec'] = True
            if CONF.consul.consul_log_level in log_levels:
                agent_conf['log_level'] = CONF.consul.consul_log_level
            agent_conf['datacenter'] = CONF.consul.cluster_name
            agent_conf['node_name'] = CONF.host
            agent_conf['retry_join'] = retry_join

            # add secure settings
            agent_conf = add_consul_secure_settings(agent_conf)
            cfg_file = PF9_CONSUL_CONF_DIR + 'conf.d/client.json'
            LOG.debug('create consul slave configure file %s with data : %s',
                      cfg_file, str(agent_conf))
            with open(cfg_file, 'w') as fptr:
                json.dump(agent_conf, fptr)
            svr_file = PF9_CONSUL_CONF_DIR + 'conf.d/server.json'
            if os.path.exists(svr_file):
                LOG.debug('remove server role file %s , which should not exist', svr_file)
                os.remove(svr_file)
        else:
            LOG.debug('generate consul config as server')
            bootstrap_expect = CONF.consul.bootstrap_expect
            if bootstrap_expect <= 0:
                bootstrap_expect = 3
            # Start consul with server conf
            with open(PF9_CONSUL_CONF_DIR + 'server.json.template', 'r') as fptr:
                server_conf = json.load(fptr)
            server_conf['advertise_addr'] = ip_address
            server_conf['bind_addr'] = bind_address
            server_conf['bootstrap_expect'] = bootstrap_expect
            server_conf['disable_remote_exec'] = True
            if CONF.consul.consul_log_level in log_levels:
                server_conf['log_level'] = CONF.consul.consul_log_level
            server_conf['datacenter'] = CONF.consul.cluster_name
            server_conf['node_name'] = CONF.host
            server_conf['retry_join'] = retry_join

            # add secure settings
            server_conf = add_consul_secure_settings(server_conf)
            cfg_file = PF9_CONSUL_CONF_DIR + 'conf.d/server.json'
            LOG.debug('create consul server configure file %s with data : %s',
                      cfg_file, str(server_conf))
            with open(cfg_file, 'w') as fptr:
                json.dump(server_conf, fptr)
            clt_file = PF9_CONSUL_CONF_DIR + 'conf.d/client.json'
            if os.path.exists(clt_file):
                LOG.debug('remove client role file %s , which should not exist', clt_file)
                os.remove(clt_file)
        LOG.debug('consul config file %s is now generated', cfg_file)
        return True
    except Exception:
        LOG.exception('unhandled exception when generating consul config')
    LOG.error('consul config file %s failed to be generated', cfg_file)
    return False


def run_cmd(cmd):
    LOG.debug('run command in shell : %s', str(cmd))
    retcode = call(cmd, shell=True)
    if retcode != 0:
        LOG.warning("command '{cmd}' returned non-zero code".format(cmd=cmd))
    LOG.debug('return code : %s , command : %s', str(retcode), str(cmd))
    return retcode


def start_consul_service():
    retval = False
    service_start_retry = 30
    try:
        while service_start_retry > 0:
            retcode = run_cmd('sudo service pf9-consul status')
            LOG.debug('retcode of command "sudo service pf9-consul status" : %s', str(retcode))
            if retcode == 0:
                LOG.warning('Consul service was already running. now stop it before start')
                retcode = run_cmd('sudo service pf9-consul stop')
                LOG.debug('retcode of command "sudo service pf9-consul stop" : %s', str(retcode))
                sleep(5)

            # force to kill the consul process if it exists
            # the 'sudo service pf9-consul stop' sometimes did not stop the consul
            pids = []
            try:
                pidtxt = os.popen("ps -ef | grep consul | grep -v grep | awk \'{ print $2 }\'").read();
                LOG.debug('output of finding consul process id : %s', str(pidtxt))
                if pidtxt:
                    ids = [x for x in pidtxt.split('\n') if x]
                    pids = [int(x) for x in ids]
            except Exception as e:
                LOG.warning('no pid found for consul process. error : %s', str(e))

            LOG.debug('pids of consul process : %s', str(pids))
            if pids:
                for pid in pids:
                    LOG.debug('kill consul process %s', str(pid))
                    retcode = run_cmd('kill -9 %s' % str(pid))
                    LOG.debug('consul process %s is killed ? %s', str(pid), str(retcode == 0))

            # the 'raft' and 'serf' folder have consul cached data, which
            # made consul still remember old datacenter info, better to remove
            # them before restart the consul service
            clean_consul_cache()
            # when failed to start consul, check whether needs to repairs node-id or keyring
            # those are the two files messed up by consul itself
            repair_consul_wiped_files_if_needed()

            LOG.debug('start pf9-consul service using restart command')
            retcode = run_cmd('sudo service pf9-consul restart')
            LOG.debug('retcode of command "sudo service pf9-consul restart" : %s', str(retcode))

            # Sleep 3s to allow consul to fail in case the bootstrap server
            # has not yet started. This also allows us 90s before the cluster
            # creation will fail
            sleep(3)
            if retcode == 0:
                retcode = run_cmd('sudo service pf9-consul status')
                if retcode == 0:
                    LOG.info('Consul service started')
                    retval = True
                    break
                else:
                    LOG.warning('Consul service stopped. Retrying...')
            else:
                LOG.warning('Consul service could not be started')

            service_start_retry = service_start_retry - 1
            sleep(3)
    except Exception:
        LOG.exception('unhandled exception when starting consul service')
    return retval


def clean_consul_cache():
    folders = ['raft', 'serf']
    folder_path = ''
    try:
        for folder in folders:
            folder_path = os.path.join(PF9_CONSUL_DATA_DIR, folder)
            if os.path.exists(folder_path):
                LOG.debug('remove consul cache folder %s', folder_path)
                rmtree(folder_path, ignore_errors=True)
    except:
        LOG.exception('unhandled exception when removing folder %s', folder_path)


def _run_command_with_retry(cmd, retries=3, interval=0.500):
    retry = 0
    succeeded = False
    while not succeeded:
        if retry > retries:
            LOG.warning("command failed after %s retries : %s", str(retries),
                        str(cmd))
            break
        try:
            LOG.debug('run command : %s ', str(cmd))
            result = run_cmd(cmd)
            if result == 0:
                succeeded = True
                LOG.debug('command is succeeded : %s', str(cmd))
                break;
        except Exception as e:
            LOG.debug("command '%s' failed with exception : %s", str(cmd), str(e))
        retry = retry + 1
        LOG.warning("retry command %s time : %s", str(retry), str(cmd))
        sleep(interval)

    return succeeded


def _replay_consul_role_rebalance_request(rebalance_mgr,
                                          cluster,
                                          request_id,
                                          host_id,
                                          reply_status,
                                          message,
                                          request_file,
                                          **kwargs):
    resp = ConsulRoleRebalanceResponse(cluster=cluster,
                                       request_id=request_id,
                                       host_id=host_id,
                                       status=reply_status,
                                       message=message,
                                       **kwargs)
    LOG.info('sending consul role rebalance response : %s', str(resp))
    rebalance_mgr.send_rpc_message(resp, message_type=message_types.MSG_ROLE_REBALANCE_RESPONSE)

    # remove cached request file if needed
    if request_file and os.path.exists(request_file):
        os.remove(request_file)


def _update_request_status(request_file, new_status):
    if not os.path.exists(request_file):
        return
    # open and update
    try:
        with open(request_file, 'r') as fp:
            obj = json.load(fp)

        if not obj:
            return

        with open(request_file, 'w') as fp:
            obj['status'] = new_status
            json.dump(obj, fp)
    except:
        LOG.exception('unhandled exception when update status for cached request')


def switch_to_new_consul_role(rebalance_mgr,
                              request,
                              cluster,
                              current_host_id,
                              join_ips,
                              request_file):
    LOG.info('start to process consul role rebalance request : %s',
              str(request))
    req_id = request['id']
    current_role = request['old_role']
    target_role = request['new_role']

    cached_status = 'created'
    cached_request = None
    if os.path.exists(request_file):
        with open(request_file, 'r') as fp:
            obj = json.load(fp)
            cached_status = obj['status']
            cached_request = obj

    no_need_to_change = False
    host_role = consul_helper.get_consul_role_for_host(current_host_id)
    if cached_status == 'created':
        _update_request_status(request_file, 'running')
        no_need_to_change = False
    elif cached_status == 'running':
        # if status is in running, need to see whether the agent was already
        # in the desired role (for example, the request was almost done , but
        # hostagent forced restarted pf9-ha-slave before response was sent)
        # when agent is already in desired status, just replay, otherwise
        # take it as new request
        if host_role == 'consul' and target_role == constants.CONSUL_ROLE_SERVER:
            no_need_to_change = True
        elif host_role == 'node' and target_role == constants.CONSUL_ROLE_CLIENT:
            no_need_to_change = True

    elif cached_status == 'done':
        LOG.info("rebalance: cached request was done, reply anyway")
        _replay_consul_role_rebalance_request(rebalance_mgr,
                                              cluster,
                                              req_id,
                                              current_host_id,
                                              constants.RPC_TASK_STATE_FINISHED,
                                              'status in cached request was done',
                                              request_file)
        return True
    else:
        msg = 'unknown status in cached request %s : %s' % (cached_status, str(cached_request))
        LOG.info("rebalance: removing request_file %s, as %s", request_file, msg)
        os.remove(request_file)
        _replay_consul_role_rebalance_request(rebalance_mgr,
                                              cluster,
                                              req_id,
                                              current_host_id,
                                              constants.RPC_TASK_STATE_ABORTED,
                                              msg,
                                              request_file)
        return False

    if no_need_to_change:
        LOG.debug('rebalance: host %s in consul cluster is already in expected role %s',
                  current_host_id, target_role)
        _replay_consul_role_rebalance_request(rebalance_mgr,
                                              cluster,
                                              req_id,
                                              current_host_id,
                                              constants.RPC_TASK_STATE_FINISHED,
                                              'already in expected role %s' % target_role,
                                              request_file)
        return True

    LOG.info('rebalance: consul role needs to be switched to: %s, its role in cluster : %s',
             target_role, str(host_role))

    server_mode = False
    if target_role == constants.CONSUL_ROLE_SERVER:
        server_mode = True
    elif target_role == constants.CONSUL_ROLE_CLIENT:
        server_mode = False
    else:
        msg = 'rebalance: unknown consul role %s' % target_role
        LOG.warning(msg)
        _replay_consul_role_rebalance_request(rebalance_mgr,
                                              cluster,
                                              req_id,
                                              current_host_id,
                                              constants.RPC_TASK_STATE_ABORTED,
                                              msg,
                                              request_file)
        return False

    # create the consul config file
    LOG.debug('creating consul config file for switching to %s role ...', target_role)
    result = generate_consul_conf(server_mode, ignore_expect=True)
    if not result:
        error = 'failed to create consul config file'
        LOG.error('rebalance request failed. request: %s, error: %s', req_id, error)
        _replay_consul_role_rebalance_request(rebalance_mgr,
                                              cluster,
                                              req_id,
                                              current_host_id,
                                              constants.RPC_TASK_STATE_ABORTED,
                                              error,
                                              request_file=request_file
                                              )
        return False

    # now consul config is updated, need to restart pf9-consul and re-join
    LOG.info('restarting consul service after updating consul role')
    result = start_consul_service()
    if not result:
        error = 'failed to restart pf9-consul service'
        LOG.error('rebalance request failed. request: %s, error: %s', req_id, error)
        _replay_consul_role_rebalance_request(rebalance_mgr,
                                              cluster,
                                              req_id,
                                              current_host_id,
                                              constants.RPC_TASK_STATE_ABORTED,
                                              error,
                                              request_file=request_file
                                              )
        return False

    LOG.debug('consul service restarted successfully')

    # re-join the new cluster
    cmd = 'consul join {ip}'.format(ip=join_ips)
    LOG.info('rejoining in consul cluster ...')
    result = _run_command_with_retry(cmd)
    if not result:
        error = 'failed to re-join into consul cluster %s' % join_ips
        LOG.error('rebalance request failed. request: %s, error: %s', req_id, error)
        _replay_consul_role_rebalance_request(rebalance_mgr,
                                              cluster,
                                              req_id,
                                              current_host_id,
                                              constants.RPC_TASK_STATE_ABORTED,
                                              error,
                                              request_file
                                              )
        return False

    LOG.debug('re-join to cluster successfully')

    # finally send response with finished status
    msg = 'Successfully switched consul role from %s to %s' % (current_role, target_role)
    LOG.info('%s',msg)
    _update_request_status(request_file, 'done')
    # when rebalance succeeded, also return the current consul status
    # in the response through kwargs key 'consul_status'
    # wait for 5 seconds for the consul to become steady after re-join
    # the leader may not immediately available
    sleep(5)
    consul_status_report = global_consul_mgr.get_consul_status_report()
    _replay_consul_role_rebalance_request(rebalance_mgr,
                                          cluster,
                                          req_id,
                                          current_host_id,
                                          constants.RPC_TASK_STATE_FINISHED,
                                          msg,
                                          request_file,
                                          **dict(consul_status=consul_status_report)
                                          )
    LOG.info('consul role of host switched to %s', target_role)
    return True


def handle_consul_refresh_request(rebalance_mgr, hostid, cluster, request):
    if not request:
        LOG.debug('ignore empty consul status refresh request.')
        return
    try:
        key_prefix = 'request-'
        # all hosts will receive this broadcast request, so there are two scenarios:
        # - receiver is consul leader:
        #     when it is alive : it can reply immediately
        #     when leader election happening : no one reply
        # - receiver is not consul leader :
        #     won't reply
        # so better to save the request in consul, with flag for if it is reported.
        msg_type = request['type']
        req_id = request['id']

        if msg_type != message_types.MSG_CONSUL_REFRESH_REQUEST:
            LOG.debug('not a consul refresh request : %s', str(request))
            resp = ConsulRefreshResponse(cluster=cluster,
                                         request_id=req_id,
                                         status=constants.RPC_TASK_STATE_ABORTED,
                                         report='',
                                         message='not a consul refresh request')
            rebalance_mgr.send_rpc_message(resp, message_type=message_types.MSG_CONSUL_REFRESH_RESPONSE)
            return

        # for valid request , store in kv first if not exist
        key = key_prefix + request['id']
        _, existing = global_consul_mgr.kv_fetch(key)
        if existing is None:
            data = json.dumps({'request': request,
                               'processed': False,
                               'timestamp': datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S'),
                               'createdBy': hostid
                               })
            global_consul_mgr.kv_update(key, data)
            LOG.info('consul refresh request %s is stored in kv store : %s', req_id, data)
        else:
            LOG.debug('consul refresh request %s already exist in kv store : %s', req_id, str(existing))

        # only leader can act on the request
        is_leader = global_consul_mgr.am_i_cluster_leader()
        if not is_leader:
            LOG.debug('i am not leader, so no response for consul refresh request. leader %s', str(global_consul_mgr.cluster_leader()))
            return

        # scan kv to see whether there are valid consul refresh requests
        valid_requests = []
        _, kv_list = global_consul_mgr.kv_fetch('', recurse=True)
        if kv_list is None:
            kv_list = []
        for kv in kv_list:
            key = str(kv['Key'])
            if key.startswith(key_prefix):
                value = json.loads(kv['Value'])
                if not value['processed']:
                    timestamp = datetime.strptime(value['timestamp'], '%Y-%m-%d %H:%M:%S')
                    if (datetime.utcnow() - timestamp) < timedelta(seconds=120):
                        valid_requests.append(value['request'])
                    else:
                        LOG.debug('delete stabled processed consul refresh request from kv store')
                        global_consul_mgr.kv_delete(key)
                else:
                    LOG.debug('delete already processed consul refresh request from kv store')
                    global_consul_mgr.kv_delete(key)

        if len(valid_requests) > 0:
            LOG.debug('found valid consul refresh requests : %s', str(valid_requests))

        for req in valid_requests:
            LOG.debug('i am leader, now response for consul refresh request : %s', str(req))
            req_id = req['id']
            req_type = req['type']
            if req_type == message_types.MSG_CONSUL_REFRESH_REQUEST:
                report = global_consul_mgr.get_consul_status_report()
                report['reportedBy'] = hostid
                resp = ConsulRefreshResponse(cluster=cluster,
                                             request_id=req_id,
                                             status=constants.RPC_TASK_STATE_FINISHED,
                                             report=json.dumps(report),
                                             message='')
                rebalance_mgr.send_rpc_message(resp, message_type=message_types.MSG_CONSUL_REFRESH_RESPONSE)
                LOG.debug('consul refresh response is sent at %s : %s', str(datetime.utcnow()), str(resp))
            else:
                LOG.warning('not a valid consul refresh request message: %s', str(req_type))
            key = key_prefix + req_id
            LOG.info('deleting consul refresh request %s from kv store', key)
            global_consul_mgr.kv_delete(key)
    except Exception as e:
        LOG.exception('unhandled exception when process consul refresh request : %s', str(e))

def on_consul_role_rebalance_request(role_rebalance_request):
    global global_rpc_mgr
    global global_hostid
    global global_join_ips
    global global_skip_config_refresh

    cluster = CONF.consul.cluster_name
    LOG.info('handle consul role rebalance request received : %s', str(role_rebalance_request))
    if not role_rebalance_request:
        LOG.warning('ignore empty consul role rebalance request')
        return
    msg_type = role_rebalance_request['type']
    if msg_type != message_types.MSG_ROLE_REBALANCE_REQUEST:
        LOG.warning('ignore non consul role rebalance request : %s', str(role_rebalance_request))
        return
    if str(role_rebalance_request['cluster']) != str(cluster):
        LOG.warning('ignore consul role rebalance request not for cluster %s but for %s: %s', str(cluster),
                 str(role_rebalance_request['cluster']), str(role_rebalance_request))
        return
    if role_rebalance_request['host_id'] != global_hostid:
        LOG.warning('ignore consul role rebalance request not for me %s but for host %s: %s', str(global_hostid),
                 role_rebalance_request['host_id'], str(role_rebalance_request))
        return

    # when hamgr requests consul role rebalance, it first updates the role
    # settings for host and send rpc to host. hostagent will sync the setting into host.
    # however, it will restart pf9-ha-slave due to the settings changes.
    # depends on how fast hostagent get the settings and restart service,
    # the rpc message may also be handled at that time. if hostagent restarts
    # service during rpc message processing, then rpc response will never
    # happen.  to avoid this , better to cache the rpc message to disk file
    # when receives the request. the file will be deleted if the rpc message
    # is processed, or retry failed, or the file is staled
    request_file = os.path.join(PF9_CONSUL_DATA_DIR, 'req-%s' % role_rebalance_request['id'])
    try:
        data = {'created': datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S'),
                'status': 'created',
                'request': role_rebalance_request
                }
        # just cache the request, don't handle it in asynchronously
        # the handle_cached_rebalance_requests will pick up latest one
        # to do the rebalance
        # here we turn asynchronous operation (event based) to
        # synchronous operation to avoid conflict . mainly due to hostagent
        # which update service config slower than here we process RPC request.
        with open(request_file, 'w') as fp:
            fp.write(json.dumps(data))
            LOG.info("cached rebalance request received '%s' to file '%s' for processing later",
                      str(role_rebalance_request), request_file)
    except:
        LOG.exception('unhandled exception in on_consul_role_rebalance_request')

def on_consul_status_request(status_request):
    global global_rpc_mgr
    global global_hostid
    global global_join_ips

    if not status_request:
        LOG.warning('ignore null or empty consul status request')
        return
    msg_type = status_request['type']
    cluster = CONF.consul.cluster_name
    if msg_type != message_types.MSG_CONSUL_REFRESH_REQUEST:
        LOG.warning('ignore non consul refresh request : %s', str(status_request))
        return
    if str(status_request['cluster']) != str(cluster):
        LOG.warning('ignore consul refresh request which is not for cluster %s but for %s: %s', str(cluster),
                 str(status_request['cluster']), str(status_request))
        return

    LOG.info('received consul refresh request at %s : %s', str(datetime.utcnow()), str(status_request))
    handle_consul_refresh_request(global_rpc_mgr, global_hostid, cluster=cluster, request=status_request)

def repair_consul_wiped_files_if_needed():
    try:
        # node-id should be created by consul itself, but observed that during power off then on scenario consul
        # might wipe out its node-id, which cause it fails to start. in this case , need to manually create a node
        # id for it, then restart consul will fix it
        nodeid_file = os.path.join(PF9_CONSUL_DATA_DIR, 'node-id')
        # set to the host id when consul wiped its id, and assume CONF.host alwasy has the value
        nodeid = CONF.host
        if not os.path.exists(nodeid_file):
            # the PF9_CONSUL_DATA_DIR must exist after pf9-ha-slave role
            # is enabled, we don't have permission to create the folder,
            # so assume it is there
            with open(nodeid_file, 'w') as fp:
                LOG.debug('detected there is no consul node-id file, now create with id %s', nodeid)
                fp.write(nodeid)
        else:
            LOG.debug('consul node-id file exist, now check its content')
            nodeid_old = None
            with open(nodeid_file, 'r') as fp:
                nodeid_old = fp.read()
            if not nodeid_old:
                with open(nodeid_file, 'w') as fp:
                    LOG.debug('detected consul node-id is empty, now write id %s', nodeid)
                    fp.write(nodeid)
            else:
                LOG.debug('skip node-id, as it is not empty : %s', str(nodeid_old))
        # when node-id was wiped out, the same time the keyring were also wiped not (not sure why). so need to
        # clean the serf folder, otherwise will get:
        # "Failed to configure keyring: unexpected end of JSON input"
        keyrings = ['local.keyring', 'remote.keyring']
        for keyring in keyrings:
            keyring_file = os.path.join(PF9_CONSUL_DATA_DIR, 'serf', keyring)
            if os.path.exists(keyring_file):
                content=''
                LOG.debug('%s file exist, now check its content ...', keyring_file)
                with open(keyring_file, 'r') as fp:
                    content=fp.read()
                if len(content) == 0:
                    LOG.debug('content of %s is empty, now delete it', keyring_file)
                    # use os.remove where the file must exist
                    os.remove(keyring_file)
                else:
                    LOG.debug('skip %s as the content is not empty', keyring_file)
            else:
                LOG.debug('skip %s as the file not exist', keyring_file)
    except Exception:
        LOG.exception('unhandled exception when repair consul wiped files')

def config_needs_refresh():
    global global_skip_config_refresh

    # when role rebalance triggered but hostagent has not reconciled settings
    # for service from resmgr, we need to wait hostagent to do its job.
    # if we are asked to ignore mismatch, we need to see
    # 1) if real consul settings match requested, means settings are reconciled
    # so continue to  refresh config (no conflict with rebalance request)
    # 2) if real consul settings not match requested, means settings has not
    # been reconciled yet, there is conflict with rebalance request
    # (which has higher priority), so refresh config does not make sense
    if global_skip_config_refresh:
        LOG.debug('global_skip_config_refresh is true')
        cfg_dir = os.path.join(PF9_CONSUL_CONF_DIR ,'conf.d')
        setting_files = [x for x in os.listdir(cfg_dir)]
        LOG.debug('files under %s : %s', str(cfg_dir), str(setting_files))
        if len(setting_files) != 1:
            # found more than one files, so need re-generate config file
            LOG.debug('more than one config %s', str(setting_files))
            return True
        file_name = setting_files[0]
        matched = True
        if file_name == "client.json":
            # if it is 'client.json' and bootstrap_expect is 0, means already
            # reconciled, so should not skip checking
            matched = (CONF.consul.bootstrap_expect  == 0)
        elif file_name == 'server.json':
            # if it is 'server.json' and bootstrap_expect is bigger than 0,
            # means already reconciled, so should not skip checking
            matched = (CONF.consul.bootstrap_expect > 0)
        else:
            # unknown consul config file, so still need refresh
            LOG.debug('unknown consul config file %s', file_name)
            return True

        if not matched:
            # mismatch means not reconciled, so wait for hostagent to
            # reconcile settings and restart service. refresh does not
            # make sense, by return False to stop refreshing
            LOG.debug('found mismatch , but will not refesh, file %s,  CONF.consul.bootstrap_expect %s',
                      str(file_name), str(CONF.consul.bootstrap_expect))
            return False

        # already matched, so it is safe to check whether needs refresh
        global_skip_config_refresh = False
    else:
        LOG.debug('global_skip_config_refresh is false')

    LOG.debug('checking config changes for consul')
    settings_source = dict(
        advertise_addr=consul_helper.get_ip_address(),
        bind_addr=consul_helper.get_bind_address(),
        bootstrap_expect=CONF.consul.bootstrap_expect,
        datacenter=CONF.consul.cluster_name,
        encrypt=CONF.consul.encrypt,
        ca_file_content=CONF.consul.ca_file_content,
        cert_file_content=CONF.consul.cert_file_content,
        key_file_content=CONF.consul.key_file_content,
        consul_log_level=CONF.consul.consul_log_level
    )

    LOG.debug('found settings for consul from pf9-ha : %s', str(settings_source))
    settings_consul = {}
    cfg_file = ""
    if CONF.consul.bootstrap_expect == 0:
        cfg_file = PF9_CONSUL_CONF_DIR + 'conf.d/client.json'
    else:
        cfg_file = PF9_CONSUL_CONF_DIR + 'conf.d/server.json'

    if not os.path.exists(cfg_file):
        LOG.debug('file %s not exist (bootstrap_expect : %s)', cfg_file, str(CONF.consul.bootstrap_expect))
        return True

    with open(cfg_file, 'r') as fptr:
        settings_consul = json.load(fptr)
    LOG.debug('found settings of consul used from %s : %s', cfg_file, str(settings_consul))

    # check whether settings in source do not exist or not match in consul settings
    if str(settings_source['advertise_addr']) != str(settings_consul.get('advertise_addr', None)):
        LOG.debug('detected changes in advertise_addr, source : %s , consul cfg : %s',
                 settings_source['advertise_addr'],
                 settings_consul.get('advertise_addr', None))
        return True
    if str(settings_source['bind_addr']) != str(settings_consul.get('bind_addr', None)):
        LOG.debug('detected changes in bind_addr, source : %s , consul cfg : %s',
                 settings_source['bind_addr'],
                 settings_consul.get('bind_addr', None))
        return True
    if str(settings_source['datacenter']) != str(settings_consul.get('datacenter', None)):
        LOG.debug('detected changes in datacenter, source : %s , consul cfg : %s',
                 settings_source['datacenter'],
                 settings_consul.get('datacenter', None))
        return True
    if str(settings_source['encrypt']) != str(settings_consul.get('encrypt', None)):
        LOG.debug('detected changes in encrypt, source : %s , consul cfg : %s',
                 settings_source['encrypt'],
                 settings_consul.get('encrypt', None))
        return True

    file_maps = [('consul_ca.pem', 'ca_file_content'),
                 ('consul_cert.pem', 'cert_file_content'),
                 ('consul_key.pem', 'key_file_content')
                 ]
    for item in file_maps:
        file_path = os.path.join(PF9_CONSUL_CONF_DIR, item[0])
        content_key = item[1]

        if settings_source[content_key] and os.path.exists(file_path) == False:
            LOG.debug('detected changes in file %s, source content: %s , consul cfg exists ?: %s',
                     file_path,
                     settings_source[content_key],
                     str(os.path.exists(file_path)))
            return True

        file_content = None
        with open(file_path, 'r') as fp:
            file_content = fp.read()

        if file_content != b64decode(settings_source[content_key]):
            LOG.info('detected changes in content in %s, source content: %s , consul cfg  content : %s',
                     file_path,
                     b64decode(settings_source[content_key]),
                     file_content)
            return True

    # consul log level
    if str(settings_source['consul_log_level']) != str(settings_consul.get('log_level', None)):
        LOG.debug('detected changes in consul_log_level, source : %s , consul cfg : %s',
                 settings_source['consul_log_level'],
                 settings_consul.get('log_level', None))

        return True
    LOG.debug('configuration for consul has not changed')
    return False

def get_join_ips():
    join_ips = ''
    if CONF.consul.join:
        ips = CONF.consul.join.split(',')
        join_ips = ' '.join([x.strip() for x in ips if x])
    return join_ips

def start_rpc_process():
    global global_rpc_mgr
    global global_hostid
    global global_join_ips
    try:
        # start dedicated rebalance request handling thread
        role_rebalance_enabled = bool(CONF.consul_role_rebalance.role_rebalance_enabled)
        LOG.debug('is consul role rebalance enabled ? %s', str(role_rebalance_enabled))
        if role_rebalance_enabled:
            amqp_host = CONF.consul_role_rebalance.amqp_host
            amqp_virtualhost = CONF.consul_role_rebalance.amqp_virtualhost
            amqp_port = CONF.consul_role_rebalance.amqp_port
            amqp_user = CONF.consul_role_rebalance.amqp_user
            amqp_passwd = CONF.consul_role_rebalance.amqp_password
            amqp_exchange = CONF.consul_role_rebalance.amqp_exchange_name
            amqp_exchange_type = CONF.consul_role_rebalance.amqp_exchange_type
            # queue name needs to be unique in order to get broadcast message from rabbitmq exchange in 'fanout' or 'direct'
            # exchange type.
            amqp_queue_for_receiving = 'queue-receiving-for-host-%s' % global_hostid
            amqp_routingkey_sending = CONF.consul_role_rebalance.amqp_routingkey_sending
            amqp_routingkey_receiving = CONF.consul_role_rebalance.amqp_routingkey_receiving
            parameters = 'host: %s, port: %s, user: %s, password: %s, exchange: %s, type: %s, queue: %s, send routing: %s, receiving routing: %s' % (
                amqp_host, amqp_port, amqp_user, amqp_passwd, amqp_exchange, amqp_exchange_type,
                amqp_queue_for_receiving, amqp_routingkey_sending, amqp_routingkey_receiving
            )
            msg = 'create consul role rebalance RPC manager with : %s' % parameters
            LOG.debug(msg)
            global_rpc_mgr = RpcManager(amqp_host,
                                        amqp_port,
                                        amqp_user,
                                        amqp_passwd,
                                        amqp_virtualhost,
                                        amqp_exchange,
                                        amqp_exchange_type,
                                        amqp_routingkey_sending,
                                        amqp_queue_for_receiving,
                                        amqp_routingkey_receiving,
                                        application='pf9-ha-slave'
                                        )
            LOG.info('consul role rebalance RPC manager is created')

            # to get better performance , rather than polling message from rabbitmq (which causes too much CPU usage)
            # redesign it to be event based by invoke callbacks once the driver received messages
            # so here register the callbacks to handle the received messages
            global_rpc_mgr.subscribe_message(message_types.MSG_ROLE_REBALANCE_REQUEST, on_consul_role_rebalance_request)
            global_rpc_mgr.subscribe_message(message_types.MSG_CONSUL_REFRESH_REQUEST, on_consul_status_request)
    except Exception:
        LOG.exception('unhandled exception when starting RPC manager')
    return global_rpc_mgr


def handle_cached_rebalance_requests():
    global global_skip_config_refresh

    if not os.path.exists(PF9_CONSUL_DATA_DIR):
        return

    # check files under /opt/pf9/consul-data-dir named 'req-{uud}'
    files = [f for f in os.listdir(PF9_CONSUL_DATA_DIR) if f.startswith('req-')]
    if len(files) < 1:
        LOG.debug('no cached requests, will check later')
        return
    LOG.info('handling cached rebalance requests under %s : %s ', PF9_CONSUL_DATA_DIR, str(files))
    # ideally there should be only one such file. but if there more , usually
    # the latest file is the last request, we should only handle the latest one
    # and remove any old ones
    objects = []
    for file in files:
        full_path = os.path.join(PF9_CONSUL_DATA_DIR, file)
        content = {}
        with open(full_path, 'r') as fp:
            content = json.load(fp)
        if not content:
            continue
        objects.append({'file': full_path,
                        'content':content,
                        'time': content['created']
                        })
    # sort them, latest on top
    ordered = sorted(objects, key=lambda obj: datetime.strptime(obj['time'],'%Y-%m-%d %H:%M:%S'), reverse=True)
    LOG.debug('all cached requests : %s', str(ordered))
    # remove old ones except the first (the latest)
    for obj in ordered[1:]:
        # now check time and status
        staled = False
        created = datetime.strptime(obj['time'],'%Y-%m-%d %H:%M:%S')
        if (datetime.utcnow() - created) > timedelta(minutes=5):
            LOG.debug('cached request %s is older than 5min now, will remove it',
                      str(obj['content']))
            staled = True
        done = False
        status = obj['content']['status']
        # as long as it was not done (can be 'created', 'running', 'done')
        # will retry it.  if in 'running' status , most likely the hostagent
        # restarted the pf9-ha-slave before it could send rebalance replay
        if status == 'done':
            LOG.info('cached request %s was done, will remove it',
                      str(obj['content']))
            done = True

        if staled or done:
            os.remove(obj['file'])

    # only process latest one
    latest = ordered[0]
    LOG.info('handle latest cached rebalance request : %s', json.dumps(latest))
    role_rebalance_request = latest['content']['request']
    cluster = CONF.consul.cluster_name
    request_file = latest['file']
    try:
        # if role is rebalanced, that means we have to ignore consul config
        # conflict ( mostly because hostagent has not reconciled the settings)
        # because rebalance PRC has higher priority.
        # if failed to rebalance role, then continue to refresh config
        global_skip_config_refresh = switch_to_new_consul_role(global_rpc_mgr,
                                                               role_rebalance_request,
                                                               cluster,
                                                               global_hostid,
                                                               global_join_ips,
                                                               request_file)
    except:
        LOG.exception('unhandled exception from switch_to_new_consul_role')
    LOG.info('cached rebalance request completed successfully ? %s', str(global_skip_config_refresh))


def loop():
    global STOPPING
    global global_hostid
    global global_join_ips
    global global_rpc_mgr
    global global_consul_mgr
    global global_skip_config_refresh

    LOG.debug('manager.loop has started ...')
    _show_conf(CONF)
    cfgparser = ConfigParser()
    cfgparser.read('/var/opt/pf9/hostagent/data.conf')
    global_hostid = cfgparser.get('DEFAULT', 'host_id')
    sleep_time = CONF.consul.status_check_interval

    # find out consul join ips
    global_join_ips = get_join_ips()
    LOG.debug('consul join addresses from config file :%s', global_join_ips)

    LOG.debug('expected consul cluster : %s', CONF.consul.cluster_details)
    cluster_details = json.loads(CONF.consul.cluster_details)
    LOG.debug('cluster details : %s', str(cluster_details))
    # the cluster_details should contains one entry for each ip in the ips_to_join
    ips_to_join = global_join_ips.strip(' ').split(' ')
    if not cluster_details:
        raise ha_exceptions.ConfigException('cluster_details is none')
    if len(cluster_details) != len(ips_to_join):
        raise ha_exceptions.ConfigException('num of hosts in join_ips does not match cluster_details')
    if not set(ips_to_join).issubset(set([x['addr'] for x in cluster_details])):
        raise ha_exceptions.ConfigException('hosts in join_ips do not match cluster_details')

    global_consul_mgr = consul_helper.consul_status(global_hostid, ips_to_join, cluster_details)
    reporter = report.HaManagerReporter()

    LOG.debug('create consul config file now')
    # TODO(pacharya): Handle restart of pf9-ha-slave service
    cluster_configured = generate_consul_conf(CONF.consul.bootstrap_expect > 0)
    LOG.debug('are consul configurations generated ? %s', str(cluster_configured))

    # Assume that consul was not running beforehand
    # TODO(pacharya): If consul was running beforehand we need to cleanup the
    #                 data dir of consul to get rid of the earlier state.
    LOG.debug('start consul service daemon')
    consul_started = start_consul_service()
    LOG.debug('is consul running ? %s', str(consul_started))

    if not cluster_configured or not consul_started:
        raise ha_exceptions.ConfigException('failed to generate consul configuration '
                                            'file or consul cluster failed to run')

    global_rpc_mgr = None
    global_skip_config_refresh = False

    try:
        # start dedicated rebalance request handling thread
        global_rpc_mgr = start_rpc_process()

        # the main thread handling host down events
        LOG.info('start main loop ...')
        while consul_started:
            try:
                # handle role rebalance RPC request has higher priority
                # over detecting consul config changes.
                # if hostagent restarted the service before role rebalance
                # could complete, then there will be a request cached.
                # so pick up from there before any consul configure changes
                if cluster_configured:
                    # check consul role rebalance cached requests
                    handle_cached_rebalance_requests()

                # if config from resmgr changed, the service should have
                # restarted. so check local against settings from resmgr
                # but if rpc based rebalance request finished faster than
                # hostagent changed the service's config, here it may overwrite
                # the settings from rpc rebalance request.
                if config_needs_refresh():
                    LOG.info('configuration changes detected for consul, '
                             'now re-configuring consul')
                    cluster_configured = False

                if not cluster_configured:
                    # refresh the config file and restart consul, in case there is no leader or join failed
                    # this happens when consul settings are updated through resmgr after consul had started
                    # so need to re-config the settings and re-start consul
                    generate_consul_conf(CONF.consul.bootstrap_expect > 0)
                    start_consul_service()

                    # Running join against oneself generates a warning message in
                    # logs but does not cause consul to crash
                    global_join_ips = get_join_ips()
                    if not global_join_ips:
                        LOG.error('null or empty consul join ip list in config file')
                        sleep(sleep_time)
                        continue
                    LOG.info('trying to join consul cluster members %s', global_join_ips)
                    retcode = run_cmd('consul join {ip}'.format(ip=global_join_ips))
                    leader = None
                    if retcode == 0:
                        sleep(sleep_time)
                        leader = global_consul_mgr.cluster_leader()
                        if leader:
                            cluster_configured = True
                            global_consul_mgr.log_kvstore()
                            LOG.info('joined consul cluster with leader %s', str(leader))
                    if not leader or retcode != 0:
                        cluster_configured = False
                        LOG.error('joining consul cluster failed, retcode %s, leader %s. Retrying',
                                 str(retcode), str(leader))

                # if still now configured, can not move on
                if not cluster_configured:
                    LOG.debug('consul is not configured, or not running')
                    sleep(sleep_time)
                    continue

                leader = global_consul_mgr.cluster_leader()
                if global_consul_mgr.am_i_cluster_leader():
                    global_consul_mgr.log_kvstore()
                    cluster_stat = global_consul_mgr.get_cluster_status()
                    if cluster_stat:
                        LOG.info('i am leader %s, found changes : %s', str(leader), str(cluster_stat))
                        LOG.debug('cluster_stat: %s', cluster_stat)
                        if reporter.report_status(cluster_stat):
                            LOG.debug('consul status is reported to hamgr: %s',
                                     cluster_stat)
                            global_consul_mgr.log_kvstore()
                            global_consul_mgr.update_reported_status(cluster_stat)
                        else:
                            LOG.error('reporting consul status to hamgr failed')
                    else:
                        LOG.debug('i am leader %s, but no changes to report for now', str(leader))
                    global_consul_mgr.log_kvstore()
                    global_consul_mgr.cleanup_consul_kv_store()
                else:
                    LOG.debug('i am not leader so do nothing, leader : %s', str(leader))

                # It is possible that host ID was not published when the consul
                # helper was created as the cluster was not yet formed. Since this
                # operation is idempotent calling it in a loop will not cause
                # multiple updates.
                LOG.debug('publish current host id %s', global_hostid)
                global_consul_mgr.publish_hostid()
                # dump kv store to file so we can check what happened
                global_consul_mgr.log_kvstore()

            except Exception as e:
                LOG.exception('unhandled exception in pf9-ha-slave loop : %s', str(e))

            LOG.debug('sleeping for %s seconds' % sleep_time)
            sleep(sleep_time)
    except Exception as e:
        LOG.exception('unhandled exception in pf9-ha-slave, exiting now : %s', str(e))
    del global_rpc_mgr
    LOG.error('pf9-ha-slave service exiting (start_loop=%s, consul_configured=%s)...',
              str(consul_started), str(cluster_configured))

