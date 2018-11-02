# Copyright 2016 Platform9 Systems Inc.
# All Rights Reserved

import json
import os
import io
import threading
from ConfigParser import ConfigParser
from datetime import datetime
from subprocess import call
from time import daylight
from time import sleep
from time import tzname

from ha.utils import consul_helper
from ha.utils import log as logging
from ha.utils import report
from oslo_config import cfg
from shared import constants
from shared.rebalance.manager import RebalanceManager
from shared.messages.rebalance_response import ConsulRoleRebalanceResponse

LOG = logging.getLogger('ha-manager')


def _show_conf(conf):
    LOG.info('CONF :')
    LOG.info('-----------------------------------')
    for key, val in conf.iteritems():
        if isinstance(val, cfg.ConfigOpts.GroupAttr):
            for g_key, g_val in val.iteritems():
                LOG.info("{:<20}{:<40}{:<15}".format(key, g_key, g_val))
        else:
            LOG.info("{:<20}{:<40}".format(key, val))
    LOG.info('-----------------------------------')


CONF = cfg.CONF

consul_grp = cfg.OptGroup('consul', title='Opt group for consul')
consul_opts = [
    cfg.IntOpt('status_check_interval', default=10,
               help='Time interval in seconds between status checks'),
    cfg.StrOpt('join',
               help='Comma separated list of IP addresses of the '
                    'servers to connect to'),
    cfg.IntOpt('bootstrap_expect', default=0,
               help='Whether to start consul as server or agent. Valid '
                    'values are 0, 1, 3 and 5. 0 indicates that consul is '
                    'started in agent mode while 1, 3 and 5 indicate that '
                    'consul is started in server mode with bootstrap_expect '
                    'being that specified value.'),
]

default_opts = [
    cfg.StrOpt('host', default='',
               help='Platform9 Host ID')
]

CONF.register_group(consul_grp)
CONF.register_opts(consul_opts, consul_grp)

role_balance_grp = cfg.OptGroup(name='consul_role_rebalance', title='group of options for consul_role_rebalance')
role_balance_opts = [
    cfg.BoolOpt('role_rebalance_enabled', default=True, help='whether the auto rebalance is enabled'),
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

PF9_CONSUL_CONF_DIR = '/opt/pf9/etc/pf9-consul/'
STOPPING = False
REBALANCE_IN_PROGRESS = False


def generate_consul_conf():
    client_json = os.path.join(PF9_CONSUL_CONF_DIR, 'conf.d/client.json')
    server_json = os.path.join(PF9_CONSUL_CONF_DIR, 'conf.d/server.json')
    client_json_exist = os.path.exists(client_json)
    server_json_exist = os.path.exists(server_json)
    if client_json_exist or server_json_exist:
        cfg_file = client_json if client_json_exist else server_json
        LOG.info('consul config file %s already exist', cfg_file)
        return

    ip_address = consul_helper.get_ip_address()
    bind_address = consul_helper.get_bind_address()
    if CONF.consul.bootstrap_expect == 0:
        # Start consul with agent conf
        with open(PF9_CONSUL_CONF_DIR + 'client.json.template') as fptr:
            agent_conf = json.load(fptr)
        agent_conf['advertise_addr'] = ip_address
        agent_conf['bind_addr'] = bind_address
        agent_conf['disable_remote_exec'] = True
        if CONF.host:
            agent_conf['node_name'] = CONF.host
        with open(PF9_CONSUL_CONF_DIR + 'conf.d/client.json', 'w') as fptr:
            json.dump(agent_conf, fptr)
    else:
        # Start consul with server conf
        with open(PF9_CONSUL_CONF_DIR + 'server.json.template') as fptr:
            server_conf = json.load(fptr)
        server_conf['advertise_addr'] = ip_address
        server_conf['bind_addr'] = bind_address
        server_conf['bootstrap_expect'] = CONF.consul.bootstrap_expect
        server_conf['disable_remote_exec'] = True
        if CONF.host:
            server_conf['node_name'] = CONF.host
        with open(PF9_CONSUL_CONF_DIR + 'conf.d/server.json', 'w') as fptr:
            json.dump(server_conf, fptr)


def run_cmd(cmd):
    retcode = call(cmd, shell=True)
    if retcode != 0:
        LOG.warn('{cmd} returned non-zero code'.format(cmd=cmd))
    return retcode


def start_consul_service():
    retval = False
    service_start_retry = 30

    retcode = run_cmd('sudo service pf9-consul status')
    if retcode == 0:
        LOG.warn('Consul service was already running. now stop it before start')
        run_cmd('sudo service pf9-consul stop')

    while service_start_retry > 0:
        retcode = run_cmd('sudo service pf9-consul start')
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
                LOG.warn('Consul service stopped. Retrying...')
        else:
            LOG.warn('Consul service could not be started')
        service_start_retry = service_start_retry - 1
    return retval


def switch_to_new_consul_role(rebalance_mgr, request, current_host_id, join_ips):
    LOG.debug('start to process switching consul role for request : %s', str(request))
    req_id = request['id']
    current_role = 'slave' if CONF.consul.bootstrap_expect == 0 else 'server'
    target_role = request['new_role']
    client_json = os.path.join(PF9_CONSUL_CONF_DIR, 'conf.d/client.json')
    server_json = os.path.join(PF9_CONSUL_CONF_DIR, 'conf.d/server.json')
    exist = False
    original_file = None
    target_file = None
    if target_role == constants.CONSUL_ROLE_SERVER:
        # change from client.json to server.sjon
        exist = os.path.exists(client_json)
        LOG.info('is original config %s exist ? %s ', client_json, str(exist))
        original_file = client_json
        target_file = server_json
    elif target_role == constants.CONSUL_ROLE_CLIENT:
        # change from server.json to client.json
        exist = os.path.exists(server_json)
        LOG.info('is original config %s exist ? %s ', server_json, str(exist))
        original_file = server_json
        target_file = client_json
    else:
        LOG.info('unknown target consul member role %s', target_role)

    if not exist:
        LOG.info('unable to switch consul role, current role is %s, but config file %s does not exist', current_role,
                 original_file)
        resp = ConsulRoleRebalanceResponse(req_id,
                                           current_host_id,
                                           status=constants.REBALANCE_STATE_ABORTED,
                                           message='file %s not exist' % target_file)
        LOG.debug('send rebalance response : %s', str(resp))
        rebalance_mgr.send_role_rebalance_response(resp)
        return False

    # change from original file to target file
    cmd = "mv -f '%s' '%s'" % (original_file, target_file)
    result = run_cmd(cmd)
    if result != 0:
        LOG.debug('unable to switch consul role, as failed to copy from file %s to %s', original_file, target_file)
        resp = ConsulRoleRebalanceResponse(req_id,
                                           current_host_id,
                                           status=constants.REBALANCE_STATE_ABORTED,
                                           message='failed to create file %s' % target_file)
        LOG.debug('send rebalance response : %s', str(resp))
        rebalance_mgr.send_role_rebalance_response(resp)
        return False

    LOG.info('pf9-consul config file is changed from %s to %s', original_file, target_file)
    cfg_obj = None
    cache_to_remove = []

    # update the target file by removeing or adding : 'bootstrap_expect' and 'server'
    with io.open(target_file, 'r') as rfp:
        content = rfp.read()
        LOG.info('content of target file %s : %s', target_file, content)
        cfg_obj = json.loads(content)
        if target_role == constants.CONSUL_ROLE_CLIENT:
            cfg_obj.pop('bootstrap_expect', None)
            cfg_obj.pop('server', None)
            # when originally it is server role, there will be folder 'raft' , need
            # to remove it before restart consul, otherwise the cached data will block consul
            raft = os.path.join(cfg_obj['data_dir'], 'raft')
            if os.path.exists(raft):
                cache_to_remove.append(raft)
        elif target_role == constants.CONSUL_ROLE_SERVER:
            cfg_obj['bootstrap_expect'] = 3
            cfg_obj['server'] = True
        else:
            LOG.warn('unknown expected consul role %s', target_role)
            cfg_obj = None

    if not cfg_obj:
        LOG.info('unable to switch consul role, as failed to modify consul config file %s', target_file)
        resp = ConsulRoleRebalanceResponse(req_id,
                                           current_host_id,
                                           status=constants.REBALANCE_STATE_ABORTED,
                                           message='failed to modify file %s' % target_file)
        LOG.debug('send rebalance response : %s', str(resp))
        rebalance_mgr.send_role_rebalance_response(resp)
        return False

    LOG.info('new config file %s : %s', target_file, str(cfg_obj))
    with io.open(target_file, 'wb') as wfp:
        content = json.dumps(cfg_obj)
        LOG.info('write content to target file %s : %s', target_file, content)
        wfp.write(content)
        LOG.info('consul config file for new role is created')

    if cache_to_remove:
        for cache in cache_to_remove:
            LOG.info('removing cached files %s', cache)
            cmd = 'rm -rf %s' % cache
            result = run_cmd(cmd)
            if result != 0:
                LOG.warn('failed to remove cache %s with cmd %s : %s', cache, cmd, str(result))
            else:
                LOG.info('consul cache path %s is now removed', cache)

        LOG.info('consul cached files are removed successfully')

    retry = 0
    error = ''
    succeeded = False
    while not succeeded:
        if retry > 3:
            resp = ConsulRoleRebalanceResponse(req_id,
                                               current_host_id,
                                               status=constants.REBALANCE_STATE_ABORTED,
                                               message=error)
            LOG.debug('failed after 3 retries, now send rebalance response : %s', str(resp))
            rebalance_mgr.send_role_rebalance_response(resp)
            return False

        has_error = False
        # ask for leave first to gracefully set internal state
        cmd = 'consul leave'
        result = run_cmd(cmd)
        if result != 0:
            has_error = True
            error = 'failed to leave the cluster'
            LOG.warn(error)
        else:
            LOG.info('left the cluster successfully, now try to restart consul service')

        # now consul config is updated, need to restart pf9-consul and re-join
        cmd = 'sudo service pf9-consul restart'
        result = run_cmd(cmd)
        if result != 0:
            has_error = True
            error = 'failed to restart pf9-consul service'
            LOG.info(error)
        else:
            LOG.info('consul service restarted successfully, now try to check status of consul')

        cmd = 'sudo service pf9-consul status'
        result = run_cmd(cmd)
        if result != 0:
            has_error = True
            error = 'pf9-consul service is not running'
            LOG.info(error)
        else:
            LOG.info('pf9-consul service is running, now try to re-join %s', join_ips)

        # re-join the new cluster
        cmd = 'consul join {ip}'.format(ip=join_ips)
        result = run_cmd(cmd)
        if result != 0:
            has_error = True
            error = 'unable to switch consul rolei after tried 3 times, as failed to re-join into consul cluster %s' % join_ips
            LOG.info(error)
        else:
            LOG.info('re-join to cluster successfully')

        if has_error:
            retry = retry + 1
            LOG.info('now try %s time', str(retry))
            sleep(1)
        else:
            succeeded = True

    # finally send response with finished status
    msg = 'successfully switched consul from role %s to role %s' % (current_role, target_role)
    resp = ConsulRoleRebalanceResponse(req_id,
                                       current_host_id,
                                       status=constants.REBALANCE_STATE_FINISHED,
                                       message=msg)
    rebalance_mgr.send_role_rebalance_response(resp)
    return True


def processing_rebalance_requests(rebalance_mgr, hostid, join_ips):
    global STOPPING
    global REBALANCE_IN_PROGRESS
    while not STOPPING:
        try:
            if rebalance_mgr:
                LOG.debug('check role rebalance request at %s', str(datetime.utcnow()))
                req = rebalance_mgr.get_role_rebalance_request()
                LOG.info('found rebalance request : %s', str(req))
                if req and req['host_id'] == hostid:
                    REBALANCE_IN_PROGRESS = True
                    LOG.debug('received rebalance request for me %s : %s', hostid, str(req))
                    cluster_setup = switch_to_new_consul_role(rebalance_mgr, req, hostid, join_ips)
                    LOG.debug('is role rebalance succeeded ? %s', str(cluster_setup))
                    REBALANCE_IN_PROGRESS = False
                else:
                    REBALANCE_IN_PROGRESS = False
                    LOG.debug('received rebalance request is None or not for me : %s', str(req))
        except:
            REBALANCE_IN_PROGRESS = False
            LOG.exception('unhandled exception in processing_rebalance_requests')
        sleep(1)


def loop():
    global STOPPING
    global REBALANCE_IN_PROGRESS
    _show_conf(CONF)
    cfgparser = ConfigParser()
    cfgparser.read('/var/opt/pf9/hostagent/data.conf')
    hostid = cfgparser.get('DEFAULT', 'host_id')
    sleep_time = CONF.consul.status_check_interval
    ch = consul_helper.consul_status(hostid)
    reporter = report.HaManagerReporter()
    start_loop = False
    cluster_setup = False

    LOG.info('create consul config file now')
    # TODO(pacharya): Handle restart of pf9-ha-slave service
    generate_consul_conf()

    # Assume that consul was not running beforehand
    # TODO(pacharya): If consul was running beforehand we need to cleanup the
    #                 data dir of consul to get rid of the earlier state.
    LOG.info('start consul service deamon')
    start_loop = start_consul_service()

    # find out consul join ips
    join_ips = ''
    if CONF.consul.join:
        ips = CONF.consul.join.split(',')
        join_ips = ' '.join([x.strip() for x in ips])
    LOG.debug('consul join addresses from config file :%s', join_ips)

    rebalance_thread = None
    rebalance_mgr = None

    try:
        # start dedicated rebalance request handling thread
        role_rebalance_enabled = CONF.consul_role_rebalance.role_rebalance_enabled
        LOG.info('is role rebalance enabled ? %s', str(role_rebalance_enabled))
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
            amqp_queue_for_receiving = 'queue-receiving-request-for-%s' % hostid
            amqp_routingkey_sending = CONF.consul_role_rebalance.amqp_routingkey_sending
            amqp_routingkey_receiving = CONF.consul_role_rebalance.amqp_routingkey_receiving
            parameters = 'host: %s, port: %s, user: %s, password: %s, exchange: %s, type: %s, queue: %s, send routing: %s, receiving routing: %s' % (
                amqp_host, amqp_port, amqp_user, amqp_passwd, amqp_exchange, amqp_exchange_type,
                amqp_queue_for_receiving, amqp_routingkey_sending, amqp_routingkey_receiving
            )
            msg = 'create rebalance manager with : %s' % parameters
            LOG.info(msg)
            rebalance_mgr = RebalanceManager(amqp_host,
                                             amqp_port,
                                             amqp_user,
                                             amqp_passwd,
                                             amqp_virtualhost,
                                             amqp_exchange,
                                             amqp_exchange_type,
                                             amqp_routingkey_sending,
                                             amqp_queue_for_receiving,
                                             amqp_routingkey_receiving
                                             )
            LOG.info('rebalance manager is created')

            # start dedicated thread to handle the rebalance requests
            LOG.info('starting thread processing_rebalance_requests')
            rebalance_thread = threading.Thread(target=processing_rebalance_requests,
                                                args=(rebalance_mgr, hostid, join_ips,))
            rebalance_thread.daemon = True
            rebalance_thread.start()
            LOG.info('thread processing_rebalance_requests started')

        # the main thread handling host down events
        while start_loop:
            if not cluster_setup:
                # Running join against oneself generates a warning message in
                # logs but does not cause consul to crash
                if not join_ips:
                    LOG.error('null or empty consul join ip list in config file')
                    sleep(sleep_time)
                    continue
                LOG.info('try to join cluster members %s', join_ips)
                retcode = run_cmd('consul join {ip}'.format(ip=join_ips))
                if retcode == 0:
                    LOG.info('joined consul cluster members {ip}'.format(
                        ip=join_ips))
                    cluster_setup = True
                    ch.log_kvstore()
                else:
                    LOG.info('join consul cluster %s failed', join_ips)
            else:
                if REBALANCE_IN_PROGRESS:
                    LOG.info('consul rebalance is in progress, need to wait for it complete')
                else:
                    if ch.am_i_cluster_leader():
                        cluster_stat = ch.get_cluster_status()
                        if cluster_stat:
                            LOG.info('i am leader, found changes : %s', str(cluster_stat))
                            LOG.debug('cluster_stat: %s', cluster_stat)
                            if reporter.report_status(cluster_stat):
                                LOG.info('consul status is reported to hamgr: %s',
                                         cluster_stat)
                                ch.update_reported_status(cluster_stat)
                            else:
                                LOG.info('report consul status to hamgr failed')
                        else:
                            LOG.debug('i am leader, but no changes to report for now')
                        ch.cleanup_consul_kv_store()
                    else:
                        LOG.debug('i am not leader so do nothing')

            # It is possible that host ID was not published when the consul
            # helper was created as the cluster was not yet formed. Since this
            # operation is idempotent calling it in a loop will not cause
            # multiple updates.
            LOG.info('publish current host id %s', hostid)
            ch.publish_hostid()

            LOG.info('sleeping for %s seconds' % sleep_time)
            sleep(sleep_time)
    except:
        LOG.exception('unhandled exception in pf9-ha-slave, exiting now')
    if rebalance_thread and rebalance_thread.is_alive():
        STOPPING = True
        rebalance_thread.join(5)
        rebalance_thread = None
    del rebalance_mgr
    LOG.error('pf9-ha-slave service exiting...')
