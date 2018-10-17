# Copyright 2016 Platform9 Systems Inc.
# All Rights Reserved

import json

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

LOG = logging.getLogger('ha-manager')

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
CONF.register_opts(default_opts)
PF9_CONSUL_CONF_DIR = '/opt/pf9/etc/pf9-consul/'


def generate_consul_conf():
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
        LOG.warn('Consul service was already running.')
        return True

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


def loop():
    cfgparser = ConfigParser()
    cfgparser.read('/var/opt/pf9/hostagent/data.conf')
    hostid = cfgparser.get('DEFAULT', 'host_id')
    sleep_time = CONF.consul.status_check_interval
    ch = consul_helper.consul_status(hostid)
    reporter = report.HaManagerReporter()
    start_loop = False
    cluster_setup = False

    # TODO(pacharya): Handle restart of pf9-ha-slave service
    generate_consul_conf()

    # Assume that consul was not running beforehand
    # TODO(pacharya): If consul was running beforehand we need to cleanup the
    #                 data dir of consul to get rid of the earlier state.
    start_loop = start_consul_service()

    while start_loop:
        if not cluster_setup:
            # Running join against oneself generates a warning message in
            # logs but does not cause consul to crash
            raw = CONF.consul.join
            if not raw:
                LOG.error('null or empty consul join ip list in config file')
                sleep(sleep_time)
                continue
            LOG.debug('consul join addresses from config file :%s', str(raw))
            ips = raw.split(',')
            members = ' '.join([x.strip() for x in ips])
            LOG.info('try to join cluster members %s', members)
            retcode = run_cmd('consul join {ip}'.format(ip=members))
            if retcode == 0:
                LOG.info('joined consul cluster members {ip}'.format(
                    ip=members))
                cluster_setup = True
                ch.log_kvstore()
        elif ch.am_i_cluster_leader():
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
        ch.publish_hostid()

        LOG.info('sleeping for %s seconds' % sleep_time)
        sleep(sleep_time)

    LOG.error('pf9-ha-slave service exiting...')
