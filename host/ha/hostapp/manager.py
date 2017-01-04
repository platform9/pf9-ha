# Copyright 2016 Platform9 Systems Inc.
# All Rights Reserved

from ConfigParser import ConfigParser
from datetime import datetime
from oslo_config import cfg
from ha.utils import log as logging
from ha.utils import consul_helper
from ha.utils import report
from subprocess import call
from time import daylight
from time import sleep
from time import tzname

import json

LOG = logging.getLogger('ha-manager')

CONF = cfg.CONF


consul_grp = cfg.OptGroup('consul', title='Opt group for consul')
consul_opts = [
    cfg.IntOpt('status_check_interval', default=10,
               help='Time interval in seconds between status checks'),
    cfg.IPOpt('join', help='IP address of the server to connect to'),
    cfg.IntOpt('bootstrap_expect', default=0,
                help='Whether to start consul as server or agent. Valid values '
                     'are 0, 1, 3 and 5. 0 indicates that consul is started in '
                     'agent mode while 1, 3 and 5 indicate that consul is '
                     'started in server mode with bootstrap_expect being that '
                     'specified value.'),
]


CONF.register_group(consul_grp)
CONF.register_opts(consul_opts, consul_grp)
PF9_CONSUL_CONF_DIR = '/opt/pf9/etc/pf9-consul/'


def expand_stats(cluster_stat):
    cluster_stat['type'] = 'rscGroup'
    cluster_stat['regionID'] = ""
    cluster_stat['time'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    cluster_stat['tzname'] = tzname[0]
    cluster_stat['daylight'] = daylight


def generate_consul_conf():
    ip_address = consul_helper.get_ip_address()
    if CONF.consul.bootstrap_expect == 0:
        # Start consul with agent conf
        with open(PF9_CONSUL_CONF_DIR + 'client.json.template') as fptr:
            agent_conf = json.load(fptr)
        agent_conf['advertise_addr'] = ip_address
        with open(PF9_CONSUL_CONF_DIR + 'conf.d/client.json', 'w') as fptr:
            json.dump(agent_conf, fptr)
    else:
        # Start consul with server conf
        with open(PF9_CONSUL_CONF_DIR + 'server.json.template') as fptr:
            server_conf = json.load(fptr)
        server_conf['advertise_addr'] = ip_address
        server_conf['bootstrap_expect'] = CONF.consul.bootstrap_expect
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
            retcode = run_cmd('consul join {ip}'.format(ip=CONF.consul.join))
            if retcode == 0:
                LOG.info('Joined consul cluster server {ip}'.format(
                    ip=CONF.consul.join))
                cluster_setup = True
        elif ch.am_i_cluster_leader():
            cluster_stat = ch.get_cluster_status()
            if cluster_stat:
                expand_stats(cluster_stat)
                LOG.info('cluster_stat: %s', cluster_stat)
                if reporter.report_status(cluster_stat):
                    ch.update_reported_status(cluster_stat)
            ch.cleanup_consul_kv_store()
        # It is possible that host ID was not published when the consul
        # helper was created as the cluster was not yet formed. Since this
        # operation is idempotent calling it in a loop will not cause
        # multiple updates.
        ch.publish_hostid()

        LOG.info('sleeping for %s seconds' % sleep_time)
        sleep(sleep_time)

    LOG.error('pf9-ha-slave service exiting...')

