#!/usr/bin/env python
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

# according to http://eventlet.net/doc/patching.html, eventlet will
# patch python standard libs, but thread in eventlet
# causes deadlock when use together with python's standard thread
# methods. to avoid this, exclude the thread module from start
# point of application to avoid thread deadlock problem.

import argparse
import logging

import ConfigParser
from eventlet import listen
from eventlet import wsgi
from hamgr.logger import setup_root_logger
from paste.deploy import loadapp

from hamgr import periodic_task
from hamgr import provider_factory
from shared.constants import LOGGER_PREFIX

LOG = logging.getLogger(LOGGER_PREFIX + __name__)

def _get_arg_parser():
    parser = argparse.ArgumentParser(
        description="High Availability Manager for VirtualMachines")
    parser.add_argument('--config-file', dest='config_file',
                        default='/etc/pf9/hamgr/hamgr.conf')
    parser.add_argument('--paste-ini', dest='paste_file')
    return parser.parse_args()


def start_server(conf, paste_ini):
    if paste_ini:
        paste_file = paste_ini
    else:
        paste_file = conf.get("DEFAULT", "paste-ini")
    try:
        LOG.debug('start periodic task')
        periodic_task.start()
        LOG.debug('get ha provider')
        provider = provider_factory.ha_provider()
        LOG.debug('add task process_consul_encryption_configuration')
        periodic_task.add_task(provider.process_consul_encryption_configuration, 60, run_now=True)
        LOG.debug('add task process_host_aggregate_changes')
        periodic_task.add_task(provider.process_host_aggregate_changes, 60, run_now=True)
        # dedicated task to handle host events
        LOG.debug('add task process_host_events')
        periodic_task.add_task(provider.process_host_events, 60, run_now=True)
        # task to handle consul role rebalance
        LOG.debug('add task process_consul_role_rebalance_requests')
        periodic_task.add_task(provider.process_consul_role_rebalance_requests, 60, run_now=True)
        # background thread for handling HA enable/disable request
        LOG.debug('add task process_ha_enable_disable_requests')
        periodic_task.add_task(provider.process_ha_enable_disable_requests, 5, run_now=True)
        LOG.debug('start wsgi server')
        wsgi_app = loadapp('config:%s' % paste_file, 'main')
        wsgi.server(listen(('', conf.getint("DEFAULT", "listen_port"))),
                    wsgi_app, LOG)
    except Exception:
        # the wsgi.server is blocking call, if comes here mean it failed
        # so we can clean up here
        LOG.exception('unhandled exception from server')

if __name__ == '__main__':
    parser = _get_arg_parser()
    conf = ConfigParser.ConfigParser()
    with open(parser.config_file) as f:
        conf.readfp(f)
    # setup root logger in main entry before any logging methods is used
    setup_root_logger()
    start_server(conf, parser.paste_file)
