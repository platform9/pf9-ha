#!/usr/bin/env python

# Copyright (c) 2016 Platform9 Systems Inc.
#
#    Licensed under the Apache License, Version 2.0 (the "License"); you may
#    not use this file except in compliance with the License. You may obtain
#    a copy of the License at
#
#         http://www.apache.org/licenses/LICENSE-2.0
#
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
#    WARRANTIES OR CONDITIONS OF ANY KIND, either expressed or implied. See the
#    License for the specific language governing permissions and limitations
#    under the License.

import argparse
import ConfigParser

import eventlet
from eventlet import wsgi
from hamgr import periodic_task
from paste.deploy import loadapp
from hamgr import ha_provider
from hamgr import logger as logging
from hamgr import notification

eventlet.monkey_patch()

LOG = logging.getLogger(__name__)

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
        LOG.debug('start notification publisher')
        notification.start(conf)
        LOG.debug('start periodic task')
        periodic_task.start()
        LOG.debug('get ha provider')
        provider = ha_provider.ha_provider()
        LOG.debug('add task check_host_aggregate_changes')
        periodic_task.add_task(provider.check_host_aggregate_changes, 120, run_now=True)
        # dedicated task to handle host events
        LOG.debug('add task host_events_processing')
        periodic_task.add_task(provider.host_events_processing, 120, run_now=True)
        LOG.debug('start wsgi server')
        # background thread for handling HA enable/disable request
        periodic_task.add_task(provider.ha_enable_disable_request_processing, 5,
                               run_now=True)
        wsgi_app = loadapp('config:%s' % paste_file, 'main')
        wsgi.server(eventlet.listen(('', conf.getint("DEFAULT", "listen_port"))),
                    wsgi_app, LOG)
    except:
        # the wsgi.server is blocking call, if comes here mean it failed
        # so we can clean up here
        LOG.debug('stop notification publisher')
        notification.stop()


if __name__ == '__main__':
    parser = _get_arg_parser()
    conf = ConfigParser.ConfigParser()
    with open(parser.config_file) as f:
        conf.readfp(f)
    start_server(conf, parser.paste_file)
