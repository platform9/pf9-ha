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

import ConfigParser
import argparse
import time

from hamgr import logger as logging
from hamgr import notification
from hamgr.notification import notification as ha_notification

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
        count = 0
        while True:
            msg = ha_notification.Notification("test", "add", str(count))
            LOG.debug('### debugger publish msg : ' + str(msg) )
            notification.publish(msg)
            count = count + 1
            time.sleep(0.100)
    except Exception as e:
        LOG.exception('## unhandled exception that went wrong : %s', str(e))
        # the wsgi.server is blocking call, if comes here mean it failed
        # so we can clean up here
        LOG.debug('###stop notification publisher')
        notification.stop()

if __name__ == '__main__':
    parser = _get_arg_parser()
    conf = ConfigParser.ConfigParser()
    with open(parser.config_file) as f:
        conf.readfp(f)
    start_server(conf, parser.paste_file)
