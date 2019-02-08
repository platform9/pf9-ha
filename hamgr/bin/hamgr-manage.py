#!/bin/env python
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

import argparse
import ConfigParser

from migrate.exceptions import DatabaseAlreadyControlledError
from migrate.versioning.api import upgrade
from migrate.versioning.api import version_control


def _get_arg_parser():
    parser = argparse.ArgumentParser(
        description="High Availability Manager for VirtualMachines")
    parser.add_argument('--config-file', dest='config_file',
                        default='/etc/pf9/hamgr/hamgr.conf')
    parser.add_argument('--command', dest='command', default='db_sync')
    return parser.parse_args()


def _version_control(conf):
    try:
        version_control(conf.get("database", "sqlconnectURI"),
                        conf.get("database", "repo"))
    except DatabaseAlreadyControlledError:
        # Ignore the already controlled error
        pass

if __name__ == '__main__':
    parser = _get_arg_parser()
    conf = ConfigParser.ConfigParser()
    conf.readfp(open(parser.config_file))
    if parser.command == 'db_sync':
        _version_control(conf)
        upgrade(conf.get("database", "sqlconnectURI"),
                conf.get("database", "repo"))
        exit(0)
    else:
        print('Unknown command')
        exit(1)
