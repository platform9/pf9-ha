#!/bin/env python
#
# Copyright (c) 2016, Platform9 Systems. All Rights Reserved.
#

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
