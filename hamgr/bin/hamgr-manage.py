#!/bin/env python

#
# Copyright (c) 2016, Platform9 Systems. All Rights Reserved.
#

import argparse
import ConfigParser
from migrate.versioning.api import upgrade, create, version_control
from migrate.exceptions import DatabaseAlreadyControlledError


def _get_arg_parser():
    parser = argparse.ArgumentParser(description="High Availability Manager for VirtualMachines")
    parser.add_argument('--config-file', dest='config_file', default='/etc/pf9/hamgr/hamgr.conf')
    parser.add_argument('--command', dest='command', default='db_sync')
    return parser.parse_args()


def _version_control(conf):
    try:
        version_control(conf.get("database", "sqlconnectURI"), conf.get("database", "repo"))
    except DatabaseAlreadyControlledError as e:
        print e
        # Ignore the already controlled error

if __name__ == '__main__':
    parser = _get_arg_parser()
    conf = ConfigParser.ConfigParser()
    conf.readfp(open(parser.config_file))
    if 'db_sync' == parser.command:
        _version_control(conf)
        upgrade(conf.get("database", "sqlconnectURI"), conf.get("database", "repo"))
        exit(0)
    else:
        print 'Unknown command'
        exit(1)




